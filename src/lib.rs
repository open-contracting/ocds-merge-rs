use std::collections::HashMap;

use derivative::Derivative;
use indexmap::IndexMap;
use log::warn;
use serde_json::{json, Value};

macro_rules! join {
    ( $vec:expr , $item:expr ) => {
        $vec.iter().chain([$item]).cloned().collect::<Vec<_>>()
    };
}

macro_rules! field {
    ( $value:expr ) => {
        Part::Field(String::from($value))
    };
}

#[derive(Debug, PartialEq)]
enum Rule {
    Omit,
    Replace,
}

enum Strategy {
    Append,
    MergeByPosition,
}

/// A part of a JSON path.
#[derive(Clone, Debug, Derivative, Eq)]
#[derivative(Hash, PartialEq)]
enum Part {
    /// The identifier of an object in an array.
    Identifier {
        /// The extracted or generated identifier.
        id: Id,
        /// The original value.
        #[derivative(Hash = "ignore")]
        #[derivative(PartialEq = "ignore")]
        original: Option<Value>,
    },
    /// The name of a field in an object.
    Field(String),
}

/// The value of an "id" field.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Id {
    Integer(i64),
    String(String),
}

#[derive(Default)]
struct Merger {
    rules: HashMap<Vec<String>, Rule>,
    overrides: HashMap<Vec<String>, Strategy>,
}

impl Merger {
    /// Merge the sorted releases into a compiled release.
    pub fn create_compiled_release(&mut self, releases: &[Value]) -> Value {
        let mut flattened = IndexMap::new();

        for release in releases {
            // Store the values of fields that set "omitWhenMerged": true.
            // In OCDS 1.0, `ocid` incorrectly sets "mergeStrategy": "ocdsOmit".
            let ocid = release["ocid"].as_str().unwrap_or("None");
            let date = release["date"].as_str().unwrap_or("None");

            let mut flat = IndexMap::new();
            self.flatten(&mut flat, &[], &mut vec![], false, release);

            flattened.extend(flat);
            flattened.insert(vec![field!("ocid")], ocid.into());
            flattened.insert(vec![field!("date")], date.into());
            flattened.insert(vec![field!("id")], Value::String(format!("{ocid}-{date}")));
        }

        flattened.insert(vec![field!("tag")], json!(["compiled"]));

        Self::unflatten(&flattened)
    }

    /// Merge the sorted releases into a versioned release.
    ///
    /// # Note
    ///
    /// The ``"tag"`` field of each release is removed.
    pub fn create_versioned_release(&mut self, releases: &mut [Value]) -> Value {
        let mut flattened = IndexMap::new();

        for release in releases {
            // Store the values of fields that set "omitWhenMerged": true.
            // Prior to OCDS 1.1.4, `tag` didn't set "omitWhenMerged": true.
            let ocid = release["ocid"].clone();
            let date = release["date"].clone();
            let id = release["id"].clone();
            let tag = release.as_object_mut().expect("Release is not an object").remove("tag");

            let mut flat = IndexMap::new();
            self.flatten(&mut flat, &[], &mut vec![], true, release);

            // Don't version the OCID.
            let path = vec![field!("ocid")];
            flat.remove(&path);
            flattened.insert(path, ocid);

            for (key, value) in flat {
                // If the value is unversioned, continue.
                if let Value::Array(vec) = flattened.entry(key).or_insert_with(|| json!([])) {
                    // If the value is new or changed, update the history.
                    if vec.is_empty() || value != vec[vec.len() - 1]["value"] {
                        vec.push(json!({
                            "releaseID": id,
                            "releaseDate": date,
                            "releaseTag": tag,
                            "value": value
                        }));
                    }
                }
            }
        }

        Self::unflatten(&flattened)
    }

    /// Calculate the merge rules from a JSON Schema.
    pub fn get_rules(value: &Value, path: &[String]) -> HashMap<Vec<String>, Rule> {
        let mut rules = HashMap::new();

        if let Value::Object(properties) = value {
            for (property, subschema) in properties {
                let new_path = join!(path, property);
                let types = Self::get_types(subschema);

                // `omitWhenMerged` supersedes all other rules.
                // See https://standard.open-contracting.org/1.1/en/schema/merging/#discarded-fields
                if subschema["omitWhenMerged"].as_bool() == Some(true)
                    || subschema["mergeStrategy"].as_str() == Some("ocdsOmit")
                {
                    rules.insert(new_path, Rule::Omit);
                // `wholeListMerge` supersedes any nested rules.
                // See https://standard.open-contracting.org/1.1/en/schema/merging/#whole-list-merge
                } else if types.contains(&"array")
                    && (subschema["wholeListMerge"].as_bool() == Some(true)
                        || subschema["mergeStrategy"].as_str() == Some("ocdsVersion"))
                {
                    rules.insert(new_path, Rule::Replace);
                // See https://standard.open-contracting.org/1.1/en/schema/merging/#object-values
                } else if types.contains(&"object") && subschema.get("properties").is_some() {
                    rules.extend(Self::get_rules(&subschema["properties"], &new_path));
                // See https://standard.open-contracting.org/1.1/en/schema/merging/#whole-list-merge
                } else if types.contains(&"array") && subschema.get("items").is_some() {
                    let item_types = Self::get_types(&subschema["items"]);
                    if item_types.iter().any(|&item_type| item_type != "object") {
                        rules.insert(new_path, Rule::Replace);
                    } else if item_types.contains(&"object") && subschema["items"].get("properties").is_some() {
                        if subschema["items"]["properties"].get("id").is_none() {
                            rules.insert(new_path, Rule::Replace);
                        } else {
                            rules.extend(Self::get_rules(&subschema["items"]["properties"], &new_path));
                        }
                    }
                }
            }
        }

        rules
    }

    fn get_types(value: &Value) -> Vec<&str> {
        match &value["type"] {
            Value::String(string) => vec![string.as_str()],
            Value::Array(vec) => vec.iter().map(|string| string.as_str().unwrap_or("")).collect(),
            _ => vec![],
        }
    }

    /// Dereference all ``$ref`` properties to local definitions.
    pub fn dereference(value: &mut Value) {
        fn f(value: &mut Value, schema: &Value, visited: &Vec<String>) {
            if let Value::Object(object) = value {
                if let Some(Value::String(reference)) = object.remove("$ref") {
                    if visited.contains(&reference) {
                        // If we already visited this $ref in this $ref chain, stop.
                        return;
                    }
                    if let Some(mut target) = schema.pointer(&reference[1..]).cloned() {
                        f(&mut target, schema, &join!(visited, &reference));
                        *value = target;
                    }
                }
            }

            // The if-statement is repeated, because `value` could be replaced with a non-object.
            if let Value::Object(object) = value {
                for v in object.values_mut() {
                    f(v, schema, visited);
                }
            }
        }

        f(value, &value.clone(), &vec![]);
    }

    fn flatten(
        &mut self,
        flattened: &mut IndexMap<Vec<Part>, Value>,
        path: &[Part],
        rule_path: &mut Vec<String>,
        versioned: bool,
        json: &Value,
    ) {
        match json {
            Value::Array(vec) => {
                // This tracks the identifiers of objects in an array, to warn about collisions.
                let mut identifiers: HashMap<Vec<Part>, usize> = HashMap::new();

                for (index, value) in vec.iter().enumerate() {
                    if let Value::Object(map) = value {
                        // If the object has an `id` field, get its value, to apply the identifier merge strategy.
                        let (original, id) = if map.contains_key("id") {
                            (
                                Some(&map["id"]),
                                match &map["id"] {
                                    Value::String(string) => Id::String(string.clone()),
                                    Value::Number(number) => Id::Integer(
                                        number.as_i64().expect("\"id\" is not an integer or is out of bounds"),
                                    ),
                                    _ => panic!("\"id\" is not a string or number"),
                                },
                            )
                        // If the object has no `id` field, set a default unique value.
                        } else {
                            (None, Id::Integer(fastrand::i64(..)))
                        };

                        // Calculate the key for checking for collisions using the identifier merge strategy.
                        let default_key = Part::Identifier {
                            id,
                            original: original.cloned(),
                        };

                        let key = match self.overrides.get(rule_path) {
                            Some(Strategy::Append) => Part::Identifier {
                                id: Id::Integer(fastrand::i64(..)),
                                original: original.cloned(),
                            },
                            Some(Strategy::MergeByPosition) => Part::Identifier {
                                id: Id::Integer(index as i64),
                                original: original.cloned(),
                            },
                            None => default_key.clone(),
                        };

                        // Check whether the identifier is used by other objects in the array.
                        if *identifiers.entry(join!(path, &default_key)).or_insert(index) != index {
                            warn!(
                                "Multiple objects have the `id` value {default_key:?} in the `{}` array",
                                rule_path.join(".")
                            );
                        }

                        self.flatten_key_value(flattened, path, rule_path, versioned, &key, value);
                    }
                }
            }
            Value::Object(map) => {
                for (key, value) in map {
                    rule_path.push(key.clone());
                    self.flatten_key_value(flattened, path, rule_path, versioned, &Part::Field(key.clone()), value);
                    rule_path.pop();
                }
            }
            _ => unreachable!(),
        };
    }

    fn flatten_key_value(
        &mut self,
        flattened: &mut IndexMap<Vec<Part>, Value>,
        path: &[Part],
        rule_path: &mut Vec<String>,
        versioned: bool,
        key: &Part,
        value: &Value,
    ) {
        match self.rules.get(rule_path) {
            Some(Rule::Omit) => {}
            // If it's `wholeListMerge` ...
            Some(Rule::Replace) => {
                flattened.insert(join!(path, key), value.clone());
            }
            None => {
                // Or if it's neither an object nor an array ...
                if !value.is_object() && !value.is_array()
                    // Or if it's an array ...
                    || matches!(value, Value::Array(vec)
                            // ... containing non-objects ...
                            if vec.iter().any(|v| !v.is_object())
                            // ... containing versioned values ...
                            || versioned
                            && !vec.is_empty()
                            && vec.iter().all(|v|
                                matches!(v, Value::Object(map)
                                    if map.len() == 4
                                    && map.contains_key("releaseID")
                                    && map.contains_key("releaseDate")
                                    && map.contains_key("releaseTag")
                                    && map.contains_key("value")
                                )
                            )
                    )
                {
                    // ... then use the whole list merge strategy.
                    flattened.insert(join!(path, key), value.clone());
                // Recurse into non-empty objects, and non-empty arrays of objects that aren't `wholeListMerge`.
                } else if matches!(value, Value::Object(map) if !map.is_empty())
                    || matches!(value, Value::Array(vec) if !vec.is_empty())
                {
                    self.flatten(flattened, &join!(path, key), rule_path, versioned, value);
                }
            }
        }
    }

    fn unflatten(flattened: &IndexMap<Vec<Part>, Value>) -> Value {
        let mut unflattened = json!({});
        // Track at which array index each object in an array maps to.
        let mut indices: HashMap<&[Part], usize> = HashMap::new();

        for (key, value) in flattened {
            // The pointer to unflattened data that corresponds to the current path.
            let mut pointer = &mut unflattened;

            // For each sub-path in the key.
            for (position, part) in key.iter().enumerate() {
                match part {
                    // The sub-path is to an item of an array.
                    Part::Identifier { original, .. } => {
                        let index = indices.entry(&key[..=position]).or_insert_with(|| {
                            let mut object = json!({});
                            // If the original object had an `id` value, set it.
                            if let Some(id) = original {
                                object["id"] = id.clone();
                            }
                            let array = pointer.as_array_mut().expect("Value should be an array");
                            array.push(object);
                            array.len() - 1
                        });
                        pointer = &mut pointer[index.to_owned()];
                    }
                    // The sub-path is to a property of an object.
                    Part::Field(field) => {
                        // If this is a visited node, change into it.
                        if pointer.get(field).is_some() {
                            pointer = &mut pointer[field];
                        // If this is not a leaf node, it is an array or object.
                        } else if position + 1 < key.len() {
                            // Peek at the next node to instantiate it, then change into it.
                            pointer[field] = match key.get(position + 1) {
                                Some(Part::Identifier { .. }) => json!([]),
                                Some(Part::Field(_)) => json!({}),
                                None => unreachable!("Index is out of bounds"),
                            };
                            pointer = &mut pointer[field];
                        // If this is a leaf node, copy the value unless it is null.
                        } else if !value.is_null() {
                            pointer[field] = value.clone();
                        }
                    }
                }
            }
        }

        unflattened
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::BufReader;
    use std::io::Read;

    use pretty_assertions::assert_eq;
    use serde_json::json;

    macro_rules! i {
        ( $value:expr ) => {
            Part::Identifier {
                id: Id::Integer($value),
                original: None,
            }
        };
    }

    macro_rules! s {
        ( $value:expr ) => {
            Part::Identifier {
                id: Id::String(String::from($value)),
                original: None,
            }
        };
    }

    macro_rules! v {
        ( $( $value:expr ),* ) => {
            vec![ $( String::from($value), )* ]
        };
    }

    fn read(path: &str) -> Value {
        let file = File::open(path).expect(format!("{path} does not exist").as_str());
        let mut reader = BufReader::new(file);
        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();
        serde_json::from_str(&contents).unwrap()
    }

    #[test]
    fn flatten_1() {
        let mut merger = Merger::default();

        let mut flattened = IndexMap::new();

        let item = json!({
            "c": "I am a",
            "b": ["A", "list"],
            "a": [
                {"id": 1, "cb": "I am ca"},
                {"id": 2, "ca": "I am cb"}
            ]
        });

        merger.flatten(&mut flattened, &[], &mut vec![], false, &item);

        assert_eq!(
            flattened,
            IndexMap::from([
                (vec![field!("c")], json!("I am a")),
                (vec![field!("b")], json!(["A", "list"])),
                (vec![field!("a"), i!(1), field!("cb")], json!("I am ca")),
                (vec![field!("a"), i!(1), field!("id")], json!(1)),
                (vec![field!("a"), i!(2), field!("ca")], json!("I am cb")),
                (vec![field!("a"), i!(2), field!("id")], json!(2)),
            ])
        );

        assert_eq!(Merger::unflatten(&flattened), item);
    }

    #[test]
    fn flatten_2() {
        // OCDS in decimal.
        fastrand::seed(79_67_68_83);

        let mut merger = Merger::default();

        let mut flattened = IndexMap::new();

        let item = json!({
            "a": [
                {"id": "identifier"},
                {"key": "value"}
            ]
        });

        merger.flatten(&mut flattened, &[], &mut vec![], false, &item);

        let values = flattened.values().collect::<Vec<_>>();
        let keys = flattened.keys().cloned().collect::<Vec<_>>();

        assert_eq!(
            flattened,
            IndexMap::from([
                (vec![field!("a"), s!("identifier"), field!("id")], json!("identifier")),
                (
                    vec![field!("a"), i!(-8433386414344686362), field!("key")],
                    json!("value")
                ),
            ])
        );

        assert_eq!(Merger::unflatten(&flattened), item);
    }

    #[test]
    fn dereference_cyclic_dependency() {
        Merger::dereference(&mut json!({"$ref": "#"}));
    }

    #[test]
    fn rules_1_1() {
        let mut schema = read("tests/fixtures/release-schema-1__1__4.json");

        Merger::dereference(&mut schema);

        assert_eq!(
            Merger::get_rules(&schema["properties"], &[]),
            HashMap::from([
                (v!["awards", "items", "additionalClassifications"], Rule::Replace),
                (v!["contracts", "items", "additionalClassifications"], Rule::Replace),
                (v!["contracts", "relatedProcesses", "relationship"], Rule::Replace),
                (v!["date"], Rule::Omit),
                (v!["id"], Rule::Omit),
                (v!["parties", "additionalIdentifiers"], Rule::Replace),
                (v!["parties", "roles"], Rule::Replace),
                (v!["relatedProcesses", "relationship"], Rule::Replace),
                (v!["tag"], Rule::Omit),
                (v!["tender", "additionalProcurementCategories"], Rule::Replace),
                (v!["tender", "items", "additionalClassifications"], Rule::Replace),
                (v!["tender", "submissionMethod"], Rule::Replace),
                // Deprecated
                (v!["awards", "amendment", "changes"], Rule::Replace),
                (v!["awards", "amendments", "changes"], Rule::Replace),
                (v!["awards", "suppliers", "additionalIdentifiers"], Rule::Replace),
                (v!["buyer", "additionalIdentifiers"], Rule::Replace),
                (v!["contracts", "amendment", "changes"], Rule::Replace),
                (v!["contracts", "amendments", "changes"], Rule::Replace),
                (
                    v![
                        "contracts",
                        "implementation",
                        "transactions",
                        "payee",
                        "additionalIdentifiers"
                    ],
                    Rule::Replace
                ),
                (
                    v![
                        "contracts",
                        "implementation",
                        "transactions",
                        "payer",
                        "additionalIdentifiers"
                    ],
                    Rule::Replace
                ),
                (v!["tender", "amendment", "changes"], Rule::Replace),
                (v!["tender", "amendments", "changes"], Rule::Replace),
                (v!["tender", "procuringEntity", "additionalIdentifiers"], Rule::Replace),
                (v!["tender", "tenderers", "additionalIdentifiers"], Rule::Replace),
            ])
        );
    }

    #[test]
    fn rules_1_0() {
        let mut schema = read("tests/fixtures/release-schema-1__0__3.json");

        Merger::dereference(&mut schema);

        assert_eq!(
            Merger::get_rules(&schema["properties"], &[]),
            HashMap::from([
                (v!["awards", "amendment", "changes"], Rule::Replace),
                (v!["awards", "items", "additionalClassifications"], Rule::Replace),
                (v!["awards", "suppliers"], Rule::Replace),
                (v!["buyer", "additionalIdentifiers"], Rule::Replace),
                (v!["contracts", "amendment", "changes"], Rule::Replace),
                (v!["contracts", "items", "additionalClassifications"], Rule::Replace),
                (v!["date"], Rule::Omit),
                (v!["id"], Rule::Omit),
                (v!["ocid"], Rule::Omit),
                (v!["tag"], Rule::Omit),
                (v!["tender", "amendment", "changes"], Rule::Replace),
                (v!["tender", "items", "additionalClassifications"], Rule::Replace),
                (v!["tender", "procuringEntity", "additionalIdentifiers"], Rule::Replace),
                (v!["tender", "submissionMethod"], Rule::Replace),
                (v!["tender", "tenderers"], Rule::Replace),
            ])
        );
    }

    fn merge(suffix: &str, path: &str, schema: &str) {
        let mut schema = read(&format!("tests/fixtures/{schema}.json"));

        Merger::dereference(&mut schema);

        let mut merger = Merger {
            rules: Merger::get_rules(&schema["properties"], &[]),
            ..Default::default()
        };

        let mut fixture = read(&format!("{}.json", path.rsplit_once('-').unwrap().0));

        let actual = match suffix {
            "compiled" => merger.create_compiled_release(&fixture.as_array().unwrap()),
            "versioned" => merger.create_versioned_release(&mut fixture.as_array_mut().unwrap()),
            _ => unreachable!(),
        };

        assert_eq!(actual, read(path));
    }

    include!(concat!(env!("OUT_DIR"), "/lib.include"));
}
