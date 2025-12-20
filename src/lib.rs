use std::collections::HashMap;

use derivative::Derivative;
use derive_more::Display;
use indexmap::IndexMap;
#[cfg(feature = "python")]
use pyo3::PyTypeInfo;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::{PyDict, PyTuple, PyType};
#[cfg(feature = "python")]
use pythonize::depythonize;
#[cfg(feature = "python")]
use pythonize::pythonize;
use serde_json::{Value, json};

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

#[cfg(feature = "python")]
mod exceptions {
    use pyo3::create_exception;

    create_exception!(exceptions, MergeError, pyo3::exceptions::PyException);
    create_exception!(exceptions, NonObjectReleaseError, MergeError);
    create_exception!(exceptions, InconsistentTypeError, MergeError);
    create_exception!(exceptions, MissingDateKeyError, MergeError);
    create_exception!(exceptions, NullDateValueError, MergeError);
    create_exception!(exceptions, NonStringDateValueError, MergeError);
    create_exception!(exceptions, OutOfOrderReleaseError, MergeError);
    create_exception!(exceptions, MergeWarning, pyo3::exceptions::PyUserWarning);
    create_exception!(exceptions, DuplicateIdValueWarning, MergeWarning);
}

#[cfg(feature = "python")]
use exceptions::{
    DuplicateIdValueWarning, InconsistentTypeError, MergeError, MergeWarning, MissingDateKeyError,
    NonObjectReleaseError, NonStringDateValueError, NullDateValueError, OutOfOrderReleaseError,
};

// The Rust implementation of OCDS Merge uses an enum instead of str.
#[cfg_attr(feature = "python", pyclass(eq, eq_int, rename_all = "SCREAMING_SNAKE_CASE"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Rule {
    Omit,    // "omitWhenMerged"
    Replace, // "wholeListMerge"
}

#[cfg_attr(feature = "python", pyclass(eq, eq_int, rename_all = "SCREAMING_SNAKE_CASE"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Strategy {
    Append,
    MergeByPosition,
}

/// Error types for merging releases.
#[derive(Debug, Clone)]
pub enum Error {
    /// Raised when a release is not an object.
    NonObjectRelease { index: usize },
    /// Raised when a path is a literal, an object, and/or an array in different releases.
    InconsistentType {
        path: Vec<Part>,
        previous: Value,
        current: String,
    },
    /// Raised when a release is missing a 'date' key.
    MissingDateKey { index: usize },
    /// Raised when a release has a null 'date' value.
    NullDateValue { index: usize },
    /// Raised when a release has a non-string 'date' value.
    NonStringDateValue { index: usize },
    /// Raised when a release is out of order (by ascending date).
    OutOfOrderRelease {
        index: usize,
        previous: String,
        current: String,
    },
}

/// Raised when multiple objects have the same ID value in an array.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DuplicateIdWarning {
    pub id: String,
    pub path: String,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonObjectRelease { index } => {
                write!(f, "Release at index {index} must be an object")
            }
            Self::InconsistentType {
                path,
                previous,
                current,
            } => {
                write!(
                    f,
                    "An earlier release had {previous} for /{}, but the current release has {current}",
                    path.iter().map(ToString::to_string).collect::<Vec<_>>().join("/"),
                )
            }
            Self::MissingDateKey { index } => {
                write!(f, "Release at index {index} is missing a `date` field")
            }
            Self::NullDateValue { index } => {
                write!(f, "Release at index {index} has a null `date` value")
            }
            Self::NonStringDateValue { index } => {
                write!(f, "Release at index {index} has a non-string `date` value")
            }
            Self::OutOfOrderRelease {
                index,
                previous,
                current,
            } => {
                write!(
                    f,
                    "Release at index {index} has date '{current}' which is less than the previous '{previous}'"
                )
            }
        }
    }
}

#[cfg(feature = "python")]
fn error_to_pyerr(py: Python<'_>, err: Error) -> PyErr {
    fn create_exc<F>(exc_type: &Bound<'_, PyType>, message: String, set_attrs: F) -> PyErr
    where
        F: FnOnce(&Bound<'_, PyAny>),
    {
        exc_type.call1((message,)).map_or_else(
            |e| e,
            |instance| {
                set_attrs(&instance);
                PyErr::from_value(instance)
            },
        )
    }

    let message = err.to_string();

    match err {
        Error::NonObjectRelease { index } => create_exc(&NonObjectReleaseError::type_object(py), message, |inst| {
            let _ = inst.setattr("index", index);
        }),
        Error::InconsistentType {
            path,
            previous,
            current,
        } => create_exc(&InconsistentTypeError::type_object(py), message, |inst| {
            let path_str = path.iter().map(ToString::to_string).collect::<Vec<_>>().join("/");
            let _ = inst.setattr("path", path_str);
            if let Ok(value) = pythonize(py, &previous) {
                let _ = inst.setattr("previous", value);
            }
            let _ = inst.setattr("current", current);
        }),
        Error::MissingDateKey { index } => create_exc(&MissingDateKeyError::type_object(py), message, |inst| {
            let _ = inst.setattr("index", index);
        }),
        Error::NullDateValue { index } => create_exc(&NullDateValueError::type_object(py), message, |inst| {
            let _ = inst.setattr("index", index);
        }),
        Error::NonStringDateValue { index } => create_exc(&NonStringDateValueError::type_object(py), message, |inst| {
            let _ = inst.setattr("index", index);
        }),
        Error::OutOfOrderRelease {
            index,
            previous,
            current,
        } => create_exc(&OutOfOrderReleaseError::type_object(py), message, |inst| {
            let _ = inst.setattr("index", index);
            let _ = inst.setattr("previous", previous);
            let _ = inst.setattr("current", current);
        }),
    }
}

/// A part of a JSON path.
#[derive(Clone, Debug, Derivative, Display, Eq)]
#[derivative(Hash, PartialEq)]
pub enum Part {
    /// The identifier of an object in an array.
    #[display("{id}")]
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
#[derive(Clone, Debug, Display, Eq, Hash, PartialEq)]
pub enum Id {
    Integer(i64),
    String(String),
}

#[cfg_attr(feature = "python", pyclass(frozen))]
#[derive(Default)]
pub struct Merger {
    rules: HashMap<Vec<String>, Rule>,
    overrides: HashMap<Vec<String>, Strategy>,
}

fn extract_date(release: &Value, index: usize, multiple_releases: bool) -> Result<Value, Error> {
    match release.get("date") {
        Some(Value::String(string)) => Ok(Value::String(string.clone())),
        Some(Value::Null) => {
            if multiple_releases {
                Err(Error::NullDateValue { index })
            } else {
                Ok(Value::Null)
            }
        }
        Some(other) => {
            if multiple_releases {
                Err(Error::NonStringDateValue { index })
            } else {
                Ok(other.clone())
            }
        }
        None => {
            if multiple_releases {
                Err(Error::MissingDateKey { index })
            } else {
                Ok(Value::Null)
            }
        }
    }
}

fn ensure_order(previous_date: Option<&String>, current_date: &Value, index: usize) -> Result<(), Error> {
    if let Some(previous) = previous_date
        && let Value::String(current) = current_date
        && current < previous
    {
        return Err(Error::OutOfOrderRelease {
            index,
            previous: previous.clone(),
            current: current.clone(),
        });
    }
    Ok(())
}

#[cfg(feature = "python")]
fn ensure_object(value: &Value, context: &str) -> PyResult<()> {
    if !value.is_object() {
        let actual_type = match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => unreachable!(),
        };
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "{context} must be an object, not {actual_type}"
        )));
    }
    Ok(())
}

#[cfg(feature = "python")]
fn emit_warnings(py: Python<'_>, warnings: Vec<DuplicateIdWarning>) -> PyResult<()> {
    let warnings_module = py.import("warnings")?;
    let warning_type = DuplicateIdValueWarning::type_object(py);

    for warning in warnings {
        let message = format!(
            "Multiple objects have the `id` value '{}' in the `{}` array",
            warning.id, warning.path
        );

        let warning_instance = warning_type.call1((message,))?;
        warning_instance.setattr("id", warning.id.clone())?;
        warning_instance.setattr("path", warning.path.clone())?;

        warnings_module.call_method1("warn", (warning_instance,))?;
    }
    Ok(())
}

// The Rust implementation of OCDS Merge is simpler than the Python implementation:
//
// - `__init__` doesn't accept a `schema` argument. It doesn't sort releases.
// - `get_rules` expects a dereferenced schema and doesn't accept a `str` value for a filename or a URL.
// - `create_compiled_release` and `create_versioned_release` expect sorted releases and don't sort releases.
// - `Merger` has no `extend` or `append` method.
#[cfg(feature = "python")]
#[pymethods]
impl Merger {
    /// Initialize a reusable `Merger` instance for creating merged releases.
    #[new]
    #[pyo3(signature = (*, rules = None, overrides = None))]
    fn new_py(rules: Option<HashMap<Vec<String>, Rule>>, overrides: Option<HashMap<Vec<String>, Strategy>>) -> Self {
        Self {
            rules: rules.unwrap_or_default(),
            overrides: overrides.unwrap_or_default(),
        }
    }

    /// Merge the **sorted** releases into a compiled release.
    #[pyo3(name = "create_compiled_release")]
    fn create_compiled_release_py<'py>(
        &self,
        py: Python<'py>,
        releases: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let deserialized = releases
            .into_iter()
            .map(|obj| depythonize(&obj))
            .collect::<Result<Vec<Value>, _>>()?;

        let (result, warnings) = py
            .detach(|| self.create_compiled_release(&deserialized))
            .map_err(|e| error_to_pyerr(py, e))?;
        emit_warnings(py, warnings)?;

        Ok(pythonize(py, &result)?.into())
    }

    /// Merge the **sorted** releases into a versioned release.
    #[pyo3(name = "create_versioned_release")]
    fn create_versioned_release_py<'py>(
        &self,
        py: Python<'py>,
        releases: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let mut deserialized = releases
            .into_iter()
            .map(|obj| depythonize(&obj))
            .collect::<Result<Vec<Value>, _>>()?;

        let (result, warnings) = py
            .detach(|| self.create_versioned_release(&mut deserialized))
            .map_err(|e| error_to_pyerr(py, e))?;
        emit_warnings(py, warnings)?;

        Ok(pythonize(py, &result)?.into())
    }

    /// Dereference all `$ref` properties to local definitions.
    #[staticmethod]
    #[pyo3(name = "dereference")]
    fn dereference_py<'py>(py: Python<'py>, schema: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
        let mut schema_value: Value = depythonize(schema)?;
        ensure_object(&schema_value, "Schema")?;

        let result = py.detach(|| {
            Self::dereference(&mut schema_value);
            schema_value
        });

        Ok(pythonize(py, &result)?.into())
    }

    /// Calculate the merge rules from a JSON Schema.
    ///
    /// The key is a JSON path as a tuple, and the value is a merge rule.
    #[staticmethod]
    #[pyo3(name = "get_rules")]
    fn get_rules_py<'py>(py: Python<'py>, schema: &Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
        let schema_value: Value = depythonize(schema)?;
        ensure_object(&schema_value, "Schema")?;

        let properties = schema_value
            .get("properties")
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("Schema must set 'properties'"))?;

        let rules = py.detach(|| Self::get_rules(properties, &[]));

        // Same as `rules_py`.
        let dict = PyDict::new(py);
        for (path, rule) in &rules {
            dict.set_item(PyTuple::new(py, path)?, Py::new(py, rule.clone())?)?;
        }
        Ok(dict.into())
    }

    // The `get_all` parameter is unsufficient, as the key must be a hashable tuple, not an unhashable list.
    // https://pyo3.rs/v0.26.0/class#customizing-the-class
    #[getter]
    #[pyo3(name = "rules")]
    fn rules_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (path, rule) in &self.rules {
            dict.set_item(PyTuple::new(py, path)?, Py::new(py, rule.clone())?)?;
        }
        Ok(dict.into())
    }

    #[getter]
    #[pyo3(name = "overrides")]
    fn overrides_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (path, strategy) in &self.overrides {
            dict.set_item(PyTuple::new(py, path)?, Py::new(py, strategy.clone())?)?;
        }
        Ok(dict.into())
    }
}

// Most records contain a small number of releases. Thus, parallelization is rarely expected to improve performance –
// and it is not worthwhile to maintain code branches based on the number of releases.
impl Merger {
    /// Merge the **sorted** releases into a compiled release.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A release is not an object
    /// - A path is a literal, an object, and/or an array in different releases
    /// - A release has a missing, null, or non-string `date` value, when merging multiple releases
    pub fn create_compiled_release(&self, releases: &[Value]) -> Result<(Value, Vec<DuplicateIdWarning>), Error> {
        let mut flattened = IndexMap::new();
        let mut warnings = Vec::new();
        let multiple_releases = releases.len() > 1;
        let mut previous_date = None;

        for (index, release) in releases.iter().enumerate() {
            if !release.is_object() {
                return Err(Error::NonObjectRelease { index });
            }

            // Store the values of fields that set "omitWhenMerged": true.
            // In OCDS 1.0, `ocid` incorrectly sets "mergeStrategy": "ocdsOmit".
            let ocid = release.get("ocid").unwrap_or(&Value::Null);
            let date = extract_date(release, index, multiple_releases)?;

            if multiple_releases {
                ensure_order(previous_date.as_ref(), &date, index)?;
                if let Value::String(string) = &date {
                    previous_date = Some(string.clone());
                }
            }

            let mut flat = IndexMap::new();
            self.flatten(&mut flat, &[], &mut vec![], false, release, &mut warnings);

            let date_str = match &date {
                Value::String(s) => s,
                Value::Null => "None", // to match Python behavior
                other => &other.to_string(),
            };
            let ocid_str = match &ocid {
                Value::String(s) => s,
                Value::Null => "None", // to match Python behavior
                other => &other.to_string(),
            };

            flattened.extend(flat);
            flattened.insert(vec![field!("ocid")], ocid.clone());
            flattened.insert(vec![field!("date")], date.clone());
            flattened.insert(vec![field!("id")], Value::String(format!("{ocid_str}-{date_str}")));
        }

        flattened.insert(vec![field!("tag")], json!(["compiled"]));

        Ok((Self::unflatten(&flattened)?, warnings))
    }

    /// Merge the **sorted** releases into a versioned release.
    ///
    /// # Note
    ///
    /// The ``"ocid"`` and ``"tag"`` fields of each release are removed in-place.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A release is not an object
    /// - A path is a literal, an object, and/or an array in different releases
    /// - A release has a missing, null, or non-string `date` value, when merging multiple releases
    ///
    /// # Panics
    ///
    /// Panics if a release that passed the object check cannot be converted to an object.
    /// This should not happen under normal circumstances.
    pub fn create_versioned_release(&self, releases: &mut [Value]) -> Result<(Value, Vec<DuplicateIdWarning>), Error> {
        let mut flattened = IndexMap::new();
        let mut warnings = Vec::new();
        let multiple_releases = releases.len() > 1;
        let mut previous_date = None;

        for (index, release) in releases.iter_mut().enumerate() {
            if !release.is_object() {
                return Err(Error::NonObjectRelease { index });
            }

            // Store the values of fields that set "omitWhenMerged": true.
            let id = release["id"].clone();
            let date = extract_date(release, index, multiple_releases)?;

            if multiple_releases {
                ensure_order(previous_date.as_ref(), &date, index)?;
                if let Value::String(string) = &date {
                    previous_date = Some(string.clone());
                }
            }

            let release_object = release.as_object_mut().expect("Release is not an object");
            // Don't version the OCID. (Restore it after flattening.)
            let ocid = release_object.remove("ocid").unwrap_or(Value::Null);
            // Prior to OCDS 1.1.4, `tag` didn't set "omitWhenMerged": true.
            let tag = release_object.remove("tag");

            let mut flat = IndexMap::new();
            self.flatten(&mut flat, &[], &mut vec![], true, release, &mut warnings);

            flattened.insert(vec![field!("ocid")], ocid);

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

        Ok((Self::unflatten(&flattened)?, warnings))
    }

    /// Calculate the merge rules from a JSON Schema.
    #[must_use]
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
            if let Value::Object(object) = value
                && let Some(Value::String(reference)) = object.remove("$ref")
            {
                if visited.contains(&reference) {
                    // If we already visited this $ref in this $ref chain, stop.
                    return;
                }
                if let Some(mut target) = schema.pointer(&reference[1..]).cloned() {
                    f(&mut target, schema, &join!(visited, &reference));
                    *value = target;
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
        &self,
        flattened: &mut IndexMap<Vec<Part>, Value>,
        path: &[Part],
        rule_path: &mut Vec<String>,
        versioned: bool,
        json: &Value,
        warnings: &mut Vec<DuplicateIdWarning>,
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
                                id: Id::Integer(index.try_into().unwrap()), // panics if array too long
                                original: original.cloned(),
                            },
                            None => default_key.clone(),
                        };

                        // Check whether the identifier is used by other objects in the array.
                        if *identifiers.entry(join!(path, &default_key)).or_insert(index) != index {
                            warnings.push(DuplicateIdWarning {
                                id: default_key.to_string(),
                                path: rule_path.join("."),
                            });
                        }

                        self.flatten_key_value(flattened, path, rule_path, versioned, &key, value, warnings);
                    }
                }
            }
            Value::Object(map) => {
                for (key, value) in map {
                    rule_path.push(key.clone());
                    self.flatten_key_value(
                        flattened,
                        path,
                        rule_path,
                        versioned,
                        &Part::Field(key.clone()),
                        value,
                        warnings,
                    );
                    rule_path.pop();
                }
            }
            _ => unreachable!(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn flatten_key_value(
        &self,
        flattened: &mut IndexMap<Vec<Part>, Value>,
        path: &[Part],
        rule_path: &mut Vec<String>,
        versioned: bool,
        key: &Part,
        value: &Value,
        warnings: &mut Vec<DuplicateIdWarning>,
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
                    self.flatten(flattened, &join!(path, key), rule_path, versioned, value, warnings);
                }
            }
        }
    }

    fn unflatten(flattened: &IndexMap<Vec<Part>, Value>) -> Result<Value, Error> {
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
                        if !pointer.is_array() {
                            return Err(Error::InconsistentType {
                                path: key[..position].to_vec(),
                                previous: pointer.clone(),
                                current: "an array".to_string(),
                            });
                        }

                        let index = indices.entry(&key[..=position]).or_insert_with(|| {
                            let mut object = json!({});
                            // If the original object had an `id` value, set it.
                            if let Some(id) = original {
                                object["id"] = id.clone();
                            }
                            let array = pointer.as_array_mut().unwrap();
                            array.push(object);
                            array.len() - 1
                        });
                        pointer = &mut pointer[index.to_owned()];
                    }
                    // The sub-path is to a property of an object.
                    Part::Field(field) => {
                        if !pointer.is_object() {
                            return Err(Error::InconsistentType {
                                path: key[..position].to_vec(),
                                previous: pointer.clone(),
                                current: format!("an object with a '{field}' key"),
                            });
                        }

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

        Ok(unflattened)
    }
}

#[cfg(feature = "python")]
#[pymodule(gil_used = false)]
fn ocdsmerge_rs<'py>(py: Python<'py>, m: &Bound<'py, PyModule>) -> PyResult<()> {
    m.add_class::<Merger>()?;
    m.add_class::<Rule>()?;
    m.add_class::<Strategy>()?;
    m.add("MergeError", py.get_type::<MergeError>())?;
    m.add("NonObjectReleaseError", py.get_type::<NonObjectReleaseError>())?;
    m.add("InconsistentTypeError", py.get_type::<InconsistentTypeError>())?;
    m.add("MissingDateKeyError", py.get_type::<MissingDateKeyError>())?;
    m.add("NullDateValueError", py.get_type::<NullDateValueError>())?;
    m.add("NonStringDateValueError", py.get_type::<NonStringDateValueError>())?;
    m.add("OutOfOrderReleaseError", py.get_type::<OutOfOrderReleaseError>())?;
    m.add("MergeWarning", py.get_type::<MergeWarning>())?;
    m.add("DuplicateIdValueWarning", py.get_type::<DuplicateIdValueWarning>())?;
    Ok(())
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

    // ocds-merge/tests/test_flatten.py::test_flatten_1
    #[test]
    fn flatten_1() {
        let merger = Merger::default();

        let mut flattened = IndexMap::new();
        let mut warnings = Vec::new();

        let item = json!({
            "c": "I am a",
            "b": ["A", "list"],
            "a": [
                {"id": 1, "cb": "I am ca"},
                {"id": 2, "ca": "I am cb"}
            ]
        });

        merger.flatten(&mut flattened, &[], &mut vec![], false, &item, &mut warnings);

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

        assert_eq!(warnings, Vec::new());
        assert_eq!(Merger::unflatten(&flattened).unwrap(), item);
    }

    // ocds-merge/tests/test_flatten.py::test_flatten_2
    #[test]
    fn flatten_2() {
        // OCDS in decimal.
        fastrand::seed(79_67_68_83);

        let merger = Merger::default();

        let mut flattened = IndexMap::new();
        let mut warnings = Vec::new();

        let item = json!({
            "a": [
                {"id": "identifier"},
                {"key": "value"}
            ]
        });

        merger.flatten(&mut flattened, &[], &mut vec![], false, &item, &mut warnings);

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

        assert_eq!(warnings, Vec::new());
        assert_eq!(Merger::unflatten(&flattened).unwrap(), item);
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

    #[test]
    fn create_versioned_release_mutate() {
        let merger = Merger::default();

        let mut data = vec![
            json!({
                "ocid": "ocds-213czf-A",
                "id": "1",
                "date": "2000-01-01T00:00:00Z",
                "tag": ["tender"]
            }),
            json!({
                "ocid": "ocds-213czf-A",
                "id": "2",
                "date": "2000-01-02T00:00:00Z",
                "tag": ["tenderUpdate"]
            }),
        ];

        merger.create_versioned_release(&mut data).unwrap();

        // The "tag" field is removed from the original data.
        assert_eq!(
            data,
            vec![
                json!({
                    "id": "1",
                    "date": "2000-01-01T00:00:00Z"
                }),
                json!({
                    "id": "2",
                    "date": "2000-01-02T00:00:00Z"
                }),
            ]
        );
    }

    // See build.rs
    fn merge(suffix: &str, path: &str, schema: &str) {
        let mut schema = read(&format!("tests/fixtures/{schema}.json"));

        Merger::dereference(&mut schema);

        let merger = Merger {
            rules: Merger::get_rules(&schema["properties"], &[]),
            ..Default::default()
        };

        let mut fixture = read(&format!("{}.json", path.rsplit_once('-').unwrap().0));

        let actual = match suffix {
            "compiled" => merger.create_compiled_release(&fixture.as_array().unwrap()).unwrap().0,
            "versioned" => {
                merger
                    .create_versioned_release(&mut fixture.as_array_mut().unwrap())
                    .unwrap()
                    .0
            }
            _ => unreachable!(),
        };

        assert_eq!(actual, read(path));
    }

    // Test arbitrary precision number handling.
    #[test]
    fn arbitrary_precision_greater_than_u64_max() {
        let merger = Merger::default();

        let data_str = r#"[{
            "ocid": "ocds-213czf-A",
            "id": "1",
            "date": "2000-01-01T00:00:00Z",
            "number": 18446744073709551616
        }]"#;

        let data: Vec<Value> = serde_json::from_str(data_str).unwrap();
        let (result, warnings) = merger.create_compiled_release(&data).unwrap();

        assert_eq!(warnings, Vec::new());
        assert_eq!(result["number"].to_string(), "18446744073709551616");
    }

    #[test]
    fn arbitrary_precision_less_than_i64_min() {
        let merger = Merger::default();

        let data_str = r#"[{
            "ocid": "ocds-213czf-A",
            "id": "1",
            "date": "2000-01-01T00:00:00Z",
            "number": -9223372036854775809
        }]"#;

        let data: Vec<Value> = serde_json::from_str(data_str).unwrap();
        let (result, warnings) = merger.create_compiled_release(&data).unwrap();

        assert_eq!(warnings, Vec::new());
        assert_eq!(result["number"].to_string(), "-9223372036854775809");
    }

    #[test]
    fn arbitrary_precision_float() {
        let merger = Merger::default();

        let data_str = r#"[{
            "ocid": "ocds-213czf-A",
            "id": "1",
            "date": "2000-01-01T00:00:00Z",
            "number": 3.141592653589793238
        }]"#;

        let data: Vec<Value> = serde_json::from_str(data_str).unwrap();
        let (result, warnings) = merger.create_compiled_release(&data).unwrap();

        assert_eq!(warnings, Vec::new());
        assert_eq!(result["number"].to_string(), "3.141592653589793238");
    }

    include!(concat!(env!("OUT_DIR"), "/lib.include"));
}
