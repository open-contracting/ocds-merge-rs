use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use glob::glob;

fn main() {
    // https://pyo3.rs/v0.26.0/building-and-distribution.html#macos
    // Only configure PyO3 when the "python" feature is enabled.
    #[cfg(feature = "python")]
    {
        pyo3_build_config::add_extension_module_link_args();
    }

    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("lib.include");
    let mut file = File::create(path).unwrap();

    for (directory, schema) in [
        ("1.1", "release-schema-1__1__4"),
        ("1.0", "release-schema-1__0__3"),
        ("schema", "schema"),
    ] {
        for suffix in ["compiled", "versioned"] {
            for entry in glob(format!("tests/fixtures/{directory}/*-{suffix}.json").as_str())
                .expect("Failed to read glob pattern")
            {
                let path = entry.unwrap();
                let name = path.file_stem().unwrap().to_str().unwrap();
                let function = format!("{name}_{directory}").replace(['-', '.'], "_");

                write!(
                    file,
                    r#"
#[test]
fn {function}() {{
    merge("{suffix}", r"{}", "{schema}")
}}
"#,
                    path.display()
                )
                .unwrap();
            }
        }
    }
}
