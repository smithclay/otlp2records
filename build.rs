fn main() {
    #[cfg(feature = "ffi")]
    generate_c_header();
}

#[cfg(feature = "ffi")]
fn generate_c_header() {
    use std::{env, fs, path::Path};

    let crate_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let crate_path = Path::new(&crate_dir);

    let include_dir = crate_path.join("include");
    fs::create_dir_all(&include_dir).ok();

    let output_file = include_dir.join("otlp2records_ffi.h");

    println!("cargo:rerun-if-changed=src/ffi.rs");

    let config = cbindgen::Config {
        language: cbindgen::Language::C,
        cpp_compat: true,
        include_guard: Some("OTLP2RECORDS_FFI_H".to_string()),
        no_includes: true,
        includes: vec!["stdint.h".to_string(), "stddef.h".to_string()],
        sys_includes: vec![],
        after_includes: Some(
            r#"
#ifdef __cplusplus
extern "C" {
#endif
"#
            .to_string(),
        ),
        trailer: Some(
            r#"
#ifdef __cplusplus
}
#endif
"#
            .to_string(),
        ),
        documentation: true,
        documentation_style: cbindgen::DocumentationStyle::C,
        ..Default::default()
    };

    match cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
    {
        Ok(bindings) => {
            bindings.write_to_file(&output_file);
            println!("cargo:note=Generated C header at {}", output_file.display());
        }
        Err(e) => {
            println!("cargo:warning=Failed to generate C bindings: {e}");
        }
    }
}
