use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

struct Exported {
    name: String,
    body: String,
}

fn exported_functions(source: &str) -> Vec<Exported> {
    let mut found = Vec::new();
    let mut offset = 0;
    let attributes: Vec<usize> = source
        .split_inclusive('\n')
        .filter_map(|line| {
            let start = offset;
            offset += line.len();
            (line.trim() == "#[no_mangle]").then_some(start + line.len())
        })
        .collect();
    for at in attributes {
        let rest = &source[at..];
        let fn_at = rest.find("fn ").expect("#[no_mangle] without fn");
        let name: String = rest[fn_at + 3..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        let body = &rest[fn_at + rest[fn_at..].find('{').unwrap()..];
        let mut depth = 0;
        let end = body
            .char_indices()
            .find_map(|(i, c)| {
                match c {
                    '{' => depth += 1,
                    '}' => depth -= 1,
                    _ => {}
                }
                (depth == 0).then_some(i)
            })
            .unwrap();
        found.push(Exported {
            name,
            body: body[..=end].to_string(),
        });
    }
    found
}

#[test]
fn every_c_abi_entry_point_routes_through_ffi_boundary() {
    let mut sources = Vec::new();
    rust_sources(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut sources,
    );
    let exported: Vec<(&PathBuf, Exported)> = sources
        .iter()
        .flat_map(|path| {
            exported_functions(&fs::read_to_string(path).unwrap())
                .into_iter()
                .map(move |function| (path, function))
        })
        .collect();
    let delegates = |body: &str, me: &str| {
        exported
            .iter()
            .any(|(_, other)| other.name != me && body.contains(&format!("{}(", other.name)))
    };
    let violations: Vec<String> = exported
        .iter()
        .filter(|(_, f)| !f.body.contains("ffi_boundary(") && !delegates(&f.body, &f.name))
        .map(|(path, Exported { name, .. })| {
            format!(
                "{}: `{name}` is #[no_mangle] but its body never calls `ffi_boundary`. \
                 Wrap the body in `ffi_boundary(<value to return on panic>, || {{ ... }})` \
                 (src/bindings/export.rs) or delegate to another #[no_mangle] entry point; \
                 a panic that reaches an extern \"C\" frame aborts the host process.",
                path.display()
            )
        })
        .collect();
    assert!(
        !exported.is_empty(),
        "no #[no_mangle] functions found; is the scan broken?"
    );
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn the_lint_sees_a_missing_boundary() {
    let unguarded = r#"
        #[no_mangle]
        pub unsafe extern "C" fn otlp_ingest_server_stop(server: *mut Server, deadline: u64) {
            if server.is_null() { return; }
            (*server).stop(deadline);
        }
        #[no_mangle]
        pub extern "C" fn otlp_new() -> *mut Decoder {
            ffi_boundary(std::ptr::null_mut(), || Box::into_raw(Box::new(Decoder::default())))
        }
    "#;
    let found = exported_functions(unguarded);
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].name, "otlp_ingest_server_stop");
    assert!(!found[0].body.contains("ffi_boundary("));
    assert!(found[1].body.contains("ffi_boundary("));
}
