use bevy_impulse::Diagram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = schemars::schema_for!(Diagram);
    let f = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open("diagram.schema.json")
        .unwrap();
    serde_json::to_writer_pretty(f, &schema)?;
    Ok(())
}

#[cfg(test)]
mod diagram {
    mod test {
        use super::super::*;
        use std::iter::zip;

        #[cfg(not(target_os = "windows"))]
        #[test]
        fn check_schema_changes() -> Result<(), String> {
            let cur_schema_json = std::fs::read("diagram.schema.json").unwrap();
            let schema = schemars::schema_for!(Diagram);
            let new_schema_json = serde_json::to_vec_pretty(&schema).unwrap();

            if cur_schema_json.len() != new_schema_json.len()
                || zip(cur_schema_json, new_schema_json).any(|(a, b)| a != b)
            {
                return Err(String::from("There are changes in the json schema, please run `cargo run -F=diagram generate_schema` to regenerate it"));
            }
            Ok(())
        }
    }
}
