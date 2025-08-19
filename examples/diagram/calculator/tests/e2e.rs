use assert_cmd::Command;

#[test]
fn multiply_by_3() {
    Command::cargo_bin("calculator")
        .unwrap()
        .args(["run", "multiply_by_3.json", "4"])
        .assert()
        .stdout("12.0\n");
}
