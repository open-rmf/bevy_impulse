use assert_cmd::Command;

#[test]
fn multiply3() {
    Command::cargo_bin("calculator")
        .unwrap()
        .args(["multiply3.json", "4"])
        .assert()
        .stdout("12.0\n");
}
