use kindlebridge_schema::{LogicalSyncPath, LogicalSyncPathError};

#[test]
fn accepts_a_non_empty_nfc_relative_path() {
    let path = LogicalSyncPath::parse("books/Café.epub").unwrap();

    assert_eq!(path.as_str(), "books/Café.epub");
    assert_eq!(path.clone().into_string(), "books/Café.epub");
    assert_eq!(path.ascii_case_fold_key(), "books/café.epub");
}

#[test]
fn accepts_the_utf8_byte_limits() {
    let component = "a".repeat(255);
    let path = format!("{component}/{}/{}/a", "b".repeat(255), "c".repeat(255));
    assert_eq!(path.len(), 769);
    LogicalSyncPath::parse(path).unwrap();

    let path = [
        "a".repeat(255),
        "b".repeat(255),
        "c".repeat(255),
        "d".repeat(255),
    ]
    .join("/");
    assert_eq!(path.len(), 1_023);
    LogicalSyncPath::parse(path).unwrap();
}

#[test]
fn rejects_non_relative_input_forms() {
    for input in ["", "/books/a.epub", r"books\a.epub"] {
        assert_eq!(
            LogicalSyncPath::parse(input),
            Err(LogicalSyncPathError::NotRelative)
        );
    }
}

#[test]
fn rejects_a_path_over_1024_utf8_bytes() {
    let path = [
        "a".repeat(255),
        "b".repeat(255),
        "c".repeat(255),
        "d".repeat(255),
        "e".to_owned(),
    ]
    .join("/");
    assert_eq!(path.len(), 1_025);
    assert_eq!(
        LogicalSyncPath::parse(path),
        Err(LogicalSyncPathError::TooLong)
    );
}

#[test]
fn rejects_a_component_over_255_utf8_bytes() {
    assert_eq!(
        LogicalSyncPath::parse("é".repeat(128)),
        Err(LogicalSyncPathError::ComponentTooLong)
    );
}

#[test]
fn rejects_empty_dot_and_dot_dot_components() {
    for input in ["books//a", "./books", "books/../a", "books/"] {
        assert_eq!(
            LogicalSyncPath::parse(input),
            Err(LogicalSyncPathError::InvalidComponent)
        );
    }
}

#[test]
fn rejects_control_characters() {
    assert_eq!(
        LogicalSyncPath::parse("books/a\nb"),
        Err(LogicalSyncPathError::ControlCharacter)
    );
}

#[test]
fn rejects_non_nfc_unicode() {
    assert_eq!(
        LogicalSyncPath::parse("books/e\u{301}.epub"),
        Err(LogicalSyncPathError::NotNfc)
    );
}

#[test]
fn ascii_case_fold_leaves_non_ascii_text_unchanged() {
    let path = LogicalSyncPath::parse("Ä/BOOKS/A.epub").unwrap();

    assert_eq!(path.ascii_case_fold_key(), "Ä/books/a.epub");
}
