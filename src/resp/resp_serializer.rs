pub fn serialize_to_bulk_string(data: String) -> String {
    let str_len = data.len();
    String::from(&format!("${}\r\n{}\r\n", str_len, data))
}
