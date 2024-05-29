use super::RespType;

fn serialize_bulk_string(data: String) -> String {
    let str_len = data.len();
    String::from(&format!("${}\r\n{}\r\n", str_len, data))
}

fn serialize_array(data: Vec<RespType>) -> String {
    let length = data.len();
    let mut serialized = String::from(&format!("*{}\r\n", length));
    for x in data {
        serialized.push_str(&serialize_resp_data(x));
    }
    serialized
}

pub fn serialize_resp_data(data: RespType) -> String {
    let serialized = match data {
        RespType::BulkString(x) => serialize_bulk_string(x.as_ref().unwrap().to_string()),
        RespType::Array(x) => serialize_array(x),
        other @ _ => panic!("Serialization isn't support for {:?}", other),
    };
    serialized
}
