use super::RespType;
use crate::redis::command::Command;

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

// TODO: Eventually I should be able to use this function for everything
pub fn serialize_command(command: &Command) -> String {
    match command {
        Command::Set(key, value, expiry) => {
            let mut serialized: Vec<RespType> = vec![
                RespType::BulkString(Some(String::from("SET"))),
                RespType::BulkString(Some(String::from(format!("{}", key)))),
                RespType::BulkString(Some(String::from(format!("{}", value)))),
            ];
            if let Some(x) = expiry {
                serialized.push(RespType::BulkString(Some(String::from(format!("{}", x)))));
            }
            let x = serialize_resp_data(RespType::Array(serialized));
            x
        }
        other @ _ => panic!("Serialization unsupported for {:?}", other),
    }
}
