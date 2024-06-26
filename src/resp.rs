pub mod resp_deserializer;
pub mod resp_serializer;

#[derive(Debug)]
pub enum RespType {
    Integer(i64),
    SimpleString(String),
    Error(String),
    BulkString(Option<String>),
    Array(Vec<RespType>),
}
