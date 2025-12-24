use crate::protocol::{
    RTMP_MSG_WINDOW_ACK_SIZE, RTMP_MSG_SET_CHUNK_SIZE, RTMP_MSG_USER_CONTROL,
    RTMP_MSG_AUDIO, RTMP_MSG_VIDEO, RTMP_MSG_AMF3_CMD, RTMP_MSG_AMF3_DATA,
    RTMP_MSG_AMF0_CMD, RTMP_MSG_AMF0_DATA,
    USER_CONTROL_STREAM_BEGIN, USER_CONTROL_STREAM_EOF, USER_CONTROL_STREAM_DRY,
    USER_CONTROL_SET_BUFFER_LENGTH, USER_CONTROL_STREAM_IS_RECORDED,
    USER_CONTROL_PING_REQUEST, USER_CONTROL_PING_RESPONSE,
};
use crate::amf::{AmfValue, Amf0Codec};

#[derive(Debug, Clone, PartialEq)]
pub enum MessageType {
    WindowAckSize(u32),
    SetPeerBandwidth(u32, u8),
    SetChunkSize(u32),
    UserControl(UserControlEvent),
    Amf0Command(AmfCommand),
    Amf0Data(Vec<u8>),
    Audio(Vec<u8>),
    Video(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum UserControlEvent {
    StreamBegin(u32),
    StreamEof(u32),
    StreamDry(u32),
    SetBufferLength(u32, u32),
    StreamIsRecorded(u32),
    PingRequest(u32),
    PingResponse(u32),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AmfCommand {
    Connect { transaction_id: f64, params: Vec<AmfValue> },
    CreateStream { transaction_id: f64 },
    Publish { transaction_id: f64, stream_name: String },
    Play { transaction_id: f64, stream_name: String },
    ReleaseStream { transaction_id: f64, stream_name: String },
    FCPublish { transaction_id: f64, stream_name: String },
    SetDataFrame,
    OnMetaData,
    Unknown { transaction_id: f64, name: String },
}

pub fn dispatch_message(
    msg_type_id: u8,
    data: &[u8],
) -> Result<MessageType, String> {
    match msg_type_id {
        RTMP_MSG_WINDOW_ACK_SIZE => {
            if data.len() < 4 {
                return Err("Invalid window ack size message".into());
            }
            let window_size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            Ok(MessageType::WindowAckSize(window_size))
        }
        RTMP_MSG_SET_CHUNK_SIZE => {
            if data.len() < 4 {
                return Err("Invalid set chunk size message".into());
            }
            let chunk_size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            Ok(MessageType::SetChunkSize(chunk_size))
        }
        RTMP_MSG_USER_CONTROL => {
            if data.len() < 2 {
                return Err("Invalid user control message".into());
            }
            let event_type = u16::from_be_bytes([data[0], data[1]]);
            let event = match event_type {
                USER_CONTROL_STREAM_BEGIN => UserControlEvent::StreamBegin(0),
                USER_CONTROL_STREAM_EOF => UserControlEvent::StreamEof(0),
                USER_CONTROL_STREAM_DRY => UserControlEvent::StreamDry(0),
                USER_CONTROL_SET_BUFFER_LENGTH => UserControlEvent::SetBufferLength(0, 0),
                USER_CONTROL_STREAM_IS_RECORDED => UserControlEvent::StreamIsRecorded(0),
                USER_CONTROL_PING_REQUEST => UserControlEvent::PingRequest(0),
                USER_CONTROL_PING_RESPONSE => UserControlEvent::PingResponse(0),
                _ => return Err(format!("Unknown user control event: {}", event_type)),
            };
            Ok(MessageType::UserControl(event))
        }
        RTMP_MSG_AMF0_CMD | RTMP_MSG_AMF3_CMD => {
            let codec = Amf0Codec;
            let (parsed, _) = codec.decode(data).map_err(|e| format!("AMF decode error: {}", e))?;
            Ok(MessageType::Amf0Command(parse_amf_command(&parsed)?))
        }
        RTMP_MSG_AMF0_DATA | RTMP_MSG_AMF3_DATA => {
            Ok(MessageType::Amf0Data(data.to_vec()))
        }
        RTMP_MSG_AUDIO => Ok(MessageType::Audio(data.to_vec())),
        RTMP_MSG_VIDEO => Ok(MessageType::Video(data.to_vec())),
        _ => Ok(MessageType::Amf0Data(data.to_vec())),
    }
}

fn parse_amf_command(data: &AmfValue) -> Result<AmfCommand, String> {
    match data {
        AmfValue::StrictArray(items) => {
            if items.len() < 2 {
                return Err("AMF command needs at least 2 elements".into());
            }
            let command_name = match &items[0] {
                AmfValue::String(s) => s.clone(),
                _ => return Err("First element should be string command".into()),
            };

            let transaction_id = match &items[1] {
                AmfValue::Number(n) => *n,
                _ => return Err("Second element should be transaction ID".into()),
            };

            Ok(match command_name.as_str() {
                "connect" => {
                    let params = items.get(2).cloned().unwrap_or(AmfValue::Null);
                    AmfCommand::Connect { transaction_id, params: vec![params] }
                }
                "createStream" => AmfCommand::CreateStream { transaction_id },
                "publish" => {
                    let stream_name = match items.get(3) {
                        Some(AmfValue::String(s)) => s.clone(),
                        _ => return Err("Publish needs stream name".into()),
                    };
                    AmfCommand::Publish { transaction_id, stream_name }
                }
                "play" => {
                    let stream_name = match items.get(3) {
                        Some(AmfValue::String(s)) => s.clone(),
                        _ => return Err("Play needs stream name".into()),
                    };
                    AmfCommand::Play { transaction_id, stream_name }
                }
                "releaseStream" => {
                    let stream_name = match items.get(3) {
                        Some(AmfValue::String(s)) => s.clone(),
                        _ => return Err("ReleaseStream needs stream name".into()),
                    };
                    AmfCommand::ReleaseStream { transaction_id, stream_name }
                }
                "FCPublish" => {
                    let stream_name = match items.get(3) {
                        Some(AmfValue::String(s)) => s.clone(),
                        _ => return Err("FCPublish needs stream name".into()),
                    };
                    AmfCommand::FCPublish { transaction_id, stream_name }
                }
                "@setDataFrame" => AmfCommand::SetDataFrame,
                "onMetaData" => AmfCommand::OnMetaData,
                _ => AmfCommand::Unknown { transaction_id, name: command_name },
            })
        }
        _ => Err("AMF command should be an array".into()),
    }
}
