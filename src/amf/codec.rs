#[derive(Debug, Clone, PartialEq)]
pub enum AmfValue {
    Number(f64),
    Boolean(bool),
    String(String),
    Object(Vec<(String, AmfValue)>),
    EcmaArray(Vec<(String, AmfValue)>),
    StrictArray(Vec<AmfValue>),
    Date(f64, Option<i32>),
    Null,
    Undefined,
}

pub struct Amf0Codec;

impl Amf0Codec {
    pub fn encode(&self, value: &AmfValue) -> Vec<u8> {
        match value {
            AmfValue::Number(n) => {
                let mut buf = vec![0x00];
                buf.extend_from_slice(&n.to_be_bytes());
                buf
            }
            AmfValue::Boolean(b) => {
                vec![if *b { 0x01 } else { 0x00 }]
            }
            AmfValue::String(s) => {
                let mut buf = vec![0x02];
                buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
                buf
            }
            AmfValue::Object(props) => {
                let mut buf = vec![0x03];
                for (name, val) in props {
                    buf.extend_from_slice(&self.encode(&AmfValue::String(name.to_string())));
                    buf.extend_from_slice(&self.encode(val));
                }
                buf.extend_from_slice(&[0x00, 0x00, 0x09]); // Object end
                buf
            }
            AmfValue::Null => vec![0x05],
            _ => vec![0x05], // Default to null for unsupported types
        }
    }

    pub fn decode(&self, data: &[u8]) -> Result<(AmfValue, usize), String> {
        if data.is_empty() {
            return Err("Empty AMF data".into());
        }

        let marker = data[0];
        let mut pos = 1;

        let value = match marker {
            0x00 => {
                if pos + 8 > data.len() {
                    return Err("Insufficient data for Number".into());
                }
                let bytes: [u8; 8] = data[pos..pos+8].try_into().unwrap();
                pos += 8;
                AmfValue::Number(f64::from_be_bytes(bytes))
            }
            0x01 => {
                if pos > data.len() {
                    return Err("Insufficient data for Boolean".into());
                }
                let val = data[pos] != 0;
                pos += 1;
                AmfValue::Boolean(val)
            }
            0x02 => {
                if pos + 2 > data.len() {
                    return Err("Insufficient data for String".into());
                }
                let len = u16::from_be_bytes([data[pos], data[pos+1]]) as usize;
                pos += 2;
                if pos + len > data.len() {
                    return Err("Insufficient data for String value".into());
                }
                let s = String::from_utf8_lossy(&data[pos..pos+len]).to_string();
                pos += len;
                AmfValue::String(s)
            }
            0x03 => {
                // Object
                if pos + 2 > data.len() {
                    return Err("Insufficient data for Object".into());
                }
                let mut props = Vec::new();

                loop {
                    if pos + 3 > data.len() {
                        break;
                    }

                    // Check for object end marker
                    if data[pos] == 0 && data[pos+1] == 0 && data[pos+2] == 0x09 {
                        pos += 3;
                        break;
                    }

                    // Property name length
                    let name_len = u16::from_be_bytes([data[pos], data[pos+1]]) as usize;
                    pos += 2;

                    if pos + name_len > data.len() {
                        return Err("Insufficient data for property name".into());
                    }
                    let name = String::from_utf8_lossy(&data[pos..pos+name_len]).to_string();
                    pos += name_len;

                    // Property value
                    if pos >= data.len() {
                        return Err("Insufficient data for property value".into());
                    }
                    let (val, consumed) = self.decode(&data[pos..])?;
                    pos += consumed;
                    props.push((name, val));
                }

                AmfValue::Object(props)
            }
            0x05 => AmfValue::Null,
            0x06 => AmfValue::Undefined,
            _ => return Err(format!("Unknown AMF marker: 0x{:02X}", marker)),
        };

        Ok((value, pos))
    }
}
