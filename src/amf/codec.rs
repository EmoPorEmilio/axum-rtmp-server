// AMF0 Type Markers
pub const AMF0_NUMBER: u8 = 0x00;
pub const AMF0_BOOLEAN: u8 = 0x01;
pub const AMF0_STRING: u8 = 0x02;
pub const AMF0_OBJECT: u8 = 0x03;
pub const AMF0_NULL: u8 = 0x05;
pub const AMF0_UNDEFINED: u8 = 0x06;
pub const AMF0_ECMA_ARRAY: u8 = 0x08;
pub const AMF0_OBJECT_END: u8 = 0x09;
pub const AMF0_STRICT_ARRAY: u8 = 0x0A;
pub const AMF0_DATE: u8 = 0x0B;
pub const AMF0_LONG_STRING: u8 = 0x0C;

#[derive(Debug, Clone, PartialEq)]
pub enum AmfValue {
    Number(f64),
    Boolean(bool),
    String(String),
    Object(Vec<(String, AmfValue)>),
    EcmaArray(Vec<(String, AmfValue)>),
    StrictArray(Vec<AmfValue>),
    Date(f64, Option<i16>),
    Null,
    Undefined,
}

impl AmfValue {
    /// Check if this value is null or undefined
    pub fn is_null(&self) -> bool {
        matches!(self, AmfValue::Null | AmfValue::Undefined)
    }

    /// Try to get this value as a string
    pub fn as_string(&self) -> Option<&str> {
        match self {
            AmfValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get this value as a number
    pub fn as_number(&self) -> Option<f64> {
        match self {
            AmfValue::Number(n) => Some(*n),
            _ => None,
        }
    }

    /// Try to get this value as a boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            AmfValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get this value as an object (returns properties)
    pub fn as_object(&self) -> Option<&[(String, AmfValue)]> {
        match self {
            AmfValue::Object(props) | AmfValue::EcmaArray(props) => Some(props),
            _ => None,
        }
    }

    /// Get a property from an object by name
    pub fn get(&self, key: &str) -> Option<&AmfValue> {
        self.as_object()?.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
}

pub struct Amf0Codec;

impl Default for Amf0Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl Amf0Codec {
    pub fn new() -> Self {
        Self
    }

    pub fn encode(&self, value: &AmfValue) -> Vec<u8> {
        match value {
            AmfValue::Number(n) => {
                let mut buf = vec![AMF0_NUMBER];
                buf.extend_from_slice(&n.to_be_bytes());
                buf
            }
            AmfValue::Boolean(b) => {
                // FIXED: Boolean encoding requires marker byte 0x01 followed by value
                vec![AMF0_BOOLEAN, if *b { 0x01 } else { 0x00 }]
            }
            AmfValue::String(s) => {
                if s.len() > 65535 {
                    // Long string
                    let mut buf = vec![AMF0_LONG_STRING];
                    buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                    buf
                } else {
                    let mut buf = vec![AMF0_STRING];
                    buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                    buf
                }
            }
            AmfValue::Object(props) => {
                let mut buf = vec![AMF0_OBJECT];
                for (name, val) in props {
                    // Object property names are NOT prefixed with type marker
                    buf.extend_from_slice(&(name.len() as u16).to_be_bytes());
                    buf.extend_from_slice(name.as_bytes());
                    buf.extend_from_slice(&self.encode(val));
                }
                buf.extend_from_slice(&[0x00, 0x00, AMF0_OBJECT_END]); // Object end
                buf
            }
            AmfValue::EcmaArray(props) => {
                let mut buf = vec![AMF0_ECMA_ARRAY];
                // ECMA Array has a count prefix (4 bytes)
                buf.extend_from_slice(&(props.len() as u32).to_be_bytes());
                for (name, val) in props {
                    buf.extend_from_slice(&(name.len() as u16).to_be_bytes());
                    buf.extend_from_slice(name.as_bytes());
                    buf.extend_from_slice(&self.encode(val));
                }
                buf.extend_from_slice(&[0x00, 0x00, AMF0_OBJECT_END]); // Object end
                buf
            }
            AmfValue::StrictArray(items) => {
                let mut buf = vec![AMF0_STRICT_ARRAY];
                buf.extend_from_slice(&(items.len() as u32).to_be_bytes());
                for item in items {
                    buf.extend_from_slice(&self.encode(item));
                }
                buf
            }
            AmfValue::Date(timestamp, timezone) => {
                let mut buf = vec![AMF0_DATE];
                buf.extend_from_slice(&timestamp.to_be_bytes());
                buf.extend_from_slice(&timezone.unwrap_or(0).to_be_bytes());
                buf
            }
            AmfValue::Null => vec![AMF0_NULL],
            AmfValue::Undefined => vec![AMF0_UNDEFINED],
        }
    }

    pub fn decode(&self, data: &[u8]) -> Result<(AmfValue, usize), String> {
        if data.is_empty() {
            return Err("Empty AMF data".into());
        }

        let marker = data[0];
        let mut pos = 1;

        let value = match marker {
            AMF0_NUMBER => {
                if pos + 8 > data.len() {
                    return Err("Insufficient data for Number".into());
                }
                let bytes: [u8; 8] = data[pos..pos+8].try_into().unwrap();
                pos += 8;
                AmfValue::Number(f64::from_be_bytes(bytes))
            }
            AMF0_BOOLEAN => {
                if pos >= data.len() {
                    return Err("Insufficient data for Boolean".into());
                }
                let val = data[pos] != 0;
                pos += 1;
                AmfValue::Boolean(val)
            }
            AMF0_STRING => {
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
            AMF0_LONG_STRING => {
                if pos + 4 > data.len() {
                    return Err("Insufficient data for Long String".into());
                }
                let len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                pos += 4;
                if pos + len > data.len() {
                    return Err("Insufficient data for Long String value".into());
                }
                let s = String::from_utf8_lossy(&data[pos..pos+len]).to_string();
                pos += len;
                AmfValue::String(s)
            }
            AMF0_OBJECT => {
                let (props, consumed) = self.decode_object_properties(&data[pos..])?;
                pos += consumed;
                AmfValue::Object(props)
            }
            AMF0_ECMA_ARRAY => {
                // ECMA Array: 4-byte count (often unreliable) + properties
                if pos + 4 > data.len() {
                    return Err("Insufficient data for ECMA Array".into());
                }
                // Skip the count - we'll read until end marker
                pos += 4;
                let (props, consumed) = self.decode_object_properties(&data[pos..])?;
                pos += consumed;
                AmfValue::EcmaArray(props)
            }
            AMF0_STRICT_ARRAY => {
                if pos + 4 > data.len() {
                    return Err("Insufficient data for Strict Array".into());
                }
                let count = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                pos += 4;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    let (item, consumed) = self.decode(&data[pos..])?;
                    pos += consumed;
                    items.push(item);
                }
                AmfValue::StrictArray(items)
            }
            AMF0_DATE => {
                if pos + 10 > data.len() {
                    return Err("Insufficient data for Date".into());
                }
                let timestamp = f64::from_be_bytes(data[pos..pos+8].try_into().unwrap());
                pos += 8;
                let timezone = i16::from_be_bytes([data[pos], data[pos+1]]);
                pos += 2;
                AmfValue::Date(timestamp, Some(timezone))
            }
            AMF0_NULL => AmfValue::Null,
            AMF0_UNDEFINED => AmfValue::Undefined,
            _ => return Err(format!("Unknown AMF marker: 0x{:02X}", marker)),
        };

        Ok((value, pos))
    }

    /// Decode object properties until end marker is found
    fn decode_object_properties(&self, data: &[u8]) -> Result<(Vec<(String, AmfValue)>, usize), String> {
        let mut pos = 0;
        let mut props = Vec::new();

        loop {
            if pos + 3 > data.len() {
                return Err("Insufficient data for object properties".into());
            }

            // Check for object end marker (0x00 0x00 0x09)
            if data[pos] == 0 && data[pos+1] == 0 && data[pos+2] == AMF0_OBJECT_END {
                pos += 3;
                break;
            }

            // Property name (length-prefixed string without type marker)
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

        Ok((props, pos))
    }

    /// Decode multiple AMF values from a byte slice
    pub fn decode_all(&self, data: &[u8]) -> Result<Vec<AmfValue>, String> {
        let mut values = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            let (value, consumed) = self.decode(&data[pos..])?;
            values.push(value);
            pos += consumed;
        }

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_number() {
        let codec = Amf0Codec::new();
        let value = AmfValue::Number(42.5);
        let encoded = codec.encode(&value);
        let (decoded, _) = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_encode_decode_boolean_true() {
        let codec = Amf0Codec::new();
        let value = AmfValue::Boolean(true);
        let encoded = codec.encode(&value);

        // Verify encoding is correct: marker (0x01) + value (0x01)
        assert_eq!(encoded, vec![0x01, 0x01]);

        let (decoded, _) = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_encode_decode_boolean_false() {
        let codec = Amf0Codec::new();
        let value = AmfValue::Boolean(false);
        let encoded = codec.encode(&value);

        // Verify encoding is correct: marker (0x01) + value (0x00)
        assert_eq!(encoded, vec![0x01, 0x00]);

        let (decoded, _) = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_encode_decode_string() {
        let codec = Amf0Codec::new();
        let value = AmfValue::String("hello world".to_string());
        let encoded = codec.encode(&value);
        let (decoded, _) = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_encode_decode_null() {
        let codec = Amf0Codec::new();
        let value = AmfValue::Null;
        let encoded = codec.encode(&value);
        assert_eq!(encoded, vec![0x05]);
        let (decoded, _) = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_encode_decode_undefined() {
        let codec = Amf0Codec::new();
        let value = AmfValue::Undefined;
        let encoded = codec.encode(&value);
        assert_eq!(encoded, vec![0x06]);
        let (decoded, _) = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_encode_decode_object() {
        let codec = Amf0Codec::new();
        let value = AmfValue::Object(vec![
            ("name".to_string(), AmfValue::String("test".to_string())),
            ("value".to_string(), AmfValue::Number(123.0)),
        ]);
        let encoded = codec.encode(&value);
        let (decoded, _) = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_encode_decode_strict_array() {
        let codec = Amf0Codec::new();
        let value = AmfValue::StrictArray(vec![
            AmfValue::Number(1.0),
            AmfValue::String("two".to_string()),
            AmfValue::Boolean(true),
        ]);
        let encoded = codec.encode(&value);
        let (decoded, _) = codec.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_amf_value_helpers() {
        let obj = AmfValue::Object(vec![
            ("code".to_string(), AmfValue::String("NetConnection.Connect.Success".to_string())),
            ("level".to_string(), AmfValue::String("status".to_string())),
        ]);

        assert!(obj.as_object().is_some());
        assert_eq!(
            obj.get("code").and_then(|v| v.as_string()),
            Some("NetConnection.Connect.Success")
        );
        assert_eq!(
            obj.get("level").and_then(|v| v.as_string()),
            Some("status")
        );
        assert!(obj.get("nonexistent").is_none());
    }

    #[test]
    fn test_decode_all() {
        let codec = Amf0Codec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&codec.encode(&AmfValue::String("connect".to_string())));
        data.extend_from_slice(&codec.encode(&AmfValue::Number(1.0)));
        data.extend_from_slice(&codec.encode(&AmfValue::Null));

        let values = codec.decode_all(&data).unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], AmfValue::String("connect".to_string()));
        assert_eq!(values[1], AmfValue::Number(1.0));
        assert_eq!(values[2], AmfValue::Null);
    }
}
