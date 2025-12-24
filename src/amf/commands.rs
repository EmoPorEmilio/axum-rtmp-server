

pub fn create_connect_response(transaction_id: f64) -> Vec<u8> {
    let mut result = vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID
        0x03, // Object marker
        0x00, 0x05, // Property name length (5)
        b'l', b'e', b'v', b'e', b'l', // "level"
        0x02, // String marker
        0x00, 0x06, // String length (6)
        b's', b't', b'a', b't', b'u', b's', // "status"
        0x00, 0x04, // Property name length (4)
        b'c', b'o', b'd', b'e', // "code"
        0x02, // String marker
        0x00, 0x17, // String length (23)
        b'N', b'e', b't', b'C', b'o', b'n', b'n', b'e', b'c', b't', b'i', b'o', b'n', b'.', b'C',
        b'o', b'n', b'n', b'e', b'c', b't', b'.', b'O', b'K', 0x00, 0x00,
        0x09, // Object end marker
        0x03, // Object marker (properties)
        0x00, 0x0C, // Property name length (12)
        b'c', b'a', b'p', b'a', b'b', b'i', b'l', b'i', b't', b'i', b'e', b's',
        0x00, // Number marker
        0x40, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 31.0
        0x00, 0x00, 0x09, // Object end marker
    ];
    result
}

pub fn create_on_bw_done() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x08, // String length (8)
        b'o', b'n', b'B', b'W', b'D', b'o', b'n', b'e', 0x00, // Number marker
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (0.0)
        0x05, // NULL marker
    ]
}

pub fn create_create_stream_response(stream_id: f64) -> Vec<u8> {
    let mut result = vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x40, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (5.0)
        0x05, // NULL marker
        0x00, // Number marker
        // Convert stream_id to f64 and encode as 8 bytes
        0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Stream ID as double (1.0)
    ];
    result
}

pub fn create_publish_response() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x08, // String length (8)
        b'o', b'n', b'S', b't', b'a', b't', b'u', b's', // "onStatus"
        0x00, // Number marker
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (0)
        0x05, // NULL marker
        0x03, // Object marker
        // level
        0x00, 0x05, // Property name length (5)
        b'l', b'e', b'v', b'e', b'l', 0x02, // String marker
        0x00, 0x06, // String length (6)
        b's', b't', b'a', b't', b'u', b's', // code
        0x00, 0x04, // Property name length (4)
        b'c', b'o', b'd', b'e', 0x02, // String marker
        0x00, 0x17, // String length (23)
        b'N', b'e', b't', b'S', b't', b'r', b'e', b'a', b'm', b'.', b'P', b'u', b'b', b'l', b'i',
        b's', b'h', b'.', b'S', b't', b'a', b'r', b't', // description
        0x00, 0x0B, // Property name length (11)
        b'd', b'e', b's', b'c', b'r', b'i', b'p', b't', b'i', b'o', b'n',
        0x02, // String marker
        0x00, 0x14, // String length (20)
        b'S', b't', b'a', b'r', b't', b' ', b'p', b'u', b'b', b'l', b'i', b's', b'h', b'i', b'n',
        b'g', b' ', b'l', b'i', b'v', b'e', 0x00, 0x00, 0x09, // Object end marker
    ]
}

pub fn create_checkbw_response() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (1.0)
        0x05, // NULL marker
        0x02, // String marker
        0x00, 0x0A, // String length (10)
        b'_', b'c', b'h', b'e', b'c', b'k', b'b', b'w', b'_', b'r', // "_checkbw_r"
    ]
}

pub fn create_release_stream_response() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (3.0)
        0x05, // NULL marker
        0x05, // NULL marker for result
    ]
}

pub fn create_fcpublish_response() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x40, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (4.0)
        0x05, // NULL marker
        0x05, // NULL marker for result
    ]
}

pub fn create_reject_response(reason: &str) -> Vec<u8> {
    let mut result = vec![
        0x02, // String marker
        0x00, 0x08, // String length (8)
        b'o', b'n', b'S', b't', b'a', b't', b'u', b's', // "onStatus"
        0x00, // Number marker
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (0)
        0x05, // NULL marker
        0x03, // Object marker
        // level
        0x00, 0x05, // Property name length (5)
        b'l', b'e', b'v', b'e', b'l', 0x02, // String marker
        0x00, 0x05, // String length (5)
        b'e', b'r', b'r', b'o', b'r', // code
        0x00, 0x04, // Property name length (4)
        b'c', b'o', b'd', b'e', 0x02, // String marker
        0x00, 0x18, // String length (24)
        b'N', b'e', b't', b'S', b't', b'r', b'e', b'a', b'm', b'.', b'P', b'u', b'b', b'l', b'i',
        b's', b'h', b'.', b'R', b'e', b'j', b'e', b'c', b't', b'e', b'd', // description
        0x00, 0x0B, // Property name length (11)
        b'd', b'e', b's', b'c', b'r', b'i', b'p', b't', b'i', b'o', b'n',
        0x02, // String marker
        0x00, (reason.len() as u16).to_be_bytes()[0],
        (reason.len() as u16).to_be_bytes()[1], // String length
    ];
    result.extend_from_slice(reason.as_bytes());
    result.push(0x00);
    result.push(0x00);
    result.push(0x09); // Object end marker
    result
}

pub fn create_rtmp_header(
    chunk_type: u8,
    chunk_stream_id: u8,
    timestamp: u32,
    message_length: u32,
    message_type_id: u8,
    message_stream_id: u32,
) -> Vec<u8> {
    let mut header = Vec::new();

    if chunk_stream_id < 64 {
        header.push((chunk_type << 6) | chunk_stream_id);
    } else {
        header.push(chunk_type << 6);
        header.push(chunk_stream_id - 64);
    }

    match chunk_type {
        0 => {
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
            header.extend_from_slice(&message_length.to_be_bytes()[1..]);
            header.push(message_type_id);
            header.extend_from_slice(&message_stream_id.to_le_bytes());
        }
        1 => {
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
            header.extend_from_slice(&message_length.to_be_bytes()[1..]);
            header.push(message_type_id);
        }
        2 => {
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
        }
        _ => {}
    }

    header
}
