use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{BufMut, Bytes, BytesMut};
use crate::settings::TransportTypeEnum;


#[derive(Debug, PartialEq)]
pub enum TransportParseError {
    InvalidPacketType(u8),
    MalformedRemainingLength,
    TopicTooLong,
    MissingPacketId,
    InsufficientData,
}


fn encode_base64(data: &[u8]) -> String {
    STANDARD.encode(data)
}

fn decode_base64(s: &str) -> Result<Vec<u8>, TransportParseError> {
    STANDARD.decode(s).map_err(|_| TransportParseError::InsufficientData)
}

const B85_ALPHABET: &[u8; 85] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*+-;<=>?@^_`{|}~";

// Encode like Python base64.b85encode (pad=False): zero-pad the last chunk to 4 bytes,
// then drop `padding` chars from the final group.
fn encode_base85(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() + 3) / 4 * 5);
    for chunk in data.chunks(4) {
        let mut word = [0u8; 4];
        word[..chunk.len()].copy_from_slice(chunk);
        let mut value = u32::from_be_bytes(word) as u64;
        let mut digits = [0u8; 5];
        for i in (0..5).rev() {
            digits[i] = B85_ALPHABET[(value % 85) as usize];
            value /= 85;
        }
        out.push_str(std::str::from_utf8(&digits).unwrap());
    }
    let padding = (4 - data.len() % 4) % 4;
    if padding > 0 {
        out.truncate(out.len() - padding);
    }
    out
}

// Decode like Python base64.b85decode: pad with '~' to a multiple of 5, then drop the tail bytes.
fn decode_base85(s: &str) -> Result<Vec<u8>, TransportParseError> {
    let bytes = s.as_bytes();
    let padding = (5 - bytes.len() % 5) % 5;
    let mut lookup = [255u8; 128];
    for (i, &c) in B85_ALPHABET.iter().enumerate() {
        lookup[c as usize] = i as u8;
    }
    let mut padded: Vec<u8> = bytes.to_vec();
    padded.resize(padded.len() + padding, b'~');
    let mut out = Vec::with_capacity(padded.len() / 5 * 4);
    for chunk in padded.chunks(5) {
        let mut value: u64 = 0;
        for &c in chunk {
            if c >= 128 || lookup[c as usize] == 255 {
                return Err(TransportParseError::InsufficientData);
            }
            value = value * 85 + lookup[c as usize] as u64;
        }
        let word = u32::try_from(value).map_err(|_| TransportParseError::InsufficientData)?;
        out.extend_from_slice(&word.to_be_bytes());
    }
    if padding > 0 {
        out.truncate(out.len() - padding);
    }
    Ok(out)
}

fn encode_data(data: &[u8], base_value: usize) -> String {
    if base_value == 85 {
        encode_base85(data)
    } else {
        encode_base64(data)
    }
}

fn decode_data(s: &str, base_value: usize) -> Result<Vec<u8>, TransportParseError> {
    if base_value == 85 {
        decode_base85(s)
    } else {
        decode_base64(s)
    }
}

const HTTP_USER_AGENT: &str = "python-requests/2.31.0";
const HTTP_SERVER_NAME: &str = "nginx/1.31.6";

fn http_date() -> String {
    chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

fn create_http_packet(payload: &Bytes, topic: uuid::Uuid, base_value: usize, server_side: bool) -> Bytes {
    let topic_str = topic.to_string();
    let encoded = encode_data(payload.as_ref(), base_value);
    let escaped = escape_json_string(&encoded);
    if server_side {
        let json_body = format!(r#"{{"name":"{}","img":"{}"}}"#, topic_str, escaped);
        let response_line = "HTTP/1.1 200 OK\r\n";
        let headers = format!(
            "Server: {}\r\nDate: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\nStrict-Transport-Security: max-age=36000\r\n\r\n",
            HTTP_SERVER_NAME,
            http_date(),
            json_body.len()
        );
        let mut buf = BytesMut::new();
        buf.extend_from_slice(response_line.as_bytes());
        buf.extend_from_slice(headers.as_bytes());
        buf.extend_from_slice(json_body.as_bytes());
        buf.freeze()
    } else {
        let json_body = format!(r#"{{"img":"{}"}}"#, escaped);
        let request_line = format!("POST /image/{}.json HTTP/1.1\r\n", topic_str);
        let headers = format!(
            "Host: localhost\r\nUser-Agent: {}\r\nAccept: */*\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            HTTP_USER_AGENT,
            json_body.len()
        );
        let mut buf = BytesMut::new();
        buf.extend_from_slice(request_line.as_bytes());
        buf.extend_from_slice(headers.as_bytes());
        buf.extend_from_slice(json_body.as_bytes());
        buf.freeze()
    }
}

pub fn forbidden_response() -> Bytes {
    // Typical HTTP 403 Forbidden response
    let body = br#"{"error":"forbidden"}"#;
    let response = format!(
        "HTTP/1.1 403 Forbidden\r\nServer: {}\r\nDate: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\nStrict-Transport-Security: max-age=36000\r\n\r\n{}",
        HTTP_SERVER_NAME,
        http_date(),
        body.len(),
        std::str::from_utf8(body).unwrap()
    );
    Bytes::from(response)
}

fn create_mqtt_packet(payload: &Bytes, topic: uuid::Uuid) -> Bytes {
    // QoS = 1, Retain = false, DUP = false
    let qos: u8 = 1;
    let retain = false;
    let dup = false;
    let flags = ((dup as u8) << 3) | ((qos & 0x03) << 1) | (retain as u8);
    let first_byte = 0x30 | flags; // 0x30 = PUBLISH packet type
    let topic_str = topic.to_string();
    let topic_bytes = topic_str.as_bytes();
    let topic_len = topic_bytes.len() as u16;
    let packet_id: u16 = 1;
    let variable_header_len = 2 + topic_len as usize + 2 /*packet id*/ + 1 /*properties len varint*/;
    let remaining_len = variable_header_len + payload.len();
    let mut buf = BytesMut::new();
    buf.put_u8(first_byte);
    // Remaining Length как VarByteInt
    let mut rem = remaining_len;
    loop {
        let mut byte = (rem % 128) as u8;
        rem /= 128;
        if rem > 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if rem == 0 {
            break;
        }
    }
    // Topic Name
    buf.extend_from_slice(&topic_len.to_be_bytes());
    buf.extend_from_slice(topic_bytes);
    // Packet Identifier
    buf.extend_from_slice(&packet_id.to_be_bytes());
    // Properties Length = 0
    buf.put_u8(0x00);
    // Payload
    buf.extend_from_slice(payload);
    buf.freeze()
}

pub fn create_packet(payload: &Bytes, topic: uuid::Uuid, transport: TransportTypeEnum, base_value: usize, server_side: bool) -> Bytes {
    match transport {
        TransportTypeEnum::Mqtt => {
            create_mqtt_packet(payload, topic)
        },
        TransportTypeEnum::Http => {
            create_http_packet(payload, topic, base_value, server_side)
        }
    }
}

pub fn extract_http_payload(packet: &Bytes, base_value: usize, server_side: bool) -> Result<(String, u8, bool, Bytes), TransportParseError> {
    let data = std::str::from_utf8(packet).map_err(|_| TransportParseError::InsufficientData)?;
    if server_side {
        let mut lines = data.split("\r\n");
        let request_line = lines.next().ok_or(TransportParseError::InsufficientData)?;
        let parts: Vec<&str> = request_line.split_whitespace().collect();
        if parts.len() < 2 {
            return Err(TransportParseError::InsufficientData);
        }
        let path = parts[1];
        let prefix = "/image/";
        let suffix = ".json";
        if !path.starts_with(prefix) || !path.ends_with(suffix) {
            return Err(TransportParseError::InsufficientData);
        }
        let topic = &path[prefix.len()..path.len() - suffix.len()];
        let rest = data[request_line.len()..].trim_start_matches("\r\n");
        let header_end = rest.find("\r\n\r\n").ok_or(TransportParseError::InsufficientData)?;
        let body_str = &rest[header_end + 4..];
        let img_marker = "\"img\":\"";
        let start_idx = body_str.find(img_marker).ok_or(TransportParseError::InsufficientData)? + img_marker.len();
        let mut encoded_chars = Vec::new();
        let mut i = start_idx;
        let bytes = body_str.as_bytes();
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'"' {
                break;
            }
            if b == b'\\' {
                i += 1;
                if i >= bytes.len() { break; }
                match bytes[i] {
                    b'"' => encoded_chars.push(b'"'),
                    b'\\' => encoded_chars.push(b'\\'),
                    b'n' => encoded_chars.push(b'\n'),
                    b'r' => encoded_chars.push(b'\r'),
                    b't' => encoded_chars.push(b'\t'),
                    _ => encoded_chars.push(bytes[i]),
                }
            } else {
                encoded_chars.push(b);
            }
            i += 1;
        }
        let encoded = String::from_utf8(encoded_chars).map_err(|_| TransportParseError::InsufficientData)?;
        let payload_bytes = decode_data(&encoded, base_value).map_err(|_| TransportParseError::InsufficientData)?;
        Ok((topic.to_string(), 0, false, Bytes::from(payload_bytes)))
    } else {
        let header_end = data.find("\r\n\r\n").ok_or(TransportParseError::InsufficientData)?;
        let body_str = &data[header_end + 4..];
        let name_marker = "\"name\":\"";
        let name_start = body_str.find(name_marker).ok_or(TransportParseError::InsufficientData)? + name_marker.len();
        let mut topic_chars = Vec::new();
        let bytes = body_str.as_bytes();
        let mut i = name_start;
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'"' { break; }
            topic_chars.push(b);
            i += 1;
        }
        let topic = String::from_utf8(topic_chars).map_err(|_| TransportParseError::InsufficientData)?;
        let img_marker = "\"img\":\"";
        let img_start = body_str.find(img_marker).ok_or(TransportParseError::InsufficientData)? + img_marker.len();
        let mut encoded_chars = Vec::new();
        i = img_start;
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'"' { break; }
            if b == b'\\' {
                i += 1;
                if i >= bytes.len() { break; }
                match bytes[i] {
                    b'"' => encoded_chars.push(b'"'),
                    b'\\' => encoded_chars.push(b'\\'),
                    b'n' => encoded_chars.push(b'\n'),
                    b'r' => encoded_chars.push(b'\r'),
                    b't' => encoded_chars.push(b'\t'),
                    _ => encoded_chars.push(bytes[i]),
                }
            } else {
                encoded_chars.push(b);
            }
            i += 1;
        }
        let encoded = String::from_utf8(encoded_chars).map_err(|_| TransportParseError::InsufficientData)?;
        let payload_bytes = decode_data(&encoded, base_value).map_err(|_| TransportParseError::InsufficientData)?;
        Ok((topic, 0, false, Bytes::from(payload_bytes)))
    }
}

pub fn extract_mqtt_payload(packet: &Bytes) -> Result<(String, u8, bool, Bytes), TransportParseError> {
    if packet.is_empty() {
        return Err(TransportParseError::InsufficientData);
    }
    let mut cursor = 0;
    // Read Fixed Header
    let first_byte = packet[cursor];
    cursor += 1;
    let packet_type = first_byte >> 4;
    if packet_type != 3 {
        return Err(TransportParseError::InvalidPacketType(packet_type));
    }
    let flags = first_byte & 0x0F;
    let qos = (flags >> 1) & 0x03;
    let retain = flags & 1 == 1;
    if qos > 2 {
        return Err(TransportParseError::MalformedRemainingLength);
    }
    // Read Remaining Length
    let (remaining_len, bytes_consumed) = read_var_byte_int(&packet[cursor..])?;
    cursor += bytes_consumed;
    if packet.len() < cursor + remaining_len {
        return Err(TransportParseError::InsufficientData);
    }
    let end_of_packet = cursor + remaining_len;
    // Read Topic Name
    if packet.len() < cursor + 2 {
        return Err(TransportParseError::InsufficientData);
    }
    let topic_len = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as usize;
    cursor += 2;
    if topic_len > 0xFFFF || packet.len() < cursor + topic_len {
        return Err(TransportParseError::TopicTooLong);
    }
    let topic_bytes = &packet[cursor..cursor + topic_len];
    let topic = String::from_utf8_lossy(topic_bytes).to_string();
    cursor += topic_len;
    // Read Packet Identifier (if QoS > 0)
    if qos > 0 {
        if packet.len() < cursor + 2 {
            return Err(TransportParseError::MissingPacketId);
        }
        cursor += 2;
    }
    // Read Properties Length and skip properties
    let (properties_len, bytes_consumed) = read_var_byte_int(&packet[cursor..])?;
    cursor += bytes_consumed;
    if packet.len() < cursor + properties_len {
        return Err(TransportParseError::InsufficientData);
    }
    cursor += properties_len;
    // Extract Payload
    let payload_len = end_of_packet - cursor;
    if packet.len() < cursor + payload_len {
        return Err(TransportParseError::InsufficientData);
    }
    let payload = packet.slice(cursor..cursor + payload_len);
    Ok((topic, qos, retain, payload))
}

fn read_var_byte_int(data: &[u8]) -> Result<(usize, usize), TransportParseError> {
    let mut multiplier = 1;
    let mut value = 0;
    let mut bytes_consumed = 0;
    for &byte in data.iter() {
        value += ((byte & 0x7F) as usize) * multiplier;
        multiplier *= 128;
        bytes_consumed += 1;
        if byte & 0x80 == 0 {
            return Ok((value, bytes_consumed));
        }
        if bytes_consumed > 4 {
            return Err(TransportParseError::MalformedRemainingLength);
        }
    }
    Err(TransportParseError::MalformedRemainingLength)
}

pub fn extract_payload(
    packet: &Bytes, transport: TransportTypeEnum, base_value: usize, server_side: bool
) -> Result<(String, u8, bool, Bytes), TransportParseError> {
    match transport {
        TransportTypeEnum::Mqtt => {
            extract_mqtt_payload(packet)
        },
        TransportTypeEnum::Http => {
            extract_http_payload(packet, base_value, server_side)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_extract_payload_simple() {
        let mut packet_vec = vec![0x30];
        packet_vec.push(0x0C);
        packet_vec.extend_from_slice(&[0x00, 0x04]);
        packet_vec.extend_from_slice(b"test");
        packet_vec.push(0x00);
        packet_vec.extend_from_slice(b"hello");
        let bytes = Bytes::from(packet_vec);
        let (topic, qos, retain, payload) = extract_payload(&bytes, TransportTypeEnum::Mqtt, 64, false).unwrap();
        assert_eq!(topic, "test");
        assert_eq!(qos, 0);
        assert!(!retain);
        assert_eq!(payload.as_ref(), b"hello");
    }

    #[test]
    fn test_extract_payload_with_qos1() {
        let mut packet_vec = vec![0x32];
        packet_vec.push(0x0A);
        packet_vec.extend_from_slice(&[0x00, 0x01]);
        packet_vec.extend_from_slice(b"t");
        packet_vec.extend_from_slice(&[0x30, 0x39]);
        packet_vec.push(0x00);
        packet_vec.extend_from_slice(b"data");
        let bytes = Bytes::from(packet_vec);
        let (topic, qos, retain, payload) = extract_payload(&bytes, TransportTypeEnum::Mqtt, 64, false).unwrap();
        assert_eq!(topic, "t");
        assert_eq!(qos, 1);
        assert!(!retain);
        assert_eq!(payload.as_ref(), b"data");
    }

    #[test]
    fn test_extract_invalid_type() {
        let packet = Bytes::from_static(&[0x40, 0x02]);
        assert_eq!(extract_payload(&packet, TransportTypeEnum::Mqtt, 64, false).unwrap_err(), TransportParseError::InvalidPacketType(4));
    }

    #[test]
    fn test_create_packet_structure() {
        let topic_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let payload = Bytes::from_static(b"test_payload");
        let packet = create_packet(&payload, topic_uuid, TransportTypeEnum::Mqtt, 64, false);
        assert_eq!(packet[0], 0x32);
        // Remaining Length: 53 -> 0x35
        assert_eq!(packet[1], 0x35);

        let topic_str = topic_uuid.to_string();
        let expected_topic_len = topic_str.len() as u16;
        let actual_topic_len_bytes = [packet[2], packet[3]];
        assert_eq!(u16::from_be_bytes(actual_topic_len_bytes), expected_topic_len);
        let start_topic = 4;
        let end_topic = 4 + topic_str.len();
        assert_eq!(&packet[start_topic..end_topic], topic_str.as_bytes());
        let start_packet_id = end_topic;
        let end_packet_id = start_packet_id + 2;
        assert_eq!(u16::from_be_bytes([packet[start_packet_id], packet[end_packet_id - 1]]), 1);
        let properties_len_index = end_packet_id;
        assert_eq!(packet[properties_len_index], 0x00);
        let start_payload = properties_len_index + 1;
        assert_eq!(&packet[start_payload..], payload.as_ref());
    }

    #[test]
    fn test_round_trip_create_and_extract() {
        let topic_uuid = Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap();
        let original_payload = Bytes::from(vec![0x01, 0x02, 0xFF, 0x00]);
        let packet = create_packet(&original_payload, topic_uuid, TransportTypeEnum::Mqtt, 64, false);
        match extract_payload(&packet, TransportTypeEnum::Mqtt, 64, false) {
            Ok((parsed_topic, parsed_qos, parsed_retain, parsed_payload)) => {
                assert_eq!(parsed_topic, topic_uuid.to_string());
                assert_eq!(parsed_qos, 1);
                assert!(!parsed_retain);
                assert_eq!(parsed_payload.as_ref(), original_payload.as_ref());
            }
            Err(e) => panic!("Failed to parse created packet: {:?}", e),
        }
    }

    #[test]
    fn test_round_trip_empty_payload() {
        let topic_uuid = Uuid::nil();
        let empty_payload = Bytes::new();
        let packet = create_packet(&empty_payload, topic_uuid, TransportTypeEnum::Mqtt, 64, false);
        match extract_payload(&packet, TransportTypeEnum::Mqtt, 64, false) {
            Ok((parsed_topic, parsed_qos, _, parsed_payload)) => {
                assert_eq!(parsed_topic, "00000000-0000-0000-0000-000000000000");
                assert_eq!(parsed_qos, 1);
                assert!(parsed_payload.is_empty());
            }
            Err(e) => panic!("Failed to parse empty payload packet: {:?}", e),
        }
    }

    #[test]
    fn test_http_create_and_extract_round_trip() {
        let topic_uuid = Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap();
        let original_payload = Bytes::from(vec![0x01, 0x02, 0xFF, 0x00]);
        let packet = create_packet(&original_payload, topic_uuid, TransportTypeEnum::Http, 64, false);
        assert!(String::from_utf8_lossy(&packet).contains("POST /image/"));
        match extract_payload(&packet, TransportTypeEnum::Http, 64, true) {
            Ok((parsed_topic, parsed_qos, parsed_retain, parsed_payload)) => {
                assert_eq!(parsed_topic, topic_uuid.to_string());
                assert_eq!(parsed_qos, 0);
                assert!(!parsed_retain);
                assert_eq!(parsed_payload.as_ref(), original_payload.as_ref());
            }
            Err(e) => panic!("Failed to parse HTTP packet: {:?}", e),
        }
    }

    #[test]
    fn test_http_create_packet_structure() {
        let topic_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let payload = Bytes::from_static(b"test_payload");
        let packet = create_packet(&payload, topic_uuid, TransportTypeEnum::Http, 64, false);
        let s = String::from_utf8_lossy(&packet);
        assert!(s.starts_with("POST /image/550e8400-e29b-41d4-a716-446655440000.json HTTP/1.1\r\n"));
        assert!(s.contains("Content-Type: application/json\r\n"));
        assert!(s.contains(r#""img":""#));
    }

    #[test]
    fn test_http_extract_payload_simple() {
        let topic = "12345678-1234-5678-1234-567812345678";
        let encoded = encode_base64(b"hello");
        let json_body = format!(r#"{{"img":"{}"}}"#, encoded);
        let request = format!(
            "POST /image/{}.json HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            topic,
            json_body.len(),
            json_body
        );
        let bytes = Bytes::from(request);
        let (parsed_topic, qos, retain, payload) = extract_payload(&bytes, TransportTypeEnum::Http, 64, true).unwrap();
        assert_eq!(parsed_topic, topic);
        assert_eq!(qos, 0);
        assert!(!retain);
        assert_eq!(payload.as_ref(), b"hello");
    }

    #[test]
    fn test_http_round_trip_empty_payload() {
        let topic_uuid = Uuid::nil();
        let empty_payload = Bytes::new();
        let packet = create_packet(&empty_payload, topic_uuid, TransportTypeEnum::Http, 64, false);
        match extract_payload(&packet, TransportTypeEnum::Http, 64, true) {
            Ok((parsed_topic, parsed_qos, _, parsed_payload)) => {
                assert_eq!(parsed_topic, "00000000-0000-0000-0000-000000000000");
                assert_eq!(parsed_qos, 0);
                assert!(parsed_payload.is_empty());
            }
            Err(e) => panic!("Failed to parse empty HTTP payload packet: {:?}", e),
        }
    }

    #[test]
    fn test_base85_known_vectors() {
        // Reference values from Python base64.b85encode/b85decode
        assert_eq!(encode_base85(b""), "");
        assert_eq!(encode_base85(b"a"), "VE");
        assert_eq!(encode_base85(b"ab"), "VPX");
        assert_eq!(encode_base85(b"abc"), "VPaz");
        assert_eq!(encode_base85(b"abcd"), "VPa!s");
        assert_eq!(encode_base85(b"hello world"), "Xk~0{Zy<MXa%^M");
        assert_eq!(decode_base85("VE").unwrap(), b"a".to_vec());
        assert_eq!(decode_base85("VPa!s").unwrap(), b"abcd".to_vec());
        assert_eq!(decode_base85("Xk~0{Zy<MXa%^M").unwrap(), b"hello world".to_vec());
    }

    #[test]
    fn test_base85_round_trip_various_lengths() {
        for len in 0..=32usize {
            let data: Vec<u8> = (0..len).map(|i| (i * 7 + 3) as u8).collect();
            assert_eq!(decode_base85(&encode_base85(&data)).unwrap(), data, "round trip failed for len {}", len);
        }
        let all_bytes: Vec<u8> = (0..=255u16).map(|b| b as u8).collect();
        assert_eq!(decode_base85(&encode_base85(&all_bytes)).unwrap(), all_bytes);
    }

    #[test]
    fn test_base85_decode_errors() {
        // '/' is not in the base85 alphabet
        assert_eq!(decode_base85("V/a!s").unwrap_err(), TransportParseError::InsufficientData);
        // 5 max digits overflow a u32 word, like Python's "base85 overflow"
        assert_eq!(decode_base85("~~~~~").unwrap_err(), TransportParseError::InsufficientData);
    }

    #[test]
    fn test_http_b85_create_and_extract_round_trip() {
        let topic_uuid = Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap();
        let original_payload = Bytes::from(vec![0x00, 0x01, 0xFF, 0x00, 0xAB]);
        // client request -> server side parse
        let packet = create_packet(&original_payload, topic_uuid, TransportTypeEnum::Http, 85, false);
        match extract_payload(&packet, TransportTypeEnum::Http, 85, true) {
            Ok((parsed_topic, parsed_qos, parsed_retain, parsed_payload)) => {
                assert_eq!(parsed_topic, topic_uuid.to_string());
                assert_eq!(parsed_qos, 0);
                assert!(!parsed_retain);
                assert_eq!(parsed_payload.as_ref(), original_payload.as_ref());
            }
            Err(e) => panic!("Failed to parse HTTP b85 request: {:?}", e),
        }
        // server response -> client side parse
        let packet = create_packet(&original_payload, topic_uuid, TransportTypeEnum::Http, 85, true);
        match extract_payload(&packet, TransportTypeEnum::Http, 85, false) {
            Ok((parsed_topic, _, _, parsed_payload)) => {
                assert_eq!(parsed_topic, topic_uuid.to_string());
                assert_eq!(parsed_payload.as_ref(), original_payload.as_ref());
            }
            Err(e) => panic!("Failed to parse HTTP b85 response: {:?}", e),
        }
    }

    #[test]
    fn test_http_request_headers() {
        let topic_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let payload = Bytes::from_static(b"test_payload");
        let packet = create_packet(&payload, topic_uuid, TransportTypeEnum::Http, 64, false);
        let s = String::from_utf8_lossy(&packet).to_string();
        assert!(s.contains("Host: localhost\r\n"));
        assert!(s.contains("User-Agent: python-requests/2.31.0\r\n"));
        assert!(s.contains("Accept: */*\r\n"));
    }

    #[test]
    fn test_http_response_headers() {
        let topic_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let payload = Bytes::from_static(b"test_payload");
        let packet = create_packet(&payload, topic_uuid, TransportTypeEnum::Http, 64, true);
        let s = String::from_utf8_lossy(&packet).to_string();
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(s.contains("Server: nginx/1.31.6\r\n"));
        assert!(s.contains("Date: "));
        assert!(s.contains("Connection: keep-alive\r\n"));
        assert!(s.contains("Strict-Transport-Security: max-age=36000\r\n"));

        let forbidden = String::from_utf8_lossy(&forbidden_response()).to_string();
        assert!(forbidden.starts_with("HTTP/1.1 403 Forbidden\r\n"));
        assert!(forbidden.contains("Server: nginx/1.31.6\r\n"));
        assert!(forbidden.contains("Connection: keep-alive\r\n"));
    }
}
