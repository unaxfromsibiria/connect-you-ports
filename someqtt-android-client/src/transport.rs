use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug, PartialEq)]
pub enum MqttParseError {
    InvalidPacketType(u8),
    MalformedRemainingLength,
    TopicTooLong,
    MissingPacketId,
    InsufficientData,
}

pub fn create_packet(payload: &Bytes, topic: uuid::Uuid) -> Bytes {
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

pub fn extract_mqtt_payload(packet: &Bytes) -> Result<(String, u8, bool, Bytes), MqttParseError> {
    if packet.is_empty() {
        return Err(MqttParseError::InsufficientData);
    }
    let mut cursor = 0;
    // Read Fixed Header
    let first_byte = packet[cursor];
    cursor += 1;
    let packet_type = first_byte >> 4;
    if packet_type != 3 {
        return Err(MqttParseError::InvalidPacketType(packet_type));
    }
    let flags = first_byte & 0x0F;
    let qos = (flags >> 1) & 0x03;
    let retain = flags & 1 == 1;
    if qos > 2 {
        return Err(MqttParseError::MalformedRemainingLength);
    }
    // Read Remaining Length
    let (remaining_len, bytes_consumed) = read_var_byte_int(&packet[cursor..])?;
    cursor += bytes_consumed;
    if packet.len() < cursor + remaining_len {
        return Err(MqttParseError::InsufficientData);
    }
    let end_of_packet = cursor + remaining_len;
    // Read Topic Name
    if packet.len() < cursor + 2 {
        return Err(MqttParseError::InsufficientData);
    }
    let topic_len = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as usize;
    cursor += 2;
    if topic_len > 0xFFFF || packet.len() < cursor + topic_len {
        return Err(MqttParseError::TopicTooLong);
    }
    let topic_bytes = &packet[cursor..cursor + topic_len];
    let topic = String::from_utf8_lossy(topic_bytes).to_string();
    cursor += topic_len;
    // Read Packet Identifier (if QoS > 0)
    if qos > 0 {
        if packet.len() < cursor + 2 {
            return Err(MqttParseError::MissingPacketId);
        }
        cursor += 2;
    }
    // Read Properties Length and skip properties
    let (properties_len, bytes_consumed) = read_var_byte_int(&packet[cursor..])?;
    cursor += bytes_consumed;
    if packet.len() < cursor + properties_len {
        return Err(MqttParseError::InsufficientData);
    }
    cursor += properties_len;
    // Extract Payload
    let payload_len = end_of_packet - cursor;
    if packet.len() < cursor + payload_len {
        return Err(MqttParseError::InsufficientData);
    }
    let payload = packet.slice(cursor..cursor + payload_len);
    Ok((topic, qos, retain, payload))
}

fn read_var_byte_int(data: &[u8]) -> Result<(usize, usize), MqttParseError> {
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
            return Err(MqttParseError::MalformedRemainingLength);
        }
    }
    Err(MqttParseError::MalformedRemainingLength)
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
        let (topic, qos, retain, payload) = extract_mqtt_payload(&bytes).unwrap();
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
        let (topic, qos, retain, payload) = extract_mqtt_payload(&bytes).unwrap();
        assert_eq!(topic, "t");
        assert_eq!(qos, 1);
        assert!(!retain);
        assert_eq!(payload.as_ref(), b"data");
    }

    #[test]
    fn test_extract_invalid_type() {
        let packet = Bytes::from_static(&[0x40, 0x02]);
        assert_eq!(extract_mqtt_payload(&packet).unwrap_err(), MqttParseError::InvalidPacketType(4));
    }

    #[test]
    fn test_create_packet_structure() {
        let topic_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let payload = Bytes::from_static(b"test_payload");
        let packet = create_packet(&payload, topic_uuid);
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
        let packet = create_packet(&original_payload, topic_uuid);
        match extract_mqtt_payload(&packet) {
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
        let packet = create_packet(&empty_payload, topic_uuid);
        match extract_mqtt_payload(&packet) {
            Ok((parsed_topic, parsed_qos, _, parsed_payload)) => {
                assert_eq!(parsed_topic, "00000000-0000-0000-0000-000000000000");
                assert_eq!(parsed_qos, 1);
                assert!(parsed_payload.is_empty());
            }
            Err(e) => panic!("Failed to parse empty payload packet: {:?}", e),
        }
    }
}
