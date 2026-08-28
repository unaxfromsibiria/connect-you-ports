use aes_gcm::{aead::{Aead, Generate, Key, KeyInit}, Aes256Gcm, Nonce};
use bytes::{Bytes, BytesMut, BufMut};
use hex::decode;
use log::{error, info};
use std::fmt;
use uuid::Uuid;

use crate::settings::EncryptionData;
use crate::transport::{create_packet, extract_mqtt_payload};

#[derive(Debug, PartialEq)]
pub enum DataMessageError {
    Malformed(String),
    BadPayload,
}

impl fmt::Display for DataMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(reason) => write!(f, "invalid message structure: {}", reason),
            Self::BadPayload => write!(f, "valid MQTT structure but content cannot be decrypted"),
        }
    }
}

impl std::error::Error for DataMessageError {}

#[derive(Debug, Clone)]
pub struct DataMsg {
    pub service: Uuid,
    pub nonce: Vec<u8>,
    pub data: Bytes,
}

impl DataMsg {
    pub fn new_quit(service: &Uuid) -> Self {
        DataMsg {
            service: service.clone(),
            nonce: Vec::new(),
            data: Bytes::new(),
        }
    }

    pub fn dump(&self) -> Bytes {
        let mut buf = BytesMut::new();
        // [1 byte size of nonce]
        buf.put_u8(self.nonce.len() as u8);
        // [2 byte size of data]
        buf.put_u16_le(self.data.len() as u16);
        // [service as 16 byte uuid]
        buf.extend_from_slice(self.service.as_bytes());
        // [nonce]
        buf.extend_from_slice(&self.nonce);
        // [data]
        buf.extend_from_slice(&self.data);
        // random padding 8..=32 bytes for encryption hiding
        let pad_len = (Uuid::new_v4().as_u128() % 25 + 8) as usize;
        let mut pad = Vec::with_capacity(pad_len);
        while pad.len() < pad_len {
            let u = Uuid::new_v4();
            pad.extend_from_slice(u.as_bytes());
        }
        pad.truncate(pad_len);
        buf.extend_from_slice(&pad);
        buf.freeze()
    }

    pub fn is_quit(&self) -> bool {
        self.nonce.is_empty() && self.data.is_empty()
    }

    pub fn load(data: &Bytes) -> Result<Self, Box<dyn std::error::Error>> {
        if data.len() < 3 + 16 {
            return Err("Invalid data length".into());
        }
        let mut offset = 0;
        // [1 byte size of nonce]
        let nonce_len = data[offset] as usize;
        offset += 1;
        // [2 byte size of data]
        let data_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        // [service as 16 byte uuid]
        let service_bytes: [u8; 16] = data[offset..offset + 16].try_into()?;
        let service = Uuid::from_bytes(service_bytes);
        offset += 16;
        // [nonce]
        if offset + nonce_len > data.len() {
            return Err("Invalid nonce length".into());
        }
        let nonce: Vec<u8> = data[offset..offset + nonce_len].to_vec();
        offset += nonce_len;
        // [data]
        if offset + data_len > data.len() {
            return Err("Invalid data length".into());
        }
        let data_bytes: Vec<u8> = data[offset..offset + data_len].to_vec();
        Ok(DataMsg {
            service,
            nonce,
            data: Bytes::from(data_bytes),
        })
    }
}

#[derive(Clone)]
pub struct DataHandlerSettings {
    cipher: Option<Aes256Gcm>,
    encryption: bool,
}

pub trait DataHandler: Sized {
    fn new<T: EncryptionData>(settings: &T) -> Self;
    fn make_data_message(&self, data: &[u8], service: &Uuid, transfer: &Uuid) -> Bytes;
    fn make_quit_message(&self, service: &Uuid, transfer: &Uuid) -> Bytes;
    fn load_data_message(&self, data: &[u8]) -> Result<(DataMsg, Uuid), DataMessageError>;
}

impl DataHandler for DataHandlerSettings {
    fn new<T: EncryptionData>(settings: &T) -> Self {
        let mut handler = DataHandlerSettings {
            cipher: None,
            encryption: false,
        };
        let cipher_key = settings.main_cipher_key();
        if !cipher_key.is_empty() {
            let key_bytes = match decode(cipher_key) {
                Ok(bytes) => bytes,
                Err(_) => {
                    error!("Incorrect Aes256Gcm key value");
                    return handler;
                }
            };
            if key_bytes.is_empty() {
                return handler;
            }
            let key = match Key::<Aes256Gcm>::try_from(&key_bytes[..]) {
                Ok(k) => k,
                Err(_) => {
                    error!("Incorrect Aes256Gcm key length");
                    return handler;
                }
            };
            let cipher = Aes256Gcm::new(&key);
            handler.cipher = Some(cipher);
            handler.encryption = true;
            info!("Using Aes-256-Gcm encryption");
        }
        handler
    }

    fn make_data_message(&self, data: &[u8], service: &Uuid, transfer: &Uuid) -> Bytes {
        let (nonce_bytes, msg_data) = if self.encryption {
            if let Some(cipher) = &self.cipher {
                let nonce = Nonce::generate();
                let nonce_bytes = nonce.to_vec();
                match cipher.encrypt(&nonce, data) {
                    Ok(res) => (nonce_bytes, res),
                    Err(err) => {
                        error!("Message creation: Unexpected cipher using error: {}", err);
                        // fallback to plaintext on error
                        (Vec::new(), data.to_vec())
                    }
                }
            } else {
                (Vec::new(), data.to_vec())
            }
        } else {
            (Vec::new(), data.to_vec())
        };

        let msg = DataMsg {
            service: service.clone(),
            nonce: nonce_bytes,
            data: Bytes::from(msg_data),
        };
        let payload = msg.dump();
        create_packet(&payload, *transfer)
    }

    fn make_quit_message(&self, service: &Uuid, transfer: &Uuid) -> Bytes {
        let msg = DataMsg::new_quit(service);
        let payload = msg.dump();
        create_packet(&payload, *transfer)
    }

    fn load_data_message(&self, data: &[u8]) -> Result<(DataMsg, Uuid), DataMessageError> {
        let packet = Bytes::from(data.to_vec());
        let (topic_str, _, _, payload) = match extract_mqtt_payload(&packet) {
            Ok(v) => v,
            Err(e) => return Err(DataMessageError::Malformed(format!("MQTT parse error: {:?}", e))),
        };

        let transfer_id = match Uuid::parse_str(&topic_str) {
            Ok(uuid) => uuid,
            Err(_) => {
                let err_msg = format!("Invalid transfer id in topic: {}", topic_str);
                error!("{}", err_msg);
                return Err(DataMessageError::Malformed(err_msg));
            }
        };

        let mut msg = match DataMsg::load(&payload) {
            Ok(m) => m,
            Err(e) => return Err(DataMessageError::Malformed(format!("DataMsg load error: {}", e))),
        };

        if self.encryption && !msg.nonce.is_empty() {
            let nonce = match Nonce::try_from(msg.nonce.as_slice()) {
                Ok(n) => n,
                Err(_) => {
                    let err_msg = "Invalid nonce length".to_string();
                    error!("{}", err_msg);
                    return Err(DataMessageError::Malformed(err_msg));
                }
            };
            if let Some(cipher) = &self.cipher {
                match cipher.decrypt(&nonce, msg.data.as_ref()) {
                    Ok(res) => msg.data = Bytes::from(res),
                    Err(err) => {
                        error!("Unexpected cipher using error: {}", err);
                        return Err(DataMessageError::BadPayload);
                    }
                }
            } else {
                return Err(DataMessageError::Malformed("Encryption enabled but cipher not initialized".to_string()));
            }
        } else if self.encryption && msg.nonce.is_empty() && !msg.is_quit() {
            error!("Encryption enabled but nonce is empty");
        }

        Ok((msg, transfer_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    struct TestEncryptionData {
        key: String,
    }

    impl EncryptionData for TestEncryptionData {
        fn main_cipher_key(&self) -> String {
            self.key.clone()
        }
    }

    const VALID_KEY: &str = "f6a5a635556a59f6eef8a65c7d146d2f138941accaa70547d27b9286b958ad7b";
    const ANOTHER_VALID_KEY: &str = "1234567890123456789012345678901234567890123456789012345678901234";
    const EMPTY_KEY: &str = "";

    fn build_mqtt_packet(topic: &[u8], payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x32); // PUBLISH, QoS 1
        let remaining_len = 2 + topic.len() + 2 /* packet id */ + 1 /* props len */ + payload.len();
        debug_assert!(remaining_len < 128, "test helper supports single-byte remaining length");
        buf.push(remaining_len as u8);
        buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        buf.extend_from_slice(topic);
        buf.extend_from_slice(&1u16.to_be_bytes()); // packet id
        buf.push(0u8); // properties length = 0
        buf.extend_from_slice(payload);
        buf
    }

    fn get_uuid() -> Uuid {
        Uuid::new_v4()
    }

    fn extract_msg(packet: &Bytes) -> DataMsg {
        let (_, _, _, payload) = extract_mqtt_payload(packet).expect("parse packet");
        DataMsg::load(&payload).expect("load msg")
    }

    #[test]
    fn test_handler_no_encryption() {
        let settings = TestEncryptionData { key: EMPTY_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        assert!(!handler.encryption);
        assert!(handler.cipher.is_none());
        let data = b"Hello, World!".to_vec();
        let service = get_uuid();
        let transfer = get_uuid();
        let packet = handler.make_data_message(&data, &service, &transfer);
        let msg = extract_msg(&packet);
        assert_eq!(msg.service, service);
        assert!(msg.nonce.is_empty());
        assert_eq!(msg.data, data);
        let (loaded, loaded_transfer) = handler.load_data_message(&packet.to_vec()).unwrap();
        assert_eq!(loaded.service, service);
        assert_eq!(loaded.data, data);
        assert_eq!(loaded_transfer, transfer);
    }

    #[test]
    fn test_handler_encryption_valid_key() {
        let settings = TestEncryptionData { key: VALID_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        assert!(handler.encryption);
        assert!(handler.cipher.is_some());
        let data = b"Secret Data".to_vec();
        let service = get_uuid();
        let transfer = get_uuid();
        let packet = handler.make_data_message(&data, &service, &transfer);
        let msg = extract_msg(&packet);
        assert_eq!(msg.service, service);
        assert!(!msg.nonce.is_empty());
        assert_ne!(msg.data, data);
        let (loaded, loaded_transfer) = handler.load_data_message(&packet.to_vec()).unwrap();
        assert_eq!(loaded.service, service);
        assert_eq!(loaded.data, data);
        assert_eq!(loaded_transfer, transfer);
    }

    #[test]
    fn test_handler_encryption_invalid_hex_format() {
        let settings = TestEncryptionData { key: "zzz".to_string() };
        let handler = DataHandlerSettings::new(&settings);
        assert!(!handler.encryption);
        assert!(handler.cipher.is_none());
    }

    #[test]
    fn test_handler_encryption_decryption_mismatch() {
        let settings_encrypt = TestEncryptionData { key: VALID_KEY.to_string() };
        let handler_encrypt = DataHandlerSettings::new(&settings_encrypt);
        let settings_decrypt = TestEncryptionData { key: ANOTHER_VALID_KEY.to_string() };
        let handler_decrypt = DataHandlerSettings::new(&settings_decrypt);
        let data = b"Secret".to_vec();
        let service = get_uuid();
        let transfer = get_uuid();
        let packet = handler_encrypt.make_data_message(&data, &service, &transfer);
        match handler_decrypt.load_data_message(&packet.to_vec()) {
            Err(DataMessageError::BadPayload) => {},
            other => panic!("expected BadPayload error, got {:?}", other),
        }
    }

    #[test]
    fn test_load_error_variants() {
        let settings = TestEncryptionData { key: VALID_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        assert!(handler.encryption);
        let data = b"Secret".to_vec();
        let service = get_uuid();
        let transfer = get_uuid();
        // wrong-key packet: valid MQTT structure, undecryptable content -> BadPayload
        let other = DataHandlerSettings::new(&TestEncryptionData { key: ANOTHER_VALID_KEY.to_string() });
        let foreign_packet = other.make_data_message(&data, &service, &transfer);
        match handler.load_data_message(&foreign_packet) {
            Err(DataMessageError::BadPayload) => {},
            other => panic!("expected BadPayload, got {:?}", other),
        }
        // valid packet from the same handler loads fine (control case)
        let own_packet = handler.make_data_message(&data, &service, &transfer);
        assert!(handler.load_data_message(&own_packet).is_ok());
        // not an MQTT packet at all -> Malformed
        match handler.load_data_message(&vec![1u8, 2u8, 3u8]) {
            Err(DataMessageError::Malformed(_)) => {},
            other => panic!("expected Malformed, got {:?}", other),
        }
        // valid MQTT structure but non-UUID topic -> Malformed (suspicious client case)
        let bad_topic = build_mqtt_packet(b"not-a-uuid-topic", &[]);
        match handler.load_data_message(&bad_topic) {
            Err(DataMessageError::Malformed(_)) => {},
            other => panic!("expected Malformed, got {:?}", other),
        }
    }


    #[test]
    fn test_encryption_overhead_rate() {
        use rand::{Rng, RngExt};
        // handler with encryption
        let settings = TestEncryptionData { key: VALID_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        assert!(handler.encryption);
        let mut rng = rand::rng();
        let mut size_src = 0usize;
        let mut size_transport = 0usize;
        // 100 random payloads
        for _ in 0..100 {
            let data_len = rng.random_range(1024..=8192);
            let mut data = vec![0u8; data_len];
            rng.fill_bytes(&mut data);
            size_src += data_len;
            let service = get_uuid();
            let transfer = get_uuid();
            let packet = handler.make_data_message(&data, &service, &transfer);
            size_transport += packet.len();
            // check encrypt/decrypt
            let (loaded, loaded_transfer) = handler.load_data_message(&packet.to_vec()).expect("decrypt failed");
            assert_eq!(loaded.service, service);
            assert_eq!(loaded.data, data);
            assert_eq!(loaded_transfer, transfer);
        }

        assert!(size_src < size_transport);
        let rate = 100.0 - (size_src as f64 / size_transport as f64 * 100.0);
        // overhead range
        assert!(rate > 2.0 && rate < 5.0);
    }

    #[test]
    fn test_make_quit_message() {
        let settings = TestEncryptionData { key: EMPTY_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        let service = get_uuid();
        let transfer = get_uuid();
        let packet = handler.make_quit_message(&service, &transfer);
        let msg = extract_msg(&packet);
        assert_eq!(msg.service, service);
        assert!(msg.nonce.is_empty());
        assert!(msg.data.is_empty());
        assert!(msg.is_quit());
    }

    #[test]
    fn test_dump_to_server_format() {
        let settings = TestEncryptionData { key: EMPTY_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        let data = b"Test".to_vec();
        let service = get_uuid();
        let transfer = get_uuid();
        let packet = handler.make_data_message(&data, &service, &transfer);
        let msg = extract_msg(&packet);
        assert_eq!(msg.service, service);
        assert_eq!(msg.data, data);
    }

    #[test]
    fn test_load_corrupted_data() {
        let settings = TestEncryptionData { key: EMPTY_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        let corrupted_data = vec![1, 2, 3, 4, 5];
        let result = handler.load_data_message(&corrupted_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_nonce_uniqueness() {
        let settings = TestEncryptionData { key: VALID_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        assert!(handler.encryption);
        let data = b"Repeated Secret Message".to_vec();
        let service = get_uuid();
        let transfer = get_uuid();
        let packet1 = handler.make_data_message(&data, &service, &transfer);
        let packet2 = handler.make_data_message(&data, &service, &transfer);
        let msg1 = extract_msg(&packet1);
        let msg2 = extract_msg(&packet2);

        assert!(!msg1.nonce.is_empty());
        assert!(!msg2.nonce.is_empty());
        assert_ne!(msg1.nonce, msg2.nonce);
        assert_ne!(msg1.data, msg2.data);
        let (loaded1, loaded_transfer1) = handler.load_data_message(&packet1.to_vec()).unwrap();
        let (loaded2, loaded_transfer2) = handler.load_data_message(&packet2.to_vec()).unwrap();
        assert_eq!(loaded1.data, data);
        assert_eq!(loaded2.data, data);
        assert_eq!(loaded_transfer1, transfer);
        assert_eq!(loaded_transfer2, transfer);
    }

    #[test]
    fn test_new_quit() {
        let service = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let msg = DataMsg::new_quit(&service);
        assert_eq!(msg.service, service);
        assert!(msg.nonce.is_empty());
        assert!(msg.data.is_empty());
    }

    #[test]
    fn test_dump_and_load_roundtrip() {
        // Create a sample DataMsg
        let service_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let nonce = vec![1, 2, 3, 4];
        let data = Bytes::from(vec![5, 6, 7, 8, 9]);
        let original_msg = DataMsg {
            service: service_uuid,
            nonce,
            data,
        };
        // Dump to bytes
        let dumped_bytes = original_msg.dump();
        // Load back from bytes
        let loaded_msg = DataMsg::load(&dumped_bytes).expect("Failed to load message");
        // Verify that the loaded message matches the original
        assert_eq!(loaded_msg.service, original_msg.service);
        assert_eq!(loaded_msg.nonce, original_msg.nonce);
        assert_eq!(loaded_msg.data, original_msg.data);
    }

    #[test]
    fn test_load_invalid_data_length() {
        let short_data = Bytes::from(vec![0u8; 5]); // Too short to contain all fields
        let result = DataMsg::load(&short_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_empty_nonce_and_data() {
        let service_uuid = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        // Manually construct bytes for empty nonce and data
        let mut bytes = Vec::new();
        bytes.push(0u8); // nonce length 0
        bytes.extend_from_slice(&0u16.to_le_bytes()); // data length 0
        bytes.extend_from_slice(service_uuid.as_bytes());
        let dumped_bytes = Bytes::from(bytes);
        let loaded_msg = DataMsg::load(&dumped_bytes).expect("Failed to load message");
        assert_eq!(loaded_msg.service, service_uuid);
        assert!(loaded_msg.nonce.is_empty());
        assert!(loaded_msg.data.is_empty());
    }

    #[test]
    fn test_load_nonce_length_mismatch() {
        let mut bytes = Vec::new();
        bytes.push(10u8); // Claim nonce length is 10
        bytes.extend_from_slice(&5u16.to_le_bytes()); // Data length 5
        bytes.extend_from_slice(Uuid::nil().as_bytes()); // Service UUID
        bytes.extend_from_slice(&vec![1u8; 3]); // Only provide 3 bytes for nonce
        let dumped_bytes = Bytes::from(bytes);
        let result = DataMsg::load(&dumped_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_data_length_mismatch() {
        let mut bytes = Vec::new();
        bytes.push(2u8); // Nonce length 2
        bytes.extend_from_slice(&10u16.to_le_bytes()); // Claim data length is 10
        bytes.extend_from_slice(Uuid::nil().as_bytes()); // Service UUID
        bytes.extend_from_slice(&vec![1u8; 2]); // Provide nonce
        bytes.extend_from_slice(&vec![2u8; 3]); // Only provide 3 bytes for data
        let dumped_bytes = Bytes::from(bytes);
        let result = DataMsg::load(&dumped_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_quit() {
        let quit_msg = DataMsg::new_quit(&Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());
        assert!(quit_msg.is_quit());
        let non_quit_msg = DataMsg {
            service: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            nonce: vec![1, 2, 3],
            data: Bytes::from(vec![4, 5, 6]),
        };
        assert!(!non_quit_msg.is_quit());
        let empty_nonce_non_empty_data = DataMsg {
            service: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            nonce: Vec::new(),
            data: Bytes::from(vec![1, 2, 3]),
        };
        assert!(!empty_nonce_non_empty_data.is_quit());
        let non_empty_nonce_empty_data = DataMsg {
            service: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            nonce: vec![1, 2, 3],
            data: Bytes::new(),
        };
        assert!(!non_empty_nonce_empty_data.is_quit());
    }

    #[test]
    fn test_dump_load_is_quit() {
        let quit_msg = DataMsg::new_quit(&Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());
        // Dump to bytes
        let dumped_bytes = quit_msg.dump();
        // Load back from bytes
        let loaded_msg = DataMsg::load(&dumped_bytes).expect("Failed to load message");
        // Verify that the loaded message is a quit message
        assert!(loaded_msg.is_quit());
    }
}
