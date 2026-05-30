use crate::common::{EncryptionData, CMD_BUF_SIZE, fake_data};

use log::{info, error};
use serde::{Serialize, Deserialize};
use hex::decode;
use uuid::fmt::Simple;
use uuid::Uuid;
use aes_gcm::{Aes256Gcm, Nonce, Key, aead::{Aead, AeadCore, KeyInit, OsRng}};

#[derive(Serialize, Deserialize, Clone)]
pub struct DataMsg {
    pub t: Uuid,
    pub s: Uuid,
    pub d: Vec<u8>,
    pub x: bool,
    pub n: Vec<u8>,
}

impl DataMsg {
    pub fn dump(&self, to_server: bool) -> Vec<u8> {
        let serialized = bincode::serialize(&self).unwrap();
        let data_len = serialized.len() as u32;
        let len_bytes = data_len.to_le_bytes();
        if to_server {
            let prefix_part = format!("SET {} ", Simple::from_uuid(self.s));
            let mut prefix_with_len = prefix_part.as_bytes().to_vec();
            prefix_with_len.extend_from_slice(&len_bytes);
            let padding_len = CMD_BUF_SIZE.saturating_sub(prefix_with_len.len());
            let mut result = Vec::with_capacity(CMD_BUF_SIZE + serialized.len());
            result.extend_from_slice(&prefix_with_len);
            result.extend(fake_data(padding_len));
            result.extend_from_slice(&serialized);
            result
        } else {
            let mut result = Vec::with_capacity(4 + serialized.len());
            result.extend_from_slice(&len_bytes);
            result.extend_from_slice(&serialized);
            result
        }
    }
}

#[derive(Clone)]
pub struct DataHandlerSettings {
    cipher: Option<Aes256Gcm>,
    encryption: bool,
}

pub trait DataHandler: Sized {
    fn new<T: EncryptionData>(settings: &T) -> Self;
    fn make_data_message(&self, data: &[u8], service: &Uuid, transfer: &Uuid) -> DataMsg;
    fn make_quit_message(&self, service: &Uuid, transfer: &Uuid) -> DataMsg;
    fn load_data_message(&self, data: &[u8]) -> Result<DataMsg, String>;
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
            let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
            let cipher = Aes256Gcm::new(key);
            handler.cipher = Some(cipher);
            handler.encryption = true;
            info!("Using Aes-256-Gcm encryption");
        }
        handler
    }

    fn make_data_message(&self, data: &[u8], service: &Uuid, transfer: &Uuid) -> DataMsg {
        let mut oper_error = "".to_string();
        let msg_data;
        let n: Vec<u8>;
        if self.encryption && let Some(cipher) = &self.cipher {       
            let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
            n = nonce.to_vec();
            match cipher.encrypt(&nonce, data) {
                Ok(res) => msg_data = res,
                Err(err) => {
                    oper_error = format!("Unexpected cipher using error: {}", err);
                    msg_data = oper_error.to_string().as_bytes().to_vec();
                    error!("Message creation: {}", oper_error);
                }
            }
        } else {
            n = [].to_vec();
            msg_data = data.to_vec();
        }
        DataMsg {
            t: transfer.clone(),
            s: service.clone(),
            d: msg_data,
            x: !oper_error.is_empty(),
            n: n,
        }
    }

    fn make_quit_message(&self, service: &Uuid, transfer: &Uuid) -> DataMsg {
        DataMsg {
            t: transfer.clone(),
            s: service.clone(),
            d: [].to_vec(),
            x: true,
            n: [].to_vec(),
        }
    }

    fn load_data_message(&self, data: &[u8]) -> Result<DataMsg, String> {
        match bincode::deserialize::<DataMsg>(data) {
            Ok(mut msg) => {
                if self.encryption && !msg.n.is_empty() {
                    let nonce = Nonce::from_slice(&msg.n);
                    if let Some(cipher) = &self.cipher {
                        match cipher.decrypt(&nonce, msg.d.as_slice()) {
                            Ok(res) => {
                                msg.d = res;
                            },
                            Err(err) => {
                                let err_msg = format!("Unexpected cipher using error: {}", err);
                                error!("{}", err_msg);
                                return Err(err_msg);
                            }
                        }
                    } else {
                         return Err("Encryption enabled but cipher not initialized".to_string());
                    }
                } else if self.encryption && msg.n.is_empty() && !msg.x {
                     error!("Encryption enabled but nonce is empty");
                     msg.x = true;
                }
                Ok(msg)
            },
            Err(err) => {
                let err_msg = format!("Deserialize error: {} with data size: {}", err, data.len());
                Err(err_msg)
            }
        }
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
    
    fn get_uuid() -> Uuid {
        Uuid::new_v4()
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
        
        let msg = handler.make_data_message(&data, &service, &transfer);
        assert_eq!(msg.d, data);
        assert!(!msg.x);
        assert!(msg.n.is_empty());

        let dumped = msg.dump(false);
        let payload = &dumped[4..];
        
        let loaded = handler.load_data_message(payload).unwrap();
        assert_eq!(loaded.d, data);
        assert_eq!(loaded.s, service);
        assert_eq!(loaded.t, transfer);
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
        
        let msg = handler.make_data_message(&data, &service, &transfer);
        assert_ne!(msg.d, data);
        assert!(!msg.x);
        assert!(!msg.n.is_empty());

        let dumped = msg.dump(false);
        let payload = &dumped[4..];
        
        let loaded = handler.load_data_message(payload).unwrap();
        assert_eq!(loaded.d, data);
        assert!(!loaded.x);
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
        
        let msg = handler_encrypt.make_data_message(&data, &service, &transfer);
        let dumped = msg.dump(false);
        let payload = &dumped[4..];
        
        let result = handler_decrypt.load_data_message(payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_make_quit_message() {
        let settings = TestEncryptionData { key: EMPTY_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        
        let service = get_uuid();
        let transfer = get_uuid();
        
        let msg = handler.make_quit_message(&service, &transfer);
        assert!(msg.d.is_empty());
        assert!(msg.n.is_empty());
        assert!(msg.x);
        assert_eq!(msg.s, service);
        assert_eq!(msg.t, transfer);
    }

    #[test]
    fn test_dump_to_server_format() {
        let settings = TestEncryptionData { key: EMPTY_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        
        let data = b"Test".to_vec();
        let service = get_uuid();
        let transfer = get_uuid();
        
        let msg = handler.make_data_message(&data, &service, &transfer);
        let dumped = msg.dump(true);
        
        let service_simple = Simple::from_uuid(service).to_string();
        let expected_prefix = format!("SET {} ", service_simple);
        
        assert!(dumped.starts_with(expected_prefix.as_bytes()));
    }

    #[test]
    fn test_load_corrupted_data() {
        let settings = TestEncryptionData { key: EMPTY_KEY.to_string() };
        let handler = DataHandlerSettings::new(&settings);
        
        let corrupted_data = vec![1, 2, 3, 4, 5];
        let result = handler.load_data_message(&corrupted_data);
        
        assert!(result.is_err());
    }
}
