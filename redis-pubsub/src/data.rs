use crate::common;

use log::{info, error};
use flate2::Compression;
use serde::{Serialize, Deserialize};
use flate2::write::{ZlibEncoder, ZlibDecoder};
use std::io::prelude::*;
use hex::decode;
use std::io::Result;
use common::{Settings, TOPIC_NAME_DATA_CLIENT, TOPIC_NAME_DATA_SERVER, TOPIC_NAME_NEW_CLIENT};
use aes_gcm::{Aes256Gcm, Nonce, Key, aead::{Aead, AeadCore, KeyInit, OsRng}};

const MODE_FORMAT_RAW: u8 = 0;
const MODE_FORMAT_COMPRESS_CRYPTO: u8 = 1;
const MODE_FORMAT_COMPRESS_ONLY: u8 = 2;
const MODE_FORMAT_CRYPTO_ONLY: u8 = 3;

pub fn client_data_topic(service_code: &str, client_id: &str) -> String {
    format!("{}-{}-{}", TOPIC_NAME_DATA_SERVER, service_code, client_id)
}

pub fn server_data_topic(service_code: &str, client_id: &str) -> String {
    format!("{}-{}-{}", TOPIC_NAME_DATA_CLIENT, service_code, client_id).to_string()
}

fn compress_data(data: &[u8]) -> Result<Vec<u8>> {
    let mut zlib = ZlibEncoder::new(Vec::new(), Compression::best());
    zlib.write_all(&data).unwrap();
    zlib.finish()
}

fn decompress_data(data: &[u8]) -> Result<Vec<u8>> {
    let mut zlib = ZlibDecoder::new(Vec::new());
    zlib.write_all(data)?;
    zlib.finish()
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NewClient {
    pub c: String,
    pub s: String,
    pub p: u8,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DataMsg {
    pub c: String,
    pub s: String,
    pub d: Vec<u8>,
    pub e: String,
    pub x: bool,
    pub m: u8,
    pub n: Vec<u8>,
}

#[derive(Clone)]
pub struct DataHandlerSettings {
    cipher: Option<Aes256Gcm>,
    compression: bool,
    encryption: bool,
}

pub trait DataHandler {
    fn new() -> Self;
    fn setup(&mut self, settings: &Settings) -> bool;
    fn make_hello_topic_message(&self, code: &str, client: &str, is_tcp: bool) -> (Vec<u8>, String);
    fn make_data_message(&self, data: &[u8], code: &str, client: &str, to_client: bool) -> (Vec<u8>, String);
    fn make_quit_message(&self, code: &str, client: &str, to_client: bool) -> (Vec<u8>, String);
    fn load_data_message(&self, data: &[u8]) -> DataMsg;
    fn load_hello_message(&self, data: &[u8]) -> NewClient;
}

impl DataHandler for DataHandlerSettings {
    fn new() -> Self {
        DataHandlerSettings {cipher: None, compression: false, encryption: false}
    }

    fn setup(&mut self, settings: &Settings) -> bool {
        if !settings.cipher_key.is_empty() {
            let key_bytes = decode(&settings.cipher_key).expect("Incorrect Aes256Gcm key value");
            if key_bytes.is_empty() {
                return false;
            } else {
                // from cryptography.hazmat.primitives.ciphers.aead import AESGCM
                // print("new:", AESGCM.generate_key(bit_length=256))
                let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
                let cipher = Aes256Gcm::new(key);
                self.cipher = Some(cipher);
                self.encryption = true;
                info!("Using Aes-256-Gcm encryption{}", if settings.use_compression {" with compression"} else {""});
            }
        } else {
            info!("Compression: {}", if settings.use_compression {"on"} else {"off"});
        }
        self.compression = settings.use_compression;
        true
    }

    fn make_hello_topic_message(&self, code: &str, client: &str, is_tcp: bool) -> (Vec<u8>, String) {
        let val = if is_tcp {1} else {0};
        let client = NewClient {c: client.to_string(), s: code.to_string(), p: val as u8};
        let serialized = bincode::serialize(&client).unwrap();
        (serialized, TOPIC_NAME_NEW_CLIENT.to_string())
    }

    fn make_data_message(&self, data: &[u8], code: &str, client: &str, to_client: bool) -> (Vec<u8>, String) {
        let mut mode = MODE_FORMAT_RAW;
        if self.compression && self.encryption {
            mode = MODE_FORMAT_COMPRESS_CRYPTO;
        } else if self.encryption {
            mode = MODE_FORMAT_CRYPTO_ONLY;
        } else if self.compression {
            mode = MODE_FORMAT_COMPRESS_ONLY;
        }
        let mut oper_error = "".to_string();
        let msg_data;
        let n;
        if self.encryption && let Some(cipher) = &self.cipher {
            let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
            n = nonce.to_vec();
            match cipher.encrypt(&nonce, data) {
                Ok(res) => msg_data = res,
                Err(err) => {
                    msg_data = [].to_vec();
                    oper_error = format!("Unexpected cipher using error: {}", err);
                    error!("Message creation: {}", oper_error);
                }
            }
        } else {
            n = [].to_vec();
            msg_data = data.to_vec();
        }
        let msg = DataMsg {
            c: client.to_string(),
            s: code.to_string(),
            d: if self.compression {
                match compress_data(&msg_data) {
                    Ok(new_data) => new_data,
                    Err(err) => {
                        oper_error = format!("Unexpected data compression error: {}", err);
                        error!("Message creation: {}", oper_error);
                        [].to_vec()
                    }
                }
            } else {
                msg_data
            },
            m: mode,
            e: oper_error,
            x: false,
            n: n,
        };
        let serialized = bincode::serialize(&msg).unwrap();
        (serialized, if to_client {client_data_topic(code, client)} else {server_data_topic(code, client)})
    }

    fn make_quit_message(&self, code: &str, client: &str, to_client: bool) -> (Vec<u8>, String) {
        let msg = DataMsg {
            c: client.to_string(),
            s: code.to_string(),
            d: [].to_vec(),
            m: MODE_FORMAT_RAW,
            e: "".to_string(),
            x: true,
            n: [].to_vec(),
        };
        let serialized = bincode::serialize(&msg).unwrap();
        (serialized, if to_client {client_data_topic(code, client)} else {server_data_topic(code, client)})
    }

    fn load_data_message(&self, data: &[u8]) -> DataMsg {
        match bincode::deserialize::<DataMsg>(data) {
            Ok(mut msg) => {
                if msg.m == MODE_FORMAT_COMPRESS_CRYPTO || msg.m == MODE_FORMAT_COMPRESS_ONLY {
                    match decompress_data(&msg.d) {
                        Ok(new_data) => {
                            msg.d = new_data
                        },
                        Err(c_err) => {
                            msg.e = format!("Unexpected data decompression error: {}", c_err);
                        }
                    }
                }
                if self.encryption && (msg.m == MODE_FORMAT_COMPRESS_CRYPTO || msg.m == MODE_FORMAT_CRYPTO_ONLY) {
                    let nonce = Nonce::from_slice(&msg.n);
                    if let Some(cipher) = &self.cipher {
                        match cipher.decrypt(&nonce, msg.d.as_slice()) {
                            Ok(res) => {
                                msg.d = res;
                            },
                            Err(err) => {
                                msg.e = format!("Unexpected cipher using error: {}", err);
                            }
                        }
                    }
                }
                msg
            },
            Err(err) => DataMsg {
                c: "".to_string(),
                s: "".to_string(),
                d: vec![],
                m: MODE_FORMAT_RAW,
                e: err.to_string(),
                x: false,
                n: vec![],
            },
        }
    }

    fn load_hello_message(&self, data: &[u8]) -> NewClient {
        match bincode::deserialize(data) {
            Ok(msg) => msg,
            Err(err) => NewClient {c: "".to_string(), s: err.to_string(), p: 0},
        }
    }
}
