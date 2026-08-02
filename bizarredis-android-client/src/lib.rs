#[macro_use] extern crate log;
extern crate android_logger;

mod common;
mod stat;
mod data;
mod client;

use jni::objects::{JClass, JString};
use jni::sys::{jstring, jboolean};
use jni::JNIEnv;
use std::sync::Mutex;
use std::sync::OnceLock;
use tokio::runtime::Runtime;
use client::run_service;
use log::{info, error, warn};
use common::{create_settings, Settings};
use stat::show_stats_sync;

struct AppState {
    is_running: Mutex<bool>, 
}

static APP_STATE: OnceLock<AppState> = OnceLock::new();

fn get_state() -> &'static AppState {
    APP_STATE.get_or_init(|| AppState {
        is_running: Mutex::new(false),
    })
}

fn is_running() -> bool {
    let state = get_state();
    *state.is_running.lock().unwrap() == true
}

fn init_android_logger() {
    android_logger::init_once(
        android_logger::Config::default().with_max_level(
            log::LevelFilter::Info
        ).with_tag("RustSocketServer"),
    );
}

fn run_tokio_server(settings: Settings) {
    init_android_logger();
    if settings.verbose {
        info!("Starting Tokio runtime for client '{}'", settings.client_name);
    }
    match Runtime::new() {
        Ok(rt) => {
            let state = get_state();
            *state.is_running.lock().unwrap() = true;
            info!("Client '{}' connection to {}:{}", settings.client_name, settings.server_host, settings.server_port);
            let result = rt.block_on(run_service(settings, is_running));
            match result {
                Ok(_) => {
                    info!("Service finished successfully");
                },
                Err(err) => {
                    error!("Service failed with error: {:?}", err);
                    *state.is_running.lock().unwrap() = false;
                },
            }
        },
        Err(e) => {
            error!("Failed to create Tokio runtime: {}", e);
            let state = get_state();
            *state.is_running.lock().unwrap() = false;
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn Java_com_example_connectports_MainActivity_00024Companion_startServer(
    mut env: JNIEnv,
    _class: JClass,
    host_jstr: JString,
    port_jint: i32,
    key_jstr: JString,
    tcp_settings_jstr: JString,
    udp_settings_jstr: JString,
    verbose_jboolean: jboolean,
) {
    init_android_logger();
    let host: String = match env.get_string(&host_jstr) {
        Ok(s) => s.into(),
        Err(_) => {
            error!("Failed to get host string from Java");
            return;
        },
    };
    let key: String = match env.get_string(&key_jstr) {
        Ok(s) => s.into(),
        Err(_) => {
            error!("Failed to get key string from Java");
            return;
        },
    };
    let tcp_settings: String = match env.get_string(&tcp_settings_jstr) {
        Ok(s) => s.into(),
        Err(_) => {
            error!("Failed to get tcp settings string from Java");
            return;
        },
    };
    let udp_settings: String = match env.get_string(&udp_settings_jstr) {
        Ok(s) => s.into(),
        Err(_) => {
            error!("Failed to get udp settings string from Java");
            return;
        },
    };
    let port = port_jint as u16;
    info!("Create connection to server {}:{}", host, port);
    let state = get_state();
    if *state.is_running.lock().unwrap() {
        warn!("Server is already running");
        return;
    }
    let verbose = (verbose_jboolean as u8) > 0;
    let settings = create_settings(&host, port, &key, &tcp_settings, &udp_settings, verbose);
    if settings.verbose {
        info!("Spawning background thread for server");
    }

    std::thread::spawn(move || {
        run_tokio_server(settings);
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn Java_com_example_connectports_MainActivity_00024Companion_stopServer(
    _env: JNIEnv,
    _class: JClass,
) {
    let state = get_state();
    *state.is_running.lock().unwrap() = false;
}

#[unsafe(no_mangle)]
pub extern "C" fn Java_com_example_connectports_MainActivity_00024Companion_getStat(
    _env: JNIEnv,
    _class: JClass,
) -> jstring {
    let result = show_stats_sync();
    match _env.new_string(&result) {
        Ok(output) => output.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_state_returns_same_instance() {
        let state1 = get_state();
        let state2 = get_state();       
        assert!(std::ptr::eq(state1, state2));
    }

    #[tokio::test]
    async fn test_is_running_initially_false() {
        let state = get_state();
        let lock = state.is_running.lock().unwrap();
        assert_eq!(*lock, false);
    }
}
