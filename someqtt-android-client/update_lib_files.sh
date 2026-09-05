#!/bin/bash

RUST_BACKTRACE=1 cargo test

RAW_NDK_PATH=$(cat .cargo/config.toml | grep NDK_HOME | cut -d '"' -f2)

if [ -z "$RAW_NDK_PATH" ]; then
    echo "Error: Could not find NDK_HOME in .cargo/config.toml"
    exit 1
fi

NDK_PATH=$(realpath "$RAW_NDK_PATH" 2>/dev/null)

echo "NDK_HOME exists at: $NDK_PATH"

cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 -t x86 build --release

if [ -z "$1" ]; then
    echo "Error: Please provide the path to your Android Studio project."
    echo "Usage: $0 '/path/to/AndroidStudioProjects/MyApplication'"
    exit 1
fi

ANDROID_PROJECT_PATH="$1"
ANDROID_LIBS_DIR="${ANDROID_PROJECT_PATH}/app/src/main/jniLibs"
RUST_TARGET_DIR="$(pwd)/target"

if [ ! -d "$ANDROID_LIBS_DIR" ]; then
    echo "Error: jniLibs directory not found at: $ANDROID_LIBS_DIR"
    exit 1
fi

echo "Copying libraries from ${RUST_TARGET_DIR} to ${ANDROID_LIBS_DIR}"

copy_lib() {
    local src_arch_dir=$1
    local dest_arch_dir=$2
    local lib_name="libsocket_phone.so"
    local src_file="${RUST_TARGET_DIR}/${src_arch_dir}/release/${lib_name}"
    local dest_dir="${ANDROID_LIBS_DIR}/${dest_arch_dir}"

    if [ ! -f "$src_file" ]; then
        echo "Warning: File not found, skipping: $src_file"
        return 1
    fi
    mkdir -p "$dest_dir"
    cp "$src_file" "$dest_dir/"
    echo "OK: Copied ${src_arch_dir} -> ${dest_arch_dir}"
}

copy_lib "aarch64-linux-android"   "arm64-v8a"
copy_lib "armv7-linux-androideabi" "armeabi-v7a"
copy_lib "x86_64-linux-android"    "x86_64"
copy_lib "i686-linux-android"      "x86"

echo "Done."
