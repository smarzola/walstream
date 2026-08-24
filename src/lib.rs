#![forbid(unsafe_code)]

//! Core storage and log primitives for Walstream.

mod codec;
pub mod config;
pub mod log;
pub mod protocol;
pub mod server;
pub mod storage;
mod wire;
