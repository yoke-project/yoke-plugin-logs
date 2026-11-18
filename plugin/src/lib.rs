//! The systemd journal as a Yoke Plugin: pages of entries, the journal followed as it is written, and the
//! most urgent entries reported.
//!
//! `backend` reads the journal through journalctl and knows nothing of Yoke; `plugin` is the Plugin around
//! it.

pub mod backend;
pub mod plugin;
