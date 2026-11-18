//! Bridges the window and the logs Plugin, through the interface contract alone.
//!
//! - **Reading.** The panel subscribes to `logs.follow` first, then asks `logs.entries` for the newest
//!   entries its filter selects, and drops what the stream brings at or below the newest `__SEQNUM` it was
//!   answered with: nothing is shown twice, and nothing written meanwhile is missed. A filter changed, or
//!   following resumed, is the same again: a page, then the stream after it.
//! - **Older entries** are asked a page at a time before the oldest one shown.
//! - **The stream carries every entry the account may read**, so the panel applies its filter to what
//!   flows, as journalctl applied it to the page.
//! - One task owns what is shown: frames, answers and the person's choices reach it in turn, so that a page
//!   and the frames around it are joined in order.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde_json::{json, Map, Value};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel, Weak};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::time::sleep;
use yoke_sdk::interface::{Attachment, Options};

use crate::{EntryRow, MainWindow};

const ENTRIES: &str = "logs.entries";
const UNITS: &str = "logs.units";
const FOLLOW: &str = "logs.follow";
const PAGE: usize = 200;
/// The most entries the window keeps; the oldest go first.
const SHOWN: usize = 5000;
const REDRAW: Duration = Duration::from_millis(300);
const AGAIN: Duration = Duration::from_secs(2);
const ALL_UNITS: &str = "all units";
const ANY_PRIORITY: &str = "any priority";
const PRIORITIES: [&str; 8] = ["emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"];

type Entry = Map<String, Value>;

pub struct Shared {
    unit: String,
    attachment: OnceLock<Arc<Attachment>>,
    started: Mutex<bool>,
}

enum Msg {
    Frame(Vec<Entry>),
    /// A page, for the generation of the filter it was asked with; `older` where it goes before.
    Page { generation: u64, older: bool, page: Result<Value, String> },
    Filter(String, String),
    Following(bool),
    Older,
}

#[derive(Default)]
struct Filter {
    unit: Option<String>,
    priority: Option<u8>,
}

impl Filter {
    fn payload(&self) -> Map<String, Value> {
        let mut p = Map::new();
        p.insert("limit".into(), json!(PAGE));
        if let Some(u) = &self.unit {
            p.insert("unit".into(), json!(u));
        }
        if let Some(n) = self.priority {
            p.insert("priority".into(), json!(n));
        }
        p
    }

    /// What journalctl selected for the page, applied to what flows: the unit the entry is of or is about,
    /// and its priority.
    fn matches(&self, e: &Entry) -> bool {
        if let Some(u) = &self.unit {
            let of = ["_SYSTEMD_UNIT", "UNIT", "OBJECT_SYSTEMD_UNIT"].iter().any(|f| text(e, f).as_deref() == Some(u.as_str()));
            if !of {
                return false;
            }
        }
        match (self.priority, priority(e)) {
            (Some(max), Some(p)) => p <= max,
            (Some(_), None) => false,
            _ => true,
        }
    }
}

/// A journal field as text: a number is written as a string, and a value that is not UTF-8 as bytes.
fn text(e: &Entry, field: &str) -> Option<String> {
    match e.get(field)? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Array(a) if a.iter().all(Value::is_u64) => {
            Some(String::from_utf8_lossy(&a.iter().map(|b| b.as_u64().unwrap_or(63) as u8).collect::<Vec<_>>()).into_owned())
        }
        _ => None,
    }
}

fn priority(e: &Entry) -> Option<u8> {
    text(e, "PRIORITY")?.parse().ok().filter(|p| *p <= 7)
}

fn seqnum(e: &Entry) -> u64 {
    text(e, "__SEQNUM").and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Microseconds since the epoch, in local time.
fn local(us: u64) -> String {
    let t = (us / 1_000_000) as libc::time_t;
    // SAFETY: tm is plain data, and localtime_r writes into it alone.
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    if unsafe { libc::localtime_r(&t, &mut tm) }.is_null() {
        return us.to_string();
    }
    format!(
        "{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        (us % 1_000_000) / 1000
    )
}

fn row(e: &Entry) -> EntryRow {
    let p = priority(e);
    let source = ["_SYSTEMD_UNIT", "_SYSTEMD_USER_UNIT", "SYSLOG_IDENTIFIER", "_COMM"].iter().find_map(|f| text(e, f)).unwrap_or_default();
    let message = text(e, "MESSAGE").unwrap_or_default();
    let mut one: String = message.trim_end().replace('\n', " ⏎ ");
    if one.len() > 400 {
        let mut end = 400;
        while !one.is_char_boundary(end) {
            end -= 1;
        }
        one.truncate(end);
        one.push('…');
    }
    EntryRow {
        time: text(e, "__REALTIME_TIMESTAMP").and_then(|t| t.parse().ok()).map(local).unwrap_or_default().into(),
        priority: p.map(i32::from).unwrap_or(-1),
        level: p.map(|p| PRIORITIES[p as usize]).unwrap_or("-").into(),
        source: source.into(),
        message: one.into(),
    }
}

fn say(ui: &Weak<MainWindow>, msg: impl Into<String>) {
    let m = msg.into();
    let _ = ui.upgrade_in_event_loop(move |u| u.set_message(m.into()));
}

pub fn start(rt: Handle, address: String, unit: String, ui: &MainWindow) -> Arc<Shared> {
    let shared = Arc::new(Shared { unit: unit.clone(), attachment: OnceLock::new(), started: Mutex::new(false) });
    let weak = ui.as_weak();
    let (tx, rx) = mpsc::unbounded_channel::<Msg>();

    let mut priorities = vec![SharedString::from(ANY_PRIORITY)];
    priorities.extend(PRIORITIES.iter().map(|p| SharedString::from(*p)));
    ui.set_priorities(ModelRc::new(VecModel::from(priorities)));
    {
        let tx = tx.clone();
        ui.on_filter_changed(move |u, p| {
            let _ = tx.send(Msg::Filter(u.to_string(), p.to_string()));
        });
    }
    {
        let tx = tx.clone();
        ui.on_follow_toggled(move |on| {
            let _ = tx.send(Msg::Following(on));
        });
    }
    {
        let tx = tx.clone();
        ui.on_load_older(move || {
            let _ = tx.send(Msg::Older);
        });
    }

    let shared2 = shared.clone();
    rt.spawn(async move {
        let at = match Attachment::attach(&address, Options::new()).await {
            Ok(at) => Arc::new(at),
            Err(e) => {
                say(&weak, format!("Cannot attach to {address}: {e}"));
                return;
            }
        };
        let _ = shared2.attachment.set(at.clone());
        say(&weak, "Attached");
        tokio::spawn(units(at.clone(), unit.clone(), weak.clone()));
        tokio::spawn(follow(at.clone(), unit.clone(), shared2, tx.clone(), weak.clone()));
        own(at, unit, weak, tx, rx).await;
    });
    shared
}

/// Once the window has closed: the stream, if this panel started it, is stopped.
pub async fn finish(shared: Arc<Shared>) {
    let Some(at) = shared.attachment.get() else { return };
    if *shared.started.lock().unwrap() {
        let _ = tokio::time::timeout(Duration::from_secs(2), at.stop_stream(&shared.unit, FOLLOW)).await;
    }
}

async fn ask(at: &Attachment, unit: &str, r#type: &str, payload: Value) -> Result<Value, String> {
    let answer = at.ask(unit, r#type, payload.to_string().as_bytes()).await.map_err(|e| e.to_string())?;
    serde_json::from_slice(&answer).map_err(|e| e.to_string())
}

/// The units the system manager's journal names, for the filter.
async fn units(at: Arc<Attachment>, unit: String, ui: Weak<MainWindow>) {
    let list = match ask(&at, &unit, UNITS, json!({ "scope": "system", "limit": 2000 })).await {
        Ok(v) => v["units"].as_array().cloned().unwrap_or_default(),
        Err(e) => {
            say(&ui, format!("The units cannot be asked: {e}"));
            return;
        }
    };
    let mut model = vec![SharedString::from(ALL_UNITS)];
    model.extend(list.iter().filter_map(Value::as_str).map(SharedString::from));
    let _ = ui.upgrade_in_event_loop(move |u| u.set_units(ModelRc::new(VecModel::from(model))));
}

/// The stream, subscribed to again should its delivery end; every frame goes to the owner.
async fn follow(at: Arc<Attachment>, unit: String, shared: Arc<Shared>, tx: mpsc::UnboundedSender<Msg>, ui: Weak<MainWindow>) {
    loop {
        match at.subscribe_stream(&unit, FOLLOW).await {
            Ok(mut delivery) => {
                if !delivery.flowing {
                    match at.start_stream(&unit, FOLLOW).await {
                        Ok(_) => *shared.started.lock().unwrap() = true,
                        Err(e) => say(&ui, format!("{FOLLOW} cannot be started: {e}")),
                    }
                }
                // Subscribed: now the page, which the stream goes on from.
                let _ = tx.send(Msg::Following(true));
                loop {
                    match delivery.next().await {
                        Ok(Some(frame)) => {
                            let Ok(v) = serde_json::from_slice::<Value>(&frame.payload) else { continue };
                            let entries = v["entries"].as_array().into_iter().flatten().filter_map(|e| e.as_object().cloned()).collect();
                            let _ = tx.send(Msg::Frame(entries));
                        }
                        Ok(None) => break,
                        Err(e) => {
                            say(&ui, format!("{FOLLOW}: {e}"));
                            break;
                        }
                    }
                }
                say(&ui, format!("{FOLLOW}: the delivery ended; subscribing again"));
            }
            Err(e) => say(&ui, format!("{FOLLOW} cannot be subscribed to: {e}")),
        }
        sleep(AGAIN).await;
    }
}

struct Shown {
    rows: VecDeque<Entry>,
    filter: Filter,
    generation: u64,
    /// The newest `__SEQNUM` shown or answered: what the stream brings at or below it is dropped.
    seen: u64,
    /// A page of the newest is awaited: frames wait for it.
    waiting: Option<Vec<Entry>>,
    following: bool,
    oldest: Option<String>,
    older_left: bool,
    dirty: bool,
}

/// The one task that decides what is shown.
async fn own(at: Arc<Attachment>, unit: String, ui: Weak<MainWindow>, tx: mpsc::UnboundedSender<Msg>, mut rx: mpsc::UnboundedReceiver<Msg>) {
    let mut s = Shown {
        rows: VecDeque::new(),
        filter: Filter::default(),
        generation: 0,
        seen: 0,
        waiting: None,
        following: false,
        oldest: None,
        older_left: false,
        dirty: false,
    };
    let page = |s: &mut Shown, payload: Map<String, Value>, older: bool| {
        let (at, unit, tx, generation) = (at.clone(), unit.clone(), tx.clone(), s.generation);
        tokio::spawn(async move {
            let page = ask(&at, &unit, ENTRIES, Value::Object(payload)).await;
            let _ = tx.send(Msg::Page { generation, older, page });
        });
    };
    let mut redraw = tokio::time::interval(REDRAW);
    loop {
        tokio::select! {
            m = rx.recv() => {
                let Some(m) = m else { return };
                match m {
                    Msg::Frame(entries) => match s.waiting.as_mut() {
                        Some(held) => held.extend(entries),
                        None if s.following => {
                            for e in entries {
                                let n = seqnum(&e);
                                if n > s.seen {
                                    s.seen = n;
                                    if s.filter.matches(&e) {
                                        s.rows.push_back(e);
                                        s.dirty = true;
                                    }
                                }
                            }
                        }
                        None => {}
                    },
                    Msg::Filter(u, p) => {
                        s.filter = Filter {
                            unit: (u != ALL_UNITS && !u.is_empty()).then_some(u),
                            priority: PRIORITIES.iter().position(|x| *x == p).map(|n| n as u8),
                        };
                        s.generation += 1;
                        s.waiting = Some(vec![]);
                        let payload = s.filter.payload();
                        page(&mut s, payload, false);
                    }
                    Msg::Following(on) => {
                        s.following = on;
                        if on {
                            s.generation += 1;
                            s.waiting = Some(vec![]);
                            let payload = s.filter.payload();
                            page(&mut s, payload, false);
                        }
                    }
                    Msg::Older => {
                        if let Some(c) = s.oldest.clone() {
                            let mut payload = s.filter.payload();
                            payload.insert("before".into(), json!(c));
                            page(&mut s, payload, true);
                        }
                    }
                    Msg::Page { generation, older, page } => {
                        if generation != s.generation {
                            continue;
                        }
                        let v = match page {
                            Ok(v) => v,
                            Err(e) => {
                                say(&ui, format!("{ENTRIES} cannot be asked: {e}"));
                                if !older {
                                    s.waiting = None;
                                }
                                continue;
                            }
                        };
                        let entries: Vec<Entry> = v["entries"].as_array().into_iter().flatten().filter_map(|e| e.as_object().cloned()).collect();
                        if older {
                            for e in entries.into_iter().rev() {
                                s.rows.push_front(e);
                            }
                        } else {
                            s.rows = entries.into();
                            s.seen = v["seqnum"].as_u64().unwrap_or(0);
                            // What flowed meanwhile goes on from the page, past what it already holds.
                            let held = s.waiting.take().unwrap_or_default();
                            if s.following {
                                for e in held {
                                    let n = seqnum(&e);
                                    if n > s.seen {
                                        s.seen = n;
                                        if s.filter.matches(&e) {
                                            s.rows.push_back(e);
                                        }
                                    }
                                }
                            }
                            say(&ui, "Ready");
                        }
                        s.oldest = s.rows.front().and_then(|e| text(e, "__CURSOR"));
                        s.older_left = v["more"].as_bool().unwrap_or(false);
                        s.dirty = true;
                    }
                }
            }
            _ = redraw.tick() => {
                if !s.dirty {
                    continue;
                }
                s.dirty = false;
                while s.rows.len() > SHOWN {
                    s.rows.pop_front();
                    s.oldest = s.rows.front().and_then(|e| text(e, "__CURSOR"));
                }
                let rows: Vec<EntryRow> = s.rows.iter().map(row).collect();
                let counted = format!("{} entries{}", rows.len(), if s.following { ", following" } else { "" });
                let older_left = s.older_left;
                let _ = ui.upgrade_in_event_loop(move |u| {
                    u.set_entries(ModelRc::new(VecModel::from(rows)));
                    u.set_counted(counted.into());
                    u.set_older_left(older_left);
                });
            }
        }
    }
}
