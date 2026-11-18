//! The logs Plugin: the systemd journal, read-only, as questions, a stream and an occurrence.
//!
//! - **`logs.entries` is a page**: the newest entries, or those just older or just newer than a cursor,
//!   selected by scope, unit, identifier, priority, boot, time and text. A page holds at most 1000 entries
//!   and stays within an answer's bound; where it is cut, it says there is `more`, and the next page is
//!   asked from its cursor. Nothing is ever left out between two pages.
//! - **`logs.follow` is a stream of happenings, not of a state**: every entry the account may read, as it
//!   is written, in batches, while the stream flows. It starts a second before it was activated.
//! - **The two are joined by the journal's own sequence number**: every entry carries `__CURSOR` and
//!   `__SEQNUM`. An interface that attaches late subscribes to `logs.follow`, then asks `logs.entries`, and
//!   drops what the stream brings at or below the newest `__SEQNUM` it was answered with.
//! - **`logs.entry.critical`** is reported for each entry written at `crit` or more urgent, while the
//!   occurrence is granted; its detail carries the entry.
//! - **An entry is the journal's**: the object journalctl writes, every field as the journal keeps it, and
//!   only what the account may read; nothing is reformatted.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use yoke_sdk::plugin::{Capability, Declaration, Event, Object, Question, Severity, Stream, Unit};

use crate::backend::{
    cursor, priority, seqnum, text, Boot, Entry, Follow, Journal, JournalError, Position, Query, Scope, Start,
};

pub const PLUGIN: &str = "com.yoke.logs";
/// The format every answer, frame and detail states as `schema`.
pub const SCHEMA: u32 = 1;

pub const ENTRIES: &str = "logs.entries";
pub const UNITS: &str = "logs.units";
pub const BOOTS: &str = "logs.boots";
pub const STATUS: &str = "logs.status";
pub const PERMISSIONS: &str = "logs.permissions";
pub const QUERIES: [&str; 5] = [ENTRIES, UNITS, BOOTS, STATUS, PERMISSIONS];

pub const FOLLOW: &str = "logs.follow";
pub const STREAMS: [&str; 1] = [FOLLOW];

pub const CRITICAL: &str = "logs.entry.critical";
pub const OCCURRENCES: [&str; 1] = [CRITICAL];

/// The entries a page holds when none is named, and the most it holds.
pub const DEFAULT_LIMIT: usize = 100;
pub const MAX_LIMIT: usize = 1000;
/// The units an answer names when none is named, and the most.
pub const DEFAULT_UNITS: usize = 1000;
pub const MAX_UNITS: usize = 5000;
/// The boots an answer names when none is named, and the most.
pub const DEFAULT_BOOTS: usize = 20;
pub const MAX_BOOTS: usize = 1000;
/// No answer is larger than this: the Core carries one of 1 MiB at most.
pub const ANSWER_BUDGET: usize = 900 * 1024;
/// No frame is larger than this, nor holds more entries.
pub const FRAME_BUDGET: usize = 512 * 1024;
pub const FRAME_ENTRIES: usize = 1000;
/// How far before its activation the stream starts, so that nothing written meanwhile is missed.
pub const LEAD_US: u64 = 1_000_000;
/// The most urgent priorities, reported as `logs.entry.critical`: emerg, alert, crit.
pub const CRITICAL_PRIORITY: u8 = 2;
/// The Core carries an occurrence's line up to 1 KiB.
pub const LINE_BUDGET: usize = 1000;

pub fn declaration() -> Declaration {
    let answer = |q: &&str| Capability { name: format!("query.{q}.answer"), governs: Object::Query(q.to_string()) };
    let publish = |s: &&str| Capability { name: format!("stream.{s}.publish"), governs: Object::Stream(s.to_string()) };
    let report = |o: &&str| Capability { name: format!("event.{o}.report"), governs: Object::Occurrence(o.to_string()) };
    Declaration {
        id: PLUGIN.into(),
        queries: QUERIES.iter().map(|q| q.to_string()).collect(),
        streams: vec![
            // An entry is a happening: one lost is not made good by the next.
            Stream { id: FOLLOW.into(), tolerates_loss: false, tolerates_reorder: false, max_frame: 0 },
        ],
        occurrences: OCCURRENCES.iter().map(|o| o.to_string()).collect(),
        capabilities: QUERIES
            .iter()
            .map(answer)
            .chain(STREAMS.iter().map(publish))
            .chain(OCCURRENCES.iter().map(report))
            .collect(),
        ..Default::default()
    }
}

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    Journal(JournalError),
}

impl From<JournalError> for Error {
    fn from(e: JournalError) -> Self {
        Error::Journal(e)
    }
}

pub fn code(e: &Error) -> &'static str {
    match e {
        Error::Invalid(_) => "invalid",
        Error::Journal(JournalError::Unavailable(_)) => "unavailable",
        Error::Journal(JournalError::PermissionDenied(_)) => "permission_denied",
        Error::Journal(JournalError::NotFound(_)) => "not_found",
        Error::Journal(JournalError::Unsupported(_)) => "unsupported",
        Error::Journal(JournalError::TimedOut(_)) => "timeout",
        Error::Journal(JournalError::Failed(_)) => "backend",
    }
}

pub fn message(e: &Error) -> String {
    match e {
        Error::Invalid(m) => m.clone(),
        Error::Journal(e) => e.to_string(),
    }
}

fn input<T: DeserializeOwned>(what: &str, payload: &[u8]) -> Result<T, Error> {
    let payload = if payload.iter().all(u8::is_ascii_whitespace) { b"{}".as_slice() } else { payload };
    serde_json::from_slice(payload).map_err(|e| {
        let kind = match e.classify() {
            serde_json::error::Category::Data => "a field is missing, unknown or of the wrong type",
            _ => "it is not JSON",
        };
        Error::Invalid(format!("the payload is not what {what} takes: {kind}, at line {} column {}", e.line(), e.column()))
    })
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NoInput {}

/// One name or several.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Names {
    One(String),
    Many(Vec<String>),
}

impl Names {
    fn list(n: Option<Names>) -> Vec<String> {
        match n {
            None => vec![],
            Some(Names::One(s)) => vec![s],
            Some(Names::Many(v)) => v,
        }
    }
}

/// A syslog priority as a number, 0 to 7, or its name.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Priority {
    Number(u8),
    Name(String),
}

pub const PRIORITIES: [&str; 8] = ["emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"];

/// Payload of `logs.entries`: every field optional.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EntriesInput {
    /// `system`, `user`, or every journal the account may read.
    pub scope: Option<String>,
    pub unit: Option<Names>,
    pub identifier: Option<Names>,
    /// This priority and every more urgent one.
    pub priority: Option<Priority>,
    /// `current`, `previous`, `all` (by default), or a boot ID.
    pub boot: Option<String>,
    pub kernel: bool,
    /// Microseconds since the epoch.
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub grep: Option<String>,
    pub limit: Option<usize>,
    pub before: Option<String>,
    pub after: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UnitsInput {
    pub scope: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BootsInput {
    pub limit: Option<usize>,
}

fn scope(s: Option<&str>) -> Result<Scope, Error> {
    match s {
        None | Some("all") => Ok(Scope::All),
        Some("system") => Ok(Scope::System),
        Some("user") => Ok(Scope::User),
        Some(o) => Err(Error::Invalid(format!("the scope is system, user or all, and {o} is none of them"))),
    }
}

/// A name handed to journalctl: never empty, never taken for an option, and on one line.
fn name(what: &str, s: &str) -> Result<String, Error> {
    if s.is_empty() || s.starts_with('-') || s.len() > 256 || s.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(Error::Invalid(format!("{what} {s:?} is not a name journalctl takes")));
    }
    Ok(s.to_string())
}

fn limit(n: Option<usize>, default: usize, max: usize) -> Result<usize, Error> {
    match n {
        Some(0) => Err(Error::Invalid("the limit is at least 1".into())),
        Some(n) => Ok(n.min(max)),
        None => Ok(default),
    }
}

impl EntriesInput {
    /// The page the payload asks for; a value out of range is invalid, a limit above the most is the most.
    pub fn query(self) -> Result<Query, Error> {
        let scope = scope(self.scope.as_deref())?;
        let max_priority = match self.priority {
            None => None,
            Some(Priority::Number(p)) if p <= 7 => Some(p),
            Some(Priority::Number(p)) => return Err(Error::Invalid(format!("a priority runs from 0 to 7, and {p} is not one"))),
            Some(Priority::Name(n)) => Some(
                PRIORITIES
                    .iter()
                    .position(|p| *p == n)
                    .ok_or_else(|| Error::Invalid(format!("{n} is not a priority: {}", PRIORITIES.join(", "))))? as u8,
            ),
        };
        let boot = match self.boot.as_deref() {
            None | Some("all") => Boot::All,
            Some("current") => Boot::Current,
            Some("previous") => Boot::Previous,
            Some(id) if id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit()) => Boot::Id(id.to_string()),
            Some(o) => return Err(Error::Invalid(format!("the boot is current, previous, all or a boot ID, and {o} is none of them"))),
        };
        let position = match (self.before, self.after) {
            (Some(_), Some(_)) => return Err(Error::Invalid("a page is taken before a cursor or after one, not both".into())),
            (Some(c), None) => Position::Before(name("the cursor", &c)?),
            (None, Some(c)) => Position::After(name("the cursor", &c)?),
            (None, None) => Position::Newest,
        };
        if let (Some(s), Some(u)) = (self.since, self.until) {
            if s > u {
                return Err(Error::Invalid("since is later than until".into()));
            }
        }
        let grep = self.grep.filter(|t| !t.is_empty());
        if grep.as_ref().is_some_and(|t| t.len() > 1024 || t.chars().any(char::is_control)) {
            return Err(Error::Invalid("the text searched for is one line, of 1 KiB at most".into()));
        }
        Ok(Query {
            scope,
            units: Names::list(self.unit).iter().map(|u| name("the unit", u)).collect::<Result<_, _>>()?,
            identifiers: Names::list(self.identifier).iter().map(|i| name("the identifier", i)).collect::<Result<_, _>>()?,
            max_priority,
            boot,
            kernel: self.kernel,
            since: self.since,
            until: self.until,
            grep,
            position,
            limit: limit(self.limit, DEFAULT_LIMIT, MAX_LIMIT)?,
        })
    }
}

fn now_us() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

/// The size an entry takes in an answer.
fn size(e: &Entry) -> usize {
    serde_json::to_string(e).map(|s| s.len() + 1).unwrap_or(0)
}

/// Keeps the entries that fit in `budget`, from the end nearest where the page was taken: the newest for a
/// page of the newest or of older entries, the oldest for a page of newer ones. Whether any was left out.
pub fn fit(entries: &mut Vec<Entry>, keep_newest: bool, budget: usize) -> bool {
    let mut used = 0;
    let mut kept = 0;
    let order: Box<dyn Iterator<Item = &Entry>> = if keep_newest { Box::new(entries.iter().rev()) } else { Box::new(entries.iter()) };
    for e in order {
        used += size(e);
        if used > budget {
            break;
        }
        kept += 1;
    }
    let cut = entries.len() - kept;
    if keep_newest {
        entries.drain(..cut);
    } else {
        entries.truncate(kept);
    }
    cut > 0
}

/// Up to 1 KiB of text, cut where a character ends.
fn bounded(s: &str, budget: usize) -> String {
    if s.len() <= budget {
        return s.to_string();
    }
    let mut end = budget;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// Who wrote an entry: its unit, else its identifier, else its command.
pub fn source(e: &Entry) -> String {
    ["_SYSTEMD_UNIT", "_SYSTEMD_USER_UNIT", "SYSLOG_IDENTIFIER", "_COMM"]
        .iter()
        .find_map(|f| text(e, f))
        .unwrap_or_else(|| "-".into())
}

/// The severity an occurrence states for the priority it reports: emerg 90, alert 80, crit 70.
pub fn severity(p: u8) -> u8 {
    90 - 10 * p.min(CRITICAL_PRIORITY)
}

/// What the Plugin does on its Session, so a test can stand in for the Unit.
pub trait Acts: Send + Sync + 'static {
    fn answer(&self, q: &Question, payload: Vec<u8>) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>> + Send;
    fn fail(&self, q: &Question, code: &str, message: &str) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>> + Send;
    /// An occurrence, its facts as JSON in its detail.
    fn report(&self, occurrence: &str, severity: u8, line: &str, detail: Value) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>> + Send;
    fn emit(&self, stream: &str, payload: Vec<u8>) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>> + Send;
}

impl Acts for Unit {
    async fn answer(&self, q: &Question, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        Unit::answer(self, q, payload).await
    }
    async fn fail(&self, q: &Question, code: &str, message: &str) -> Result<(), yoke_sdk::base::Error> {
        Unit::fail(self, &q.id, code, message).await
    }
    async fn report(&self, occurrence: &str, severity: u8, line: &str, detail: Value) -> Result<(), yoke_sdk::base::Error> {
        Unit::report(self, occurrence, Some(Severity::of(severity)), line, detail.to_string().into_bytes()).await
    }
    async fn emit(&self, stream: &str, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        Unit::emit(self, stream, payload).await
    }
}

pub type Backend = Result<Arc<dyn Journal>, JournalError>;

pub struct Logs {
    pub backend: Backend,
    /// The stream's follower, while it flows.
    following: Mutex<Option<JoinHandle<()>>>,
}

impl Logs {
    pub fn new(backend: Backend) -> Arc<Logs> {
        Arc::new(Logs { backend, following: Mutex::new(None) })
    }

    fn journal(&self) -> Result<&dyn Journal, Error> {
        match &self.backend {
            Ok(b) => Ok(b.as_ref()),
            Err(e) => Err(Error::Journal(JournalError::Unavailable(e.to_string()))),
        }
    }

    pub fn flowing(&self) -> bool {
        self.following.lock().unwrap().as_ref().is_some_and(|h| !h.is_finished())
    }

    pub async fn answer(&self, query: &str, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let v = match query {
            ENTRIES => {
                let q = input::<EntriesInput>(query, payload)?.query()?;
                let page = self.journal()?.page(&q).await?;
                let mut entries = page.entries;
                let keep_newest = !matches!(q.position, Position::After(_));
                let cut = fit(&mut entries, keep_newest, ANSWER_BUDGET);
                json!({
                    "schema": SCHEMA,
                    "entries": entries,
                    "more": page.more || cut,
                    "first": entries.first().and_then(cursor),
                    "last": entries.last().and_then(cursor),
                    "seqnum": entries.iter().filter_map(seqnum).max(),
                })
            }
            UNITS => {
                let i: UnitsInput = input(query, payload)?;
                let (scope, limit) = (scope(i.scope.as_deref())?, limit(i.limit, DEFAULT_UNITS, MAX_UNITS)?);
                let mut units = self.journal()?.units(scope).await?;
                let more = units.len() > limit;
                units.truncate(limit);
                json!({ "schema": SCHEMA, "units": units, "more": more })
            }
            BOOTS => {
                let i: BootsInput = input(query, payload)?;
                let limit = limit(i.limit, DEFAULT_BOOTS, MAX_BOOTS)?;
                let mut boots = self.journal()?.boots().await?;
                let cut = boots.len().saturating_sub(limit);
                boots.drain(..cut);
                json!({ "schema": SCHEMA, "boots": boots, "more": cut > 0 })
            }
            STATUS => {
                let _: NoInput = input(query, payload)?;
                let s = self.journal()?.status().await?;
                json!({
                    "schema": SCHEMA,
                    "journalctl": s.journalctl,
                    "version": s.version,
                    "following": self.flowing(),
                    "newest": s.newest.as_ref().map(|e| json!({
                        "cursor": cursor(e),
                        "seqnum": seqnum(e),
                        "at": text(e, "__REALTIME_TIMESTAMP").and_then(|t| t.parse::<u64>().ok()),
                    })),
                })
            }
            PERMISSIONS => {
                let _: NoInput = input(query, payload)?;
                json!({ "schema": SCHEMA, "permissions": self.journal()?.permissions().await })
            }
            other => return Err(Error::Invalid(format!("{other} is not a question of {PLUGIN}"))),
        };
        Ok(v.to_string().into_bytes())
    }

    /// A stream was activated: its follower starts, a second before now, and flows until it is stopped.
    pub async fn activated<A: Acts>(&self, u: Arc<A>, stream: &str) -> Result<(), Error> {
        if stream != FOLLOW {
            return Ok(());
        }
        let rx = self.journal()?.follow(Follow { start: Start::Since(now_us().saturating_sub(LEAD_US)), max_priority: None }).await?;
        let task = tokio::spawn(async move { flow(u.as_ref(), rx).await });
        if let Some(before) = self.following.lock().unwrap().replace(task) {
            before.abort();
        }
        Ok(())
    }

    /// The stream was stopped: its follower ends, and journalctl with it.
    pub fn stopped(&self, stream: &str) {
        if stream == FOLLOW {
            if let Some(task) = self.following.lock().unwrap().take() {
                task.abort();
            }
        }
    }

    /// One entry written at `crit` or more urgent: reported, its facts in the detail.
    pub async fn critical(&self, u: &impl Acts, e: &Entry) -> Result<(), yoke_sdk::base::Error> {
        let Some(p) = priority(e).filter(|p| *p <= CRITICAL_PRIORITY) else { return Ok(()) };
        let message = text(e, "MESSAGE").unwrap_or_default();
        let first = message.lines().next().unwrap_or("");
        let line = bounded(&format!("{} {}: {first}", PRIORITIES[p as usize], source(e)), LINE_BUDGET);
        let detail = json!({
            "schema": SCHEMA,
            "line": line,
            "priority": p,
            "unit": source(e),
            "cursor": cursor(e),
            "seqnum": seqnum(e),
            "entry": e,
        });
        u.report(CRITICAL, severity(p), &line, detail).await
    }
}

/// What the follower gives, emitted in batches: an entry is awaited, and whatever else has already
/// arrived goes with it, up to a frame's bound. Ends when the follower does.
pub async fn flow(u: &impl Acts, mut rx: mpsc::Receiver<Entry>) {
    while let Some(first) = rx.recv().await {
        let mut used = size(&first);
        let mut batch = vec![first];
        while batch.len() < FRAME_ENTRIES && used < FRAME_BUDGET {
            match rx.try_recv() {
                Ok(e) => {
                    used += size(&e);
                    batch.push(e);
                }
                Err(_) => break,
            }
        }
        let frame = json!({ "schema": SCHEMA, "entries": batch }).to_string().into_bytes();
        if let Err(err) = u.emit(FOLLOW, frame).await {
            eprintln!("logs: {FOLLOW}: {err}");
        }
    }
}

/// Does what one event of the Session asks: a declared question is answered, or failed with a code;
/// anything else is left alone.
pub async fn handle(u: &impl Acts, event: &Event, l: &Logs) -> Result<(), yoke_sdk::base::Error> {
    match event {
        Event::Question(q) if QUERIES.contains(&q.r#type.as_str()) => match l.answer(&q.r#type, &q.payload).await {
            Ok(answer) => u.answer(q, answer).await,
            Err(e) => u.fail(q, code(&e), &message(&e)).await,
        },
        _ => Ok(()),
    }
}
