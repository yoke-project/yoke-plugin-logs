//! The systemd journal, read through journalctl.
//!
//! [`Journalctl`] asks journalctl for a page of entries, the units and the boots the journal holds, and
//! follows the journal as it is written: `journalctl --follow` waits on the journal's own notification of
//! a write, and reads nothing at an interval. Every entry is the object journalctl writes, its fields as
//! the journal keeps them; nothing is reformatted. journalctl runs as the account the unit runs as, so the
//! journal's own permissions decide what an entry, and a page, may hold.
//!
//! A unit is handed no `PATH`: journalctl is looked for where systemd installs it.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

/// Where journalctl is looked for, in order.
pub const JOURNALCTL: [&str; 2] = ["/usr/bin/journalctl", "/bin/journalctl"];

/// The longest one run of journalctl is given, within the Core's wait for an answer: a search through a
/// long journal for text it does not hold reads every entry it has.
pub const RUN_WAIT: Duration = Duration::from_secs(20);

/// One journal entry: the object journalctl writes, every value as the journal gives it.
pub type Entry = Map<String, Value>;

#[derive(Debug, Clone, PartialEq)]
pub enum JournalError {
    /// No journalctl, or nothing it can read.
    Unavailable(String),
    /// The journal's permissions keep the account out.
    PermissionDenied(String),
    /// No such cursor, no such boot.
    NotFound(String),
    /// This journalctl cannot do what was asked: a text search, without pattern matching.
    Unsupported(String),
    /// journalctl did not finish within [`RUN_WAIT`].
    TimedOut(String),
    Failed(String),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalError::Unavailable(m)
            | JournalError::PermissionDenied(m)
            | JournalError::NotFound(m)
            | JournalError::Unsupported(m)
            | JournalError::TimedOut(m)
            | JournalError::Failed(m) => f.write_str(m),
        }
    }
}

/// Which journals a query reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Scope {
    /// Every journal the account may read: the system's, and its own.
    #[default]
    All,
    System,
    /// The account's own, its user manager's units among them.
    User,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Boot {
    #[default]
    All,
    Current,
    Previous,
    /// A boot ID, as `logs.boots` names it.
    Id(String),
}

/// Where a page is taken.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Position {
    /// The newest entries.
    #[default]
    Newest,
    /// The entries just older than this cursor.
    Before(String),
    /// The entries just newer than this cursor.
    After(String),
}

/// A page of entries: what journalctl selects, where it starts, and how many.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Query {
    pub scope: Scope,
    /// Units of the system manager, or of the account's own where the scope is `User`.
    pub units: Vec<String>,
    pub identifiers: Vec<String>,
    /// This priority and every more urgent one, 0 (emerg) to 7 (debug).
    pub max_priority: Option<u8>,
    pub boot: Boot,
    /// The kernel's messages alone.
    pub kernel: bool,
    /// Microseconds since the epoch, as the journal counts.
    pub since: Option<u64>,
    pub until: Option<u64>,
    /// Text the message holds, whatever its case.
    pub grep: Option<String>,
    pub position: Position,
    pub limit: usize,
}

/// A page, oldest first, and whether the journal holds more beyond it, in the direction it was taken.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Page {
    pub entries: Vec<Entry>,
    pub more: bool,
}

/// Where a follower starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    /// With the next entry written.
    Now,
    /// With the entries written since this time, microseconds since the epoch.
    Since(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Follow {
    pub start: Start,
    pub max_priority: Option<u8>,
}

/// Something the account may or may not read, and how that was found.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Permission {
    pub action: String,
    /// `yes` or `no`.
    pub result: String,
    pub how: String,
}

/// The reader and the journal it reads.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Status {
    pub journalctl: String,
    /// journalctl's first line: `systemd 262 (262.2-1-arch)`.
    pub version: String,
    /// The newest entry the account may read.
    pub newest: Option<Entry>,
}

#[async_trait]
pub trait Journal: Send + Sync + 'static {
    async fn page(&self, q: &Query) -> Result<Page, JournalError>;
    /// The units that have written to the journal, sorted.
    async fn units(&self, scope: Scope) -> Result<Vec<String>, JournalError>;
    /// The boots the journal holds, oldest first, as journalctl lists them.
    async fn boots(&self) -> Result<Vec<Value>, JournalError>;
    async fn status(&self) -> Result<Status, JournalError>;
    async fn permissions(&self) -> Vec<Permission>;
    /// The entries as they are written, until the receiver is dropped. A reader that dies is started
    /// again after the last entry it gave, so that none is lost or given twice.
    async fn follow(&self, f: Follow) -> Result<mpsc::Receiver<Entry>, JournalError>;
}

/// The journal's sequence number of an entry. A journal older than systemd 254 writes no `__SEQNUM`;
/// its cursor's `i=` holds the same number, in hex.
pub fn seqnum(e: &Entry) -> Option<u64> {
    if let Some(n) = e.get("__SEQNUM").and_then(number) {
        return Some(n);
    }
    cursor(e)?.split(';').find_map(|p| p.strip_prefix("i=")).and_then(|h| u64::from_str_radix(h, 16).ok())
}

pub fn cursor(e: &Entry) -> Option<&str> {
    e.get("__CURSOR").and_then(Value::as_str)
}

/// A journal field as text: the journal writes a number as a string, and a value that is not UTF-8 as an
/// array of bytes.
pub fn text(e: &Entry, field: &str) -> Option<String> {
    match e.get(field)? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Array(a) if a.iter().all(Value::is_u64) => {
            Some(String::from_utf8_lossy(&a.iter().map(|b| b.as_u64().unwrap_or(63) as u8).collect::<Vec<_>>()).into_owned())
        }
        Value::Array(a) => a.iter().find_map(|v| v.as_str().map(str::to_string)),
        _ => None,
    }
}

pub fn priority(e: &Entry) -> Option<u8> {
    text(e, "PRIORITY")?.trim().parse().ok().filter(|p| *p <= 7)
}

fn number(v: &Value) -> Option<u64> {
    match v {
        Value::String(s) => s.trim().parse().ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

/// One line of `journalctl --output=json`; `None` for anything that is not an entry. Where the journal
/// writes no `__SEQNUM`, it is added from the cursor, so that every entry carries it.
pub fn parse_line(line: &str) -> Option<Entry> {
    let line = line.trim();
    if !line.starts_with('{') {
        return None;
    }
    let mut e: Entry = serde_json::from_str(line).ok()?;
    if !e.contains_key("__SEQNUM") {
        if let Some(n) = seqnum(&e) {
            e.insert("__SEQNUM".into(), Value::String(n.to_string()));
        }
    }
    Some(e)
}

/// Escapes text to be found as it is, for `--grep`, which takes a pattern.
pub fn escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        if !(c.is_alphanumeric() || c == ' ' || c == '_') {
            o.push('\\');
        }
        o.push(c);
    }
    o
}

fn at(us: u64) -> String {
    format!("@{}.{:06}", us / 1_000_000, us % 1_000_000)
}

/// What every run of journalctl is given: JSON, one entry a line, and no pager.
fn base() -> Vec<String> {
    vec!["--output=json".into(), "--no-pager".into(), "--quiet".into()]
}

/// The selection a query makes, apart from where it starts. Every value is given as `--option=value`, so
/// that none can be taken for an option.
fn selection(q: &Query, a: &mut Vec<String>) {
    match q.scope {
        Scope::All => {}
        Scope::System => a.push("--system".into()),
        Scope::User => a.push("--user".into()),
    }
    match &q.boot {
        Boot::All => {}
        Boot::Current => a.push("--boot=0".into()),
        Boot::Previous => a.push("--boot=-1".into()),
        Boot::Id(id) => a.push(format!("--boot={id}")),
    }
    let unit = if q.scope == Scope::User { "--user-unit" } else { "--unit" };
    a.extend(q.units.iter().map(|u| format!("{unit}={u}")));
    a.extend(q.identifiers.iter().map(|i| format!("--identifier={i}")));
    if let Some(p) = q.max_priority {
        a.push(format!("--priority={p}"));
    }
    if let Some(s) = q.since {
        a.push(format!("--since={}", at(s)));
    }
    if let Some(u) = q.until {
        a.push(format!("--until={}", at(u)));
    }
    if let Some(t) = q.grep.as_deref().filter(|t| !t.is_empty()) {
        a.push(format!("--grep={}", escape(t)));
        a.push("--case-sensitive=false".into());
    }
}

/// journalctl's arguments for a page, and how many lines to read of what it writes: one more than the
/// page, to know whether the journal holds more; for an older page, one more again, since journalctl
/// starts with the entry the cursor names.
pub fn page_args(q: &Query) -> (Vec<String>, usize) {
    let mut a = base();
    selection(q, &mut a);
    let take = match &q.position {
        Position::Newest => {
            a.push(format!("--lines={}", q.limit + 1));
            q.limit + 1
        }
        Position::Before(c) => {
            a.push(format!("--cursor={c}"));
            a.push("--reverse".into());
            q.limit + 2
        }
        Position::After(c) => {
            a.push(format!("--after-cursor={c}"));
            q.limit + 1
        }
    };
    if q.kernel {
        a.push("_TRANSPORT=kernel".into());
    }
    (a, take)
}

/// The lines read for a page, as the page: oldest first, `limit` at most, and whether more lie beyond.
pub fn page_of(q: &Query, lines: Vec<String>) -> Page {
    let mut entries: Vec<Entry> = lines.iter().filter_map(|l| parse_line(l)).collect();
    let more;
    match &q.position {
        Position::Newest => {
            more = entries.len() > q.limit;
            let cut = entries.len().saturating_sub(q.limit);
            entries.drain(..cut);
        }
        Position::Before(c) => {
            if entries.first().and_then(cursor) == Some(c.as_str()) {
                entries.remove(0);
            }
            more = entries.len() > q.limit;
            entries.truncate(q.limit);
            entries.reverse();
        }
        Position::After(_) => {
            more = entries.len() > q.limit;
            entries.truncate(q.limit);
        }
    }
    Page { entries, more }
}

/// journalctl's arguments for a follower: from where it starts, or after the last entry it gave.
pub fn follow_args(f: &Follow, after: Option<&str>) -> Vec<String> {
    let mut a = base();
    a.push("--follow".into());
    if let Some(p) = f.max_priority {
        a.push(format!("--priority={p}"));
    }
    match (after, f.start) {
        (Some(c), _) => a.push(format!("--after-cursor={c}")),
        (None, Start::Now) => a.push("--lines=0".into()),
        (None, Start::Since(us)) => a.push(format!("--since={}", at(us))),
    }
    a
}

/// What journalctl's complaint says went wrong.
pub fn complaint(stderr: &str) -> JournalError {
    let s = stderr.trim().to_string();
    let lower = s.to_lowercase();
    if lower.contains("permission denied") || lower.contains("insufficient permissions") {
        JournalError::PermissionDenied(s)
    } else if lower.contains("cursor")
        || (lower.contains("boot") && (lower.contains("not available") || lower.contains("no such") || lower.contains("not found") || lower.contains("found for"))) {
        JournalError::NotFound(s)
    } else if lower.contains("pattern") || lower.contains("pcre") || lower.contains("--grep") {
        JournalError::Unsupported(format!("this journalctl cannot search text: {s}"))
    } else if s.is_empty() {
        JournalError::Failed("journalctl failed and said nothing".into())
    } else {
        JournalError::Failed(s)
    }
}

/// journalctl, found where systemd installs it.
pub struct Journalctl {
    path: PathBuf,
}

/// What one run of journalctl wrote.
struct Ran {
    lines: Vec<String>,
    /// It was stopped once enough was read.
    cut: bool,
    /// It was stopped at [`RUN_WAIT`].
    timed_out: bool,
    success: bool,
    code: Option<i32>,
    stderr: String,
}

impl Journalctl {
    pub fn find() -> Result<Journalctl, JournalError> {
        JOURNALCTL
            .iter()
            .map(Path::new)
            .find(|p| p.is_file())
            .map(|p| Journalctl { path: p.to_path_buf() })
            .ok_or_else(|| JournalError::Unavailable(format!("no journalctl at {}", JOURNALCTL.join(" or "))))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn command(&self, args: &[String]) -> Command {
        let mut c = Command::new(&self.path);
        // Nothing of the unit's own environment, its token among it, reaches journalctl.
        c.args(args).env_clear().stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
        c
    }

    /// Runs journalctl and reads at most `take` lines of what it writes; once they are read it is stopped.
    async fn run(&self, args: &[String], take: usize) -> Result<Ran, JournalError> {
        let mut child = self.command(args).spawn().map_err(|e| JournalError::Unavailable(format!("journalctl cannot run: {e}")))?;
        let stderr = child.stderr.take();
        let complaint = tokio::spawn(async move {
            let mut s = String::new();
            if let Some(e) = stderr {
                let _ = e.take(8192).read_to_string(&mut s).await;
            }
            s
        });
        let mut lines = Vec::new();
        let mut cut = false;
        let mut timed_out = false;
        if let Some(out) = child.stdout.take() {
            let mut out = BufReader::new(out).lines();
            let deadline = tokio::time::sleep(RUN_WAIT);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    l = out.next_line() => match l {
                        Ok(Some(l)) => {
                            lines.push(l);
                            if lines.len() >= take {
                                cut = true;
                                break;
                            }
                        }
                        _ => break,
                    },
                    _ = &mut deadline => {
                        timed_out = true;
                        break;
                    }
                }
            }
        }
        if cut || timed_out {
            let _ = child.kill().await;
        }
        let status = child.wait().await.map_err(|e| JournalError::Failed(format!("journalctl: {e}")))?;
        let stderr = if cut || timed_out { String::new() } else { complaint.await.unwrap_or_default() };
        Ok(Ran { lines, cut, timed_out, success: status.success(), code: status.code(), stderr })
    }

    /// A run read whole, or what its complaint says went wrong.
    async fn read(&self, args: &[String], take: usize) -> Result<Vec<String>, JournalError> {
        let ran = self.run(args, take).await?;
        if ran.timed_out {
            return Err(timed_out());
        }
        if ran.cut || ran.success {
            return Ok(ran.lines);
        }
        // `--grep` ends with 1, and says nothing, where no entry matches: that is an empty page.
        if args.iter().any(|a| a.starts_with("--grep=")) && ran.code == Some(1) && ran.lines.is_empty() && ran.stderr.trim().is_empty() {
            return Ok(vec![]);
        }
        Err(complaint(&ran.stderr))
    }
}

fn timed_out() -> JournalError {
    JournalError::TimedOut(format!(
        "journalctl did not finish within {} s: name a boot, a time, a unit or a priority to read less of the journal",
        RUN_WAIT.as_secs()
    ))
}

/// Whether a `.journal` file under `dir` opens; `None` where there is none to try.
fn journal_file_opens(dir: &str, name: impl Fn(&str) -> bool) -> Option<(bool, String)> {
    for machine in std::fs::read_dir(dir).ok()?.flatten() {
        let Ok(files) = std::fs::read_dir(machine.path()) else { continue };
        for f in files.flatten() {
            let n = f.file_name().to_string_lossy().into_owned();
            if n.ends_with(".journal") && name(&n) {
                return Some((std::fs::File::open(f.path()).is_ok(), f.path().display().to_string()));
            }
        }
    }
    None
}

fn uid() -> u32 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata("/proc/self").map(|m| m.uid()).unwrap_or(u32::MAX)
}

fn permission(action: &str, yes: bool, how: String) -> Permission {
    Permission { action: action.into(), result: if yes { "yes" } else { "no" }.into(), how }
}

#[async_trait]
impl Journal for Journalctl {
    async fn page(&self, q: &Query) -> Result<Page, JournalError> {
        let (args, take) = page_args(q);
        if q.position == Position::Newest {
            return Ok(page_of(q, self.read(&args, take).await?));
        }
        // Older or newer than a cursor, journalctl writes as it reads: what it found before the wait ran
        // out is a page, and the next one is asked from its cursor.
        let ran = self.run(&args, take).await?;
        if ran.timed_out {
            let mut page = page_of(q, ran.lines);
            if page.entries.is_empty() {
                return Err(timed_out());
            }
            page.more = true;
            return Ok(page);
        }
        Ok(page_of(q, self.read(&args, take).await?))
    }

    async fn units(&self, scope: Scope) -> Result<Vec<String>, JournalError> {
        let mut a = vec!["--no-pager".to_string()];
        match scope {
            Scope::User => a.extend(["--user".into(), "--field=_SYSTEMD_USER_UNIT".into()]),
            Scope::System => a.extend(["--system".into(), "--field=_SYSTEMD_UNIT".into()]),
            Scope::All => a.push("--field=_SYSTEMD_UNIT".into()),
        }
        let mut units: Vec<String> = self.read(&a, usize::MAX).await?.into_iter().filter(|u| !u.is_empty()).collect();
        units.sort();
        units.dedup();
        Ok(units)
    }

    async fn boots(&self) -> Result<Vec<Value>, JournalError> {
        let a = vec!["--list-boots".to_string(), "--output=json".into(), "--no-pager".into()];
        let text = self.read(&a, usize::MAX).await?.join("\n");
        match serde_json::from_str::<Value>(&text) {
            Ok(Value::Array(boots)) => Ok(boots),
            _ => Err(JournalError::Unsupported("this journalctl does not list the boots as JSON".into())),
        }
    }

    async fn status(&self) -> Result<Status, JournalError> {
        let version = self.read(&["--version".to_string()], 1).await?.into_iter().next().unwrap_or_default();
        let mut a = base();
        a.push("--lines=1".into());
        let newest = self.read(&a, 1).await?.first().and_then(|l| parse_line(l));
        Ok(Status { journalctl: self.path.display().to_string(), version, newest })
    }

    async fn permissions(&self) -> Vec<Permission> {
        let mut out = vec![];
        for (action, scope) in [
            ("read the journal", None),
            ("read the system's entries", Some("--system")),
            ("read the account's own entries", Some("--user")),
        ] {
            let mut a = vec!["--output=json".to_string(), "--no-pager".into(), "--lines=1".into()];
            a.extend(scope.map(str::to_string));
            let shown = format!("journalctl {}", a.join(" "));
            out.push(match self.run(&a, 1).await {
                Ok(r) => {
                    let got = r.lines.first().and_then(|l| parse_line(l)).is_some();
                    let said = r.stderr.trim();
                    let said = if said.is_empty() { String::new() } else { format!("; it said: {said}") };
                    permission(action, got, format!("ran `{shown}`: {}{said}", if got { "an entry" } else { "no entry" }))
                }
                Err(e) => permission(action, false, format!("could not run journalctl: {e}")),
            });
        }
        let me = uid();
        for (action, dir) in [
            ("open the persistent journal's files", "/var/log/journal"),
            ("open the volatile journal's files", "/run/log/journal"),
        ] {
            out.push(match journal_file_opens(dir, |n| n.starts_with("system")) {
                Some((ok, f)) => permission(action, ok, format!("opened {f}")),
                None => permission(action, false, format!("no system journal file to open under {dir}")),
            });
        }
        let mine = format!("user-{me}");
        // `user-1000.journal`, or one archived as `user-1000@….journal`; never `user-100000`'s.
        let is_mine = |n: &str| n == format!("{mine}.journal") || n.starts_with(&format!("{mine}@"));
        let own = journal_file_opens("/var/log/journal", is_mine).or_else(|| journal_file_opens("/run/log/journal", is_mine));
        out.push(match own {
            Some((ok, f)) => permission("open the account's own journal file", ok, format!("opened {f}")),
            None => permission(
                "open the account's own journal file",
                false,
                format!("no {mine}.journal: journald keeps one apart only on a persistent journal, and for an account other than root"),
            ),
        });
        out
    }

    async fn follow(&self, f: Follow) -> Result<mpsc::Receiver<Entry>, JournalError> {
        let (tx, rx) = mpsc::channel(1024);
        let path = self.path.clone();
        tokio::spawn(async move { follow(Journalctl { path }, f, tx).await });
        Ok(rx)
    }
}

/// The follower: journalctl `--follow`, read line by line, started again after the last entry it gave
/// should it end. It ends when the receiver is dropped.
async fn follow(j: Journalctl, f: Follow, tx: mpsc::Sender<Entry>) {
    let mut last: Option<String> = None;
    let mut attempt: u32 = 0;
    loop {
        let started = Instant::now();
        let said = match j.command(&follow_args(&f, last.as_deref())).spawn() {
            Err(e) => {
                eprintln!("logs: journalctl cannot run: {e}");
                return;
            }
            Ok(mut child) => {
                let stderr = child.stderr.take();
                let complaint = tokio::spawn(async move {
                    let mut s = String::new();
                    if let Some(e) = stderr {
                        let _ = e.take(8192).read_to_string(&mut s).await;
                    }
                    s
                });
                if let Some(out) = child.stdout.take() {
                    let mut lines = BufReader::new(out).lines();
                    loop {
                        tokio::select! {
                            _ = tx.closed() => return,
                            l = lines.next_line() => match l {
                                Ok(Some(l)) => {
                                    let Some(e) = parse_line(&l) else { continue };
                                    if let Some(c) = cursor(&e) {
                                        last = Some(c.to_string());
                                    }
                                    if tx.send(e).await.is_err() {
                                        return;
                                    }
                                }
                                _ => break,
                            }
                        }
                    }
                }
                let _ = child.kill().await;
                complaint.await.unwrap_or_default()
            }
        };
        if matches!(complaint(&said), JournalError::PermissionDenied(_)) && last.is_none() {
            eprintln!("logs: the follower is refused: {}", said.trim());
            return;
        }
        eprintln!("logs: journalctl --follow ended{}", if said.trim().is_empty() { String::new() } else { format!(": {}", said.trim()) });
        if started.elapsed() > Duration::from_secs(10) {
            attempt = 0;
        }
        attempt += 1;
        let delay = Duration::from_millis((500u64 << (attempt - 1).min(6)).min(30_000));
        tokio::select! {
            _ = tx.closed() => return,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(position: Position, limit: usize) -> Query {
        Query { position, limit, ..Default::default() }
    }

    fn line(i: u64) -> String {
        format!(r#"{{"__CURSOR":"s=ab;i={i:x};b=1","__SEQNUM":"{i}","MESSAGE":"m{i}"}}"#)
    }

    #[test]
    fn a_page_selects_through_options_that_cannot_be_taken_for_others() {
        let query = Query {
            scope: Scope::User,
            units: vec!["-x".into()],
            identifiers: vec!["sshd".into()],
            max_priority: Some(3),
            boot: Boot::Previous,
            kernel: true,
            since: Some(1_700_000_000_500_000),
            grep: Some("a.b".into()),
            limit: 20,
            ..Default::default()
        };
        let (a, take) = page_args(&query);
        assert_eq!(
            a.join(" "),
            "--output=json --no-pager --quiet --user --boot=-1 --user-unit=-x --identifier=sshd --priority=3 \
             --since=@1700000000.500000 --grep=a\\.b --case-sensitive=false --lines=21 _TRANSPORT=kernel"
        );
        assert_eq!(take, 21);
        let (a, take) = page_args(&q(Position::Before("c1".into()), 5));
        assert_eq!((a[3..].join(" "), take), ("--cursor=c1 --reverse".into(), 7));
        let (a, take) = page_args(&q(Position::After("c1".into()), 5));
        assert_eq!((a[3..].join(" "), take), ("--after-cursor=c1".into(), 6));
    }

    #[test]
    fn pages_are_oldest_first_and_say_whether_more_lie_beyond() {
        let newest = page_of(&q(Position::Newest, 2), (1..=3).map(line).collect());
        assert_eq!(newest.entries.iter().filter_map(seqnum).collect::<Vec<_>>(), [2, 3]);
        assert!(newest.more);
        // Older: journalctl gives the cursor's own entry first, then backwards.
        let older = page_of(&q(Position::Before("s=ab;i=5;b=1".into()), 2), [5, 4, 3, 2].into_iter().map(line).collect());
        assert_eq!(older.entries.iter().filter_map(seqnum).collect::<Vec<_>>(), [3, 4]);
        assert!(older.more);
        let after = page_of(&q(Position::After("x".into()), 3), [6, 7].into_iter().map(line).collect());
        assert_eq!(after.entries.iter().filter_map(seqnum).collect::<Vec<_>>(), [6, 7]);
        assert!(!after.more);
    }

    #[test]
    fn a_follower_starts_where_it_is_told_and_again_after_its_last_entry() {
        let f = Follow { start: Start::Since(2_000_001), max_priority: Some(2) };
        assert_eq!(follow_args(&f, None).join(" "), "--output=json --no-pager --quiet --follow --priority=2 --since=@2.000001");
        assert_eq!(follow_args(&f, Some("c9")).join(" "), "--output=json --no-pager --quiet --follow --priority=2 --after-cursor=c9");
        let now = Follow { start: Start::Now, max_priority: None };
        assert_eq!(follow_args(&now, None).join(" "), "--output=json --no-pager --quiet --follow --lines=0");
    }

    #[test]
    fn an_entry_is_carried_as_the_journal_wrote_it_with_its_sequence_number() {
        let l = r#"{"__CURSOR":"s=ab;i=4eace1;b=5c","PRIORITY":"6","MESSAGE":[104,105,255],"_SYSTEMD_UNIT":"a.service"}"#;
        let e = parse_line(l).unwrap();
        assert_eq!(seqnum(&e), Some(0x4eace1));
        assert_eq!(e["__SEQNUM"], "5156065", "added from the cursor where the journal writes none");
        assert_eq!(e.keys().next().map(String::as_str), Some("__CURSOR"), "the journal's order is kept");
        assert_eq!(e["MESSAGE"], serde_json::json!([104, 105, 255]), "a value that is not UTF-8 stays as the journal gives it");
        assert_eq!(text(&e, "MESSAGE").unwrap(), "hi\u{fffd}");
        assert_eq!(priority(&e), Some(6));
        assert!(parse_line("-- No entries --").is_none());
        assert!(parse_line("{not json").is_none());
    }

    #[test]
    fn complaints_are_told_apart() {
        assert!(matches!(complaint("Failed to seek to cursor: Invalid argument"), JournalError::NotFound(_)));
        assert!(matches!(complaint("Data from the specified boot (-9) is not available: No such boot ID in journal"), JournalError::NotFound(_)));
        assert!(matches!(complaint("No journal boot entry found for the specified boot (0123+0)."), JournalError::NotFound(_)));
        assert!(matches!(complaint("No journal files were opened due to insufficient permissions."), JournalError::PermissionDenied(_)));
        assert!(matches!(complaint("Compiled without pattern matching support"), JournalError::Unsupported(_)));
        assert_eq!(escape("a.b (c)*"), "a\\.b \\(c\\)\\*");
    }
}
