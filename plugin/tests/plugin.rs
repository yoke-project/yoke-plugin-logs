//! The Plugin against a journal that is not there: a fake backend stands in for journalctl, and a
//! recorder for the Core.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use logs::backend::*;
use logs::plugin::*;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use yoke_sdk::plugin::{Event, Question};

fn entry(seq: u64, unit: &str, prio: u8, message: &str) -> Entry {
    let v = json!({
        "__CURSOR": format!("s=ab;i={seq:x};b=1"),
        "__REALTIME_TIMESTAMP": (1_000_000 * seq).to_string(),
        "__SEQNUM": seq.to_string(),
        "PRIORITY": prio.to_string(),
        "_SYSTEMD_UNIT": unit,
        "MESSAGE": message,
    });
    v.as_object().unwrap().clone()
}

/// A journal of entries 1 to `n`, every third written by cron at `err`, the rest by sshd at `info`. It
/// remembers the last page it was asked, and hands out followers the test writes to.
struct Fake {
    journal: Mutex<Vec<Entry>>,
    asked: Mutex<Option<Query>>,
    followers: Mutex<Vec<(Follow, mpsc::Sender<Entry>)>>,
}

impl Fake {
    fn new(n: u64) -> Arc<Fake> {
        Arc::new(Fake {
            journal: Mutex::new((1..=n).map(Fake::nth).collect()),
            asked: Mutex::new(None),
            followers: Mutex::new(vec![]),
        })
    }
    fn nth(i: u64) -> Entry {
        if i.is_multiple_of(3) {
            entry(i, "cron.service", 3, &format!("job {i} failed"))
        } else {
            entry(i, "sshd.service", 6, &format!("accepted {i}"))
        }
    }
    /// An entry is written: kept, and given to every follower.
    async fn write(&self, e: Entry) {
        self.journal.lock().unwrap().push(e.clone());
        let followers: Vec<_> = self.followers.lock().unwrap().iter().map(|(f, tx)| (*f, tx.clone())).collect();
        for (f, tx) in followers {
            if f.max_priority.is_none_or(|m| priority(&e).is_some_and(|p| p <= m)) {
                let _ = tx.send(e.clone()).await;
            }
        }
    }
    fn last_query(&self) -> Query {
        self.asked.lock().unwrap().clone().unwrap()
    }
}

#[async_trait]
impl Journal for Fake {
    async fn page(&self, q: &Query) -> Result<Page, JournalError> {
        *self.asked.lock().unwrap() = Some(q.clone());
        if q.grep.as_deref() == Some("forbidden") {
            return Err(JournalError::PermissionDenied("No journal files were opened due to insufficient permissions.".into()));
        }
        if q.grep.as_deref() == Some("nowhere") {
            return Err(JournalError::TimedOut("journalctl did not finish within 20 s".into()));
        }
        if matches!(&q.position, Position::Before(c) | Position::After(c) if c == "s=gone") {
            return Err(JournalError::NotFound("Failed to seek to cursor: Invalid argument".into()));
        }
        let all: Vec<Entry> = self
            .journal
            .lock()
            .unwrap()
            .iter()
            .filter(|e| q.units.is_empty() || q.units.iter().any(|u| text(e, "_SYSTEMD_UNIT").as_deref() == Some(u)))
            .filter(|e| q.max_priority.is_none_or(|m| priority(e).is_some_and(|p| p <= m)))
            .cloned()
            .collect();
        let at = |c: &str| all.iter().position(|e| cursor(e) == Some(c)).unwrap_or(all.len());
        let (entries, more) = match &q.position {
            Position::Newest => {
                let from = all.len().saturating_sub(q.limit);
                (all[from..].to_vec(), from > 0)
            }
            Position::Before(c) => {
                let to = at(c);
                let from = to.saturating_sub(q.limit);
                (all[from..to].to_vec(), from > 0)
            }
            Position::After(c) => {
                let from = (at(c) + 1).min(all.len());
                let to = (from + q.limit).min(all.len());
                (all[from..to].to_vec(), to < all.len())
            }
        };
        Ok(Page { entries, more })
    }
    async fn units(&self, scope: Scope) -> Result<Vec<String>, JournalError> {
        Ok(match scope {
            Scope::User => vec!["app.scope".into(), "dunst.service".into(), "pipewire.service".into()],
            _ => vec!["cron.service".into(), "sshd.service".into()],
        })
    }
    async fn boots(&self) -> Result<Vec<Value>, JournalError> {
        Ok((-29..=0).map(|i| json!({ "index": i, "boot_id": format!("{:032x}", 30 + i), "first_entry": 1, "last_entry": 2 })).collect())
    }
    async fn status(&self) -> Result<Status, JournalError> {
        Ok(Status {
            journalctl: "/usr/bin/journalctl".into(),
            version: "systemd 262".into(),
            newest: self.journal.lock().unwrap().last().cloned(),
        })
    }
    async fn permissions(&self) -> Vec<Permission> {
        vec![Permission { action: "read the journal".into(), result: "yes".into(), how: "ran journalctl".into() }]
    }
    async fn follow(&self, f: Follow) -> Result<mpsc::Receiver<Entry>, JournalError> {
        let (tx, rx) = mpsc::channel(1024);
        self.followers.lock().unwrap().push((f, tx));
        Ok(rx)
    }
}

/// Stands in for the Core: every answer, failure, occurrence and frame, as one line.
#[derive(Default)]
struct Core {
    said: Mutex<Vec<String>>,
}

impl Core {
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.said.lock().unwrap())
    }
    fn push(&self, line: String) {
        self.said.lock().unwrap().push(line);
    }
}

impl Acts for Core {
    async fn answer(&self, _q: &Question, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        assert!(payload.len() < 1024 * 1024, "the Core carries no answer above 1 MiB");
        self.push(format!("answer {}", String::from_utf8_lossy(&payload)));
        Ok(())
    }
    async fn fail(&self, _q: &Question, code: &str, message: &str) -> Result<(), yoke_sdk::base::Error> {
        self.push(format!("fail {code}: {message}"));
        Ok(())
    }
    async fn report(&self, occurrence: &str, severity: u8, line: &str, detail: Value) -> Result<(), yoke_sdk::base::Error> {
        assert_eq!(detail["line"], line, "the detail carries the line a client reads");
        assert!(line.len() <= 1024, "the Core carries a line up to 1 KiB");
        self.push(format!("report {occurrence} {severity} {detail}"));
        Ok(())
    }
    async fn emit(&self, stream: &str, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        assert!(payload.len() < 1024 * 1024, "the Core carries no frame above 1 MiB");
        self.push(format!("emit {stream} {}", String::from_utf8_lossy(&payload)));
        Ok(())
    }
}

fn question(r#type: &str, payload: &str) -> Event {
    Event::Question(Question { id: "q-1".into(), r#type: r#type.into(), payload: payload.as_bytes().to_vec() })
}

fn json_of(line: &str, skip: usize) -> Value {
    serde_json::from_str(line.splitn(skip + 1, ' ').nth(skip).unwrap()).unwrap()
}

async fn ask(l: &Logs, r#type: &str, payload: &str) -> Value {
    let core = Core::default();
    handle(&core, &question(r#type, payload), l).await.unwrap();
    let said = core.take();
    assert_eq!(said.len(), 1, "every declared question is answered or failed, once");
    if said[0].starts_with("fail ") {
        return json!({ "failed": said[0] });
    }
    json_of(&said[0], 1)
}

fn seqnums(page: &Value) -> Vec<u64> {
    page["entries"].as_array().unwrap().iter().map(|e| e["__SEQNUM"].as_str().unwrap().parse().unwrap()).collect()
}

/// Waits until the recorder has said `n` things.
async fn said(core: &Core, n: usize) -> Vec<String> {
    for _ in 0..200 {
        if core.said.lock().unwrap().len() >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    core.take()
}

#[test]
fn the_manifest_kept_is_the_manifest_declared() {
    assert_eq!(declaration().manifest(), include_str!("../com.yoke.logs.yaml"));
}

#[test]
fn every_object_declared_has_one_capability() {
    let d = declaration();
    assert_eq!(d.capabilities.len(), d.queries.len() + d.commands.len() + d.streams.len() + d.occurrences.len());
    assert!(d.commands.is_empty(), "the Plugin only reads");
    assert!(!d.streams[0].tolerates_loss, "an entry lost is not made good by the next");
}

#[tokio::test]
async fn a_page_is_the_newest_entries_oldest_first_and_names_its_cursors() {
    let fake = Fake::new(250);
    let l = Logs::new(Ok(fake.clone()));
    let page = ask(&l, ENTRIES, "").await;
    assert_eq!(page["schema"], 1);
    assert_eq!(seqnums(&page), (151..=250).collect::<Vec<_>>(), "100 by default");
    assert_eq!(page["more"], true);
    assert_eq!(page["seqnum"], 250);
    assert_eq!(page["first"], "s=ab;i=97;b=1");
    assert_eq!(page["last"], "s=ab;i=fa;b=1");
    assert_eq!(page["entries"][0]["MESSAGE"], "accepted 151", "an entry is the journal's, its fields as they are");
    assert_eq!(fake.last_query().position, Position::Newest);

    let all = ask(&l, ENTRIES, r#"{"limit": 50000}"#).await;
    assert_eq!(fake.last_query().limit, MAX_LIMIT, "a page holds 1000 at most");
    assert_eq!(seqnums(&all).len(), 250);
    assert_eq!(all["more"], false);
}

#[tokio::test]
async fn pages_taken_from_cursors_leave_nothing_out_and_give_nothing_twice() {
    let l = Logs::new(Ok(Fake::new(95)));
    let mut seen = vec![];
    let mut page = ask(&l, ENTRIES, r#"{"limit": 30}"#).await;
    loop {
        let mut s = seqnums(&page);
        s.extend(seen);
        seen = s;
        if page["more"] == false {
            break;
        }
        page = ask(&l, ENTRIES, &json!({ "limit": 30, "before": page["first"] }).to_string()).await;
    }
    assert_eq!(seen, (1..=95).collect::<Vec<_>>(), "backwards, every entry once");

    let mut seen = vec![];
    let mut page = ask(&l, ENTRIES, r#"{"limit": 40, "after": "s=ab;i=a;b=1"}"#).await;
    loop {
        seen.extend(seqnums(&page));
        if page["more"] == false {
            break;
        }
        page = ask(&l, ENTRIES, &json!({ "limit": 40, "after": page["last"] }).to_string()).await;
    }
    assert_eq!(seen, (11..=95).collect::<Vec<_>>(), "forwards, every entry once");
}

#[tokio::test]
async fn a_page_selects_as_its_payload_names() {
    let fake = Fake::new(30);
    let l = Logs::new(Ok(fake.clone()));
    let errors = ask(&l, ENTRIES, r#"{"unit": "cron.service", "priority": "err", "limit": 3}"#).await;
    assert_eq!(seqnums(&errors), [24, 27, 30]);
    let q = fake.last_query();
    assert_eq!((q.units, q.max_priority, q.limit), (vec!["cron.service".to_string()], Some(3), 3));

    ask(&l, ENTRIES, r#"{"scope": "user", "unit": ["a.service", "b.service"], "identifier": "sshd", "priority": 4,
        "boot": "previous", "kernel": true, "since": 5, "until": 9, "grep": "fail"}"#)
    .await;
    let q = fake.last_query();
    assert_eq!(q.scope, Scope::User);
    assert_eq!(q.units.len(), 2);
    assert_eq!(q.identifiers, ["sshd"]);
    assert_eq!((q.max_priority, q.boot, q.kernel), (Some(4), Boot::Previous, true));
    assert_eq!((q.since, q.until, q.grep.as_deref()), (Some(5), Some(9), Some("fail")));

    ask(&l, ENTRIES, r#"{"boot": "5c60eaa2c2844e71981eb95373f9eca4", "scope": "system"}"#).await;
    let q = fake.last_query();
    assert_eq!((q.boot, q.scope), (Boot::Id("5c60eaa2c2844e71981eb95373f9eca4".into()), Scope::System));
}

#[tokio::test]
async fn a_page_stays_within_an_answer_and_says_there_is_more() {
    let fake = Fake::new(0);
    for i in 1..=300 {
        fake.journal.lock().unwrap().push(entry(i, "big.service", 6, &"x".repeat(4000)));
    }
    let l = Logs::new(Ok(fake));
    let newest = ask(&l, ENTRIES, r#"{"limit": 300}"#).await;
    let s = seqnums(&newest);
    assert!(s.len() < 300 && *s.last().unwrap() == 300, "the newest are kept");
    assert_eq!(newest["more"], true);
    let older = ask(&l, ENTRIES, &json!({ "limit": 300, "before": newest["first"] }).to_string()).await;
    assert_eq!(*seqnums(&older).last().unwrap() + 1, s[0], "the next page goes on where it was cut");
    let after = ask(&l, ENTRIES, r#"{"limit": 300, "after": "s=ab;i=1;b=1"}"#).await;
    assert_eq!(seqnums(&after)[0], 2, "after a cursor, the oldest are kept");
    assert_eq!(after["more"], true);
}

#[tokio::test]
async fn what_cannot_be_asked_is_failed_with_a_code() {
    let l = Logs::new(Ok(Fake::new(10)));
    for payload in [
        "{oops",
        r#"{"lines": 3}"#,
        r#"{"priority": 9}"#,
        r#"{"priority": "loud"}"#,
        r#"{"limit": 0}"#,
        r#"{"boot": "next"}"#,
        r#"{"scope": "everyone"}"#,
        r#"{"unit": "--output=cat"}"#,
        r#"{"unit": "a b"}"#,
        r#"{"before": "c1", "after": "c2"}"#,
        r#"{"since": 9, "until": 5}"#,
        r#"{"grep": "a\nb"}"#,
    ] {
        let a = ask(&l, ENTRIES, payload).await;
        assert!(a["failed"].as_str().unwrap().starts_with("fail invalid:"), "{payload}: {a}");
    }
    let denied = ask(&l, ENTRIES, r#"{"grep": "forbidden"}"#).await;
    assert!(denied["failed"].as_str().unwrap().starts_with("fail permission_denied:"));
    let long = ask(&l, ENTRIES, r#"{"grep": "nowhere"}"#).await;
    assert!(long["failed"].as_str().unwrap().starts_with("fail timeout:"), "a search too long is said to be, within the Core's wait");
    let gone = ask(&l, ENTRIES, r#"{"before": "s=gone"}"#).await;
    assert!(gone["failed"].as_str().unwrap().starts_with("fail not_found:"));
    for q in [STATUS, PERMISSIONS] {
        assert!(ask(&l, q, r#"{"x": 1}"#).await["failed"].as_str().unwrap().starts_with("fail invalid:"));
    }

    let none = Logs::new(Err(JournalError::Unavailable("no journalctl at /usr/bin/journalctl or /bin/journalctl".into())));
    for q in QUERIES {
        let a = ask(&none, q, "").await;
        assert!(a["failed"].as_str().unwrap().starts_with("fail unavailable: no journalctl"), "{q}: {a}");
    }
}

#[tokio::test]
async fn the_units_the_boots_the_status_and_the_permissions_are_answered_bounded() {
    let fake = Fake::new(12);
    let l = Logs::new(Ok(fake.clone()));
    let units = ask(&l, UNITS, "").await;
    assert_eq!(units["units"], json!(["cron.service", "sshd.service"]));
    assert_eq!(units["more"], false);
    let user = ask(&l, UNITS, r#"{"scope": "user", "limit": 2}"#).await;
    assert_eq!(user["units"], json!(["app.scope", "dunst.service"]));
    assert_eq!(user["more"], true);

    let boots = ask(&l, BOOTS, "").await;
    assert_eq!(boots["boots"].as_array().unwrap().len(), 20, "the newest 20 by default");
    assert_eq!(boots["boots"][19]["index"], 0, "the current boot last");
    assert_eq!(boots["more"], true);
    assert_eq!(ask(&l, BOOTS, r#"{"limit": 100}"#).await["more"], false);

    let status = ask(&l, STATUS, "{}").await;
    assert_eq!(status["version"], "systemd 262");
    assert_eq!(status["following"], false);
    assert_eq!(status["newest"]["seqnum"], 12);
    assert_eq!(status["newest"]["cursor"], "s=ab;i=c;b=1");
    assert_eq!(status["newest"]["at"], 12_000_000);

    let perms = ask(&l, PERMISSIONS, "").await;
    assert_eq!(perms["permissions"][0]["result"], "yes");
}

#[tokio::test]
async fn an_undeclared_question_is_left_alone() {
    let l = Logs::new(Ok(Fake::new(1)));
    let core = Core::default();
    handle(&core, &question("logs.tail", ""), &l).await.unwrap();
    assert!(core.take().is_empty());
}

#[tokio::test]
async fn the_stream_follows_the_journal_while_it_flows_and_from_before_it_was_activated() {
    let fake = Fake::new(5);
    let l = Logs::new(Ok(fake.clone()));
    let core = Arc::new(Core::default());
    let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as u64;
    l.activated(core.clone(), FOLLOW).await.unwrap();
    assert!(l.flowing());
    let start = fake.followers.lock().unwrap()[0].0;
    match start.start {
        Start::Since(us) => assert!(us <= before.saturating_sub(LEAD_US) + 100_000 && us + 2 * LEAD_US > before),
        other => panic!("the stream starts before its activation, not {other:?}"),
    }
    assert_eq!(start.max_priority, None, "every entry the account may read");

    fake.write(Fake::nth(6)).await;
    let frames = said(&core, 1).await;
    let f = json_of(&frames[0], 2);
    assert!(frames[0].starts_with("emit logs.follow "));
    assert_eq!(f["schema"], 1);
    assert_eq!(seqnums(&f), [6]);

    // What has already arrived goes in one frame.
    let (_, tx) = fake.followers.lock().unwrap()[0].clone();
    for i in 7..=20 {
        tx.try_send(Fake::nth(i)).unwrap();
    }
    let frames = said(&core, 1).await;
    let all: Vec<u64> = frames.iter().flat_map(|f| seqnums(&json_of(f, 2))).collect();
    assert_eq!(all, (7..=20).collect::<Vec<_>>());
    assert!(frames.len() < 14, "batched, not one frame an entry");
    assert_eq!(ask(&l, STATUS, "").await["following"], true);

    l.stopped(FOLLOW);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!l.flowing());
    assert!(tx.is_closed(), "the follower, and journalctl with it, ends with the stream");
    fake.write(Fake::nth(21)).await;
    assert!(said(&core, 0).await.is_empty(), "nothing flows once it is stopped");
}

#[tokio::test]
async fn an_interface_that_attaches_late_joins_the_page_and_the_stream_by_sequence_number() {
    let fake = Fake::new(40);
    let l = Logs::new(Ok(fake.clone()));
    let core = Arc::new(Core::default());
    // It subscribes and the stream starts; entries are written before and after it asks.
    l.activated(core.clone(), FOLLOW).await.unwrap();
    for i in 41..=45 {
        fake.write(Fake::nth(i)).await;
    }
    let page = ask(&l, ENTRIES, r#"{"limit": 10}"#).await;
    for i in 46..=50 {
        fake.write(Fake::nth(i)).await;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let mut shown = seqnums(&page);
    let answered = page["seqnum"].as_u64().unwrap();
    for f in core.take() {
        shown.extend(seqnums(&json_of(&f, 2)).into_iter().filter(|s| *s > answered));
    }
    assert_eq!(shown, (36..=50).collect::<Vec<_>>(), "no gap, nothing twice");
    l.stopped(FOLLOW);
}

#[tokio::test]
async fn the_most_urgent_entries_are_reported_with_the_entry_in_the_detail() {
    let l = Logs::new(Ok(Fake::new(0)));
    let core = Core::default();
    let mut e = entry(7, "disk.service", 2, &format!("{}\nsecond line", "y".repeat(3000)));
    l.critical(&core, &e).await.unwrap();
    e.insert("PRIORITY".into(), "0".into());
    l.critical(&core, &e).await.unwrap();
    e.insert("PRIORITY".into(), "3".into());
    l.critical(&core, &e).await.unwrap();
    let said = core.take();
    assert_eq!(said.len(), 2, "err is not reported");
    assert!(said[0].starts_with("report logs.entry.critical 70 "));
    assert!(said[1].starts_with("report logs.entry.critical 90 "));
    let d = json_of(&said[0], 3);
    assert_eq!(d["schema"], 1);
    assert_eq!(d["priority"], 2);
    assert_eq!(d["unit"], "disk.service");
    assert_eq!(d["seqnum"], 7);
    assert!(d["line"].as_str().unwrap().starts_with("crit disk.service: yyy"));
    assert!(!d["line"].as_str().unwrap().contains("second line"));
    assert_eq!(d["entry"]["MESSAGE"].as_str().unwrap().len(), 3012, "the entry whole");
    assert_eq!((severity(0), severity(1), severity(2)), (90, 80, 70));
}
