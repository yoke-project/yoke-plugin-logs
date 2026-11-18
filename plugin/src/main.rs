//! logs: the systemd journal as a Yoke Plugin unit.

use std::sync::Arc;

use logs::backend::{Follow, Journal, Journalctl, Start};
use logs::plugin::{declaration, handle, Logs, CRITICAL, CRITICAL_PRIORITY};
use yoke_sdk::plugin::{start, Event};

#[tokio::main]
async fn main() {
    // `logs --manifest` prints the Manifest the declaration generates, which the bundle carries.
    if std::env::args().nth(1).as_deref() == Some("--manifest") {
        print!("{}", declaration().manifest());
        return;
    }
    let unit = match start(&declaration()).await {
        Ok(unit) => Arc::new(unit),
        Err(err) => {
            eprintln!("logs: {err}");
            std::process::exit(1);
        }
    };
    let backend = Journalctl::find().map(|j| Arc::new(j) as Arc<dyn Journal>);
    let (grade, line) = match &backend {
        Ok(j) => match j.status().await {
            Ok(s) if s.newest.is_some() => (0, format!("the journal is readable ({})", s.version)),
            Ok(s) => (50, format!("{} reads no entry: the account may read none", s.journalctl)),
            Err(e) => (50, format!("the journal cannot be read: {e}")),
        },
        Err(e) => (50, e.to_string()),
    };
    if let Err(err) = unit.health(grade, &line).await {
        eprintln!("logs: {err}");
    }
    let logs = Logs::new(backend);
    // The most urgent entries are followed only where their occurrence is granted: nothing is read for
    // nobody.
    let granted = unit.admission().granted.occurrences.iter().any(|o| o == CRITICAL);
    let mut critical = match (&logs.backend, granted) {
        (Ok(j), true) => j.follow(Follow { start: Start::Now, max_priority: Some(CRITICAL_PRIORITY) }).await.ok(),
        _ => None,
    };
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("logs listens for SIGTERM");
    loop {
        tokio::select! {
            event = unit.next() => match event {
                None | Some(Event::Ended(_)) => return,
                Some(Event::Activated(a)) => {
                    if let Err(err) = logs.activated(unit.clone(), &a.stream).await {
                        eprintln!("logs: {}: {}", a.stream, logs::plugin::message(&err));
                    }
                }
                Some(Event::Stopped { stream }) => logs.stopped(&stream),
                // Each question on its own task: a page that takes journalctl a while holds no other.
                Some(event) => {
                    let (unit, logs) = (unit.clone(), logs.clone());
                    tokio::spawn(async move {
                        if let Err(err) = handle(unit.as_ref(), &event, &logs).await {
                            eprintln!("logs: {err}");
                        }
                    });
                }
            },
            e = async { match critical.as_mut() { Some(rx) => rx.recv().await, None => std::future::pending().await } } => match e {
                Some(e) => {
                    if let Err(err) = logs.critical(unit.as_ref(), &e).await {
                        eprintln!("logs: {err}");
                    }
                }
                None => {
                    critical = None;
                    let _ = unit.health(50, "the journal can no longer be followed").await;
                }
            },
            _ = term.recv() => {
                logs.stopped(logs::plugin::FOLLOW);
                let _ = unit.close().await;
                while let Some(event) = unit.next().await {
                    if matches!(event, Event::Ended(_)) {
                        break;
                    }
                }
                return;
            }
        }
    }
}
