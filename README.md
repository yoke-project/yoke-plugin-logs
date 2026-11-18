# logs

A Yoke Plugin that reads the host's systemd journal: pages of entries selected by unit, priority, boot,
time and text, the journal followed as it is written, and the most urgent entries reported as they come.
It only reads.

| | |
| --- | --- |
| questions | `logs.entries` (a page of entries, the newest or from a cursor), `logs.units`, `logs.boots`, `logs.status`, `logs.permissions` |
| commands | none: the Plugin changes nothing |
| streams | `logs.follow`: every entry the account may read, as it is written, none lost |
| occurrences | `logs.entry.critical` for each entry written at `crit` (70), `alert` (80) or `emerg` (90) |

Every payload is JSON and states `schema`. `logs.entries` takes, each optional:

| Field | |
| --- | --- |
| `scope` | `system`, `user` (the account's own journal), or every journal the account may read, by default |
| `unit`, `identifier` | a name or a list; a `user` scope's units are its user manager's |
| `priority` | `0` to `7` or `emerg` … `debug`: that priority and every more urgent one |
| `boot` | `current`, `previous`, `all` (by default) or a boot ID from `logs.boots` |
| `kernel` | `true`: the kernel's messages alone |
| `since`, `until` | microseconds since the epoch, as the journal counts |
| `grep` | text the message holds, whatever its case |
| `limit` | 100 by default, 1000 at most |
| `before`, `after` | a cursor: the page just older, or just newer, than that entry |

**A page is bounded.** It holds `limit` entries at most and stays within what the Core carries in an
answer; where it is cut it says `more`, and the next page is asked `before` its `first` cursor, or `after`
its `last`. Nothing is left out between two pages. journalctl is given 20 s for a page: a search for text
through a long journal that does not hold it is failed as `timeout`, and asks for a boot, a time, a unit
or a priority to narrow it.

**An entry is the journal's**: the object journalctl writes, every field as the journal keeps it — a
number as a string, a value that is not UTF-8 as an array of bytes, a field above 4 KiB as `null`.
journalctl runs as the account that runs the unit, so the journal's own permissions decide what an entry,
and a page, may hold; `logs.permissions` says what they allow.

**`logs.follow` is a stream of happenings, not of a state.** While it flows, journalctl follows the journal
and waits on its notification of a write; nothing is read at an interval. A frame holds a batch of
entries, `{"schema": 1, "entries": [...]}`. The stream starts a second before it is activated.

**An interface that attaches late** subscribes to `logs.follow`, then asks `logs.entries`, and drops what
the stream brings at or below the `seqnum` it was answered with: every entry carries the journal's own
`__CURSOR` and `__SEQNUM`, counted across every file the journal writes. The stream carries every entry
the account may read, so an interface that asked for a filtered page applies the same filter to it.

`logs.entry.critical` is followed only while it is granted. Its detail carries `line`, the `priority`, the
`unit`, the `cursor`, the `seqnum` and the `entry` whole, which is what an interface client receives.

A unit is handed no `PATH`: journalctl is looked for at `/usr/bin/journalctl`, then `/bin/journalctl`. A
host without it runs the unit, says so in its health, and fails every question as `unavailable`.

Take it into an application with `yoke-compose add plugin https://github.com/yoke-project/yoke-plugin-logs@<tag>`.
Build it with `yoke-compose build`, and try it from `try/`.

Licensed under the Apache License 2.0.
