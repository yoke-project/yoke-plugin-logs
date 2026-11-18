# The watch

| | |
| --- | --- |
| **Feature** | this Plugin against what Yoke last published |
| **Planning item** | yoke-project/yoke-plugin-logs#1 |

## yoke-plugin-logs:the-watch.01 — built and taken by the newest release of Yoke, the Plugin runs under its Core and answers its status and a page of the journal

| Field | Value |
| --- | --- |
| **Cites** | specs/95.40 |
| **Level** | L3 |
| **Method** | check |
| **Not applicable in** | — |
| **Label** | blocking |
| **Precondition** | the newest published programs of Yoke; journalctl or not |
| **Action** | build the Plugin, take it into `try/`, run it, grant `logs.status` and `logs.entries` and ask them |
| **Expected** | the instance comes up; the status names journalctl's version and a page holds an entries list where journalctl is installed, and both are failed for want of it where none is |
