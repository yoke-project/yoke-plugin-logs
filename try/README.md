# Trying logs

This directory is an application project that uses the Plugin in `..`: `yoke-compose init` made it and
`add plugin .. --yes` took the Plugin into it. Nothing here is the Plugin, and taking the Plugin into
another application never takes this directory.

| Path | What it is |
| --- | --- |
| `yoke.yaml` | the application: the unit `logs` and the channel `panel`, a local socket for one client |
| `ui/` | the panel: an interface client written with Slint and the Rust interface library. It shows the newest entries, filters them by unit and by priority, follows the journal as it is written, and pages back to older entries. It reaches the Plugin through the contract alone and links none of its code |
| `follow/` | an interface client: it asks every question, follows every stream and every event of the unit, and prints each as one line |
| `run.sh` | runs the application on a local Core, grants what the Plugin declares, and opens the panel, or follows the unit with `--follow` |

The programs of Yoke are installed from the module proxy, the newest release or a tag:

```sh
for p in yoke yoke-compose yoke-core yoke-guardian yoke-auth; do
  go install "github.com/yoke-project/yoke/cmd/$p@latest"
done
```

```sh
./run.sh                        # the panel; closing it stops everything
./run.sh --follow --seconds 10  # one line per answer, frame and event, in place of the panel
```

The unit reads the journal as the account that runs the Core: what that account may read is all the
panel shows. `logs.permissions` says what it is.

With `--follow`, a page of the journal goes from another terminal, as `run.sh` says:

    echo '{"limit":5,"priority":"err"}' | YOKE_INSTANCE=<instance> yoke unit ask logs logs.entries --input -
