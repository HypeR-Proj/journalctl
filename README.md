<!--
SPDX-FileCopyrightText: 2026 roolrz
SPDX-License-Identifier: Apache-2.0
-->

# journalctl

Reader for the volatile Native service journal.

Uses `lib-journal-client` query records and `lib-service-ipc` through the delegated
journal query connector. Text mode supports component filters and polling; JSON mode
returns one bounded cursor page.

See the [implementation entry](src/main.rs) and [component declaration](component.json)
for the public interfaces, build dependencies and delivered artifacts.

## Usage

In a Native session with the required capabilities:

```sh
journalctl -n 20
journalctl --json --limit 128
```

## Boundaries

Records are retained in memory and may be evicted. The client reports retention loss,
rejected sources and mirror drops. JSON pages are limited to 128 records; follow mode
polls rather than subscribing to a persistent stream.

## Build and test

Use this checkout inside the [repo-managed workspace](https://github.com/HypeR-Proj/HypeR).
From this repository directory:

```sh
make
make test
```

An absolute `OUT=/path/to/out` selects a shared output directory. The module
entry delegates to build/system, which prepares the installed SDK and declared
dependencies. Native binaries require HypeR; host module checks cover the tests
selected by component.json. A standalone clone does not contain the workspace’s
other source repositories or Native SDK.
