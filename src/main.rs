// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut component = None;
    let mut follow = false;
    let mut count = 100;
    let mut json = false;
    let mut cursor = hyper_journal::Query {
        component: None,
        after: 0,
        through: None,
        limit: 128,
    };
    let mut paging = false;
    let mut lines = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-c" | "--component" => {
                component = Some(args.next().ok_or("--component requires PATH")?)
            }
            "-n" | "--lines" => {
                lines = true;
                count = args
                    .next()
                    .ok_or("--lines requires a count")?
                    .parse::<usize>()?
            }
            "-f" | "--follow" => follow = true,
            "--json" => json = true,
            "--after" | "--through" | "--limit" => {
                paging = true;
                let value = args.next().ok_or("cursor option requires an integer")?;
                match arg.as_str() {
                    "--after" => cursor.after = value.parse()?,
                    "--through" => cursor.through = Some(value.parse()?),
                    _ => cursor.limit = value.parse()?,
                }
            }
            "-h" | "--help" => {
                println!(
                    "Usage: journalctl [-c PATH] [-n COUNT] [-f]\n       journalctl --json [-c PATH] [--after N] [--through N] [--limit N]\nRead the volatile service journal. --follow polls for new records.\nRecords include generation, stdout/stderr source and monotonic time. Retention loss is reported.\n--json returns one bounded query page with cursors and loss counters (limit 1..128)."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown option: {arg}").into()),
        }
    }
    if count == 0 || count > 4096 {
        return Err("line count must be 1..4096".into());
    }
    if (paging && !json) || (json && (follow || lines)) || !(1..=128).contains(&cursor.limit) {
        return Err(
            "JSON paging requires --json, without --follow or --lines; limit is 1..128".into(),
        );
    }
    let startup = hyper_rt::process::startup()?;
    let connector = startup.borrow(hyper_journal::QUERY)?;
    if json {
        cursor.component = component;
        let page: hyper_journal::Page = hyper_service_ipc::request(connector, &cursor)?;
        println!("{}", serde_json::to_string(&page)?);
        return Ok(());
    }
    let mut after = 0;
    let mut through = None;
    let mut losses = (0, 0, 0);
    let mut initial = true;
    let mut collected = std::collections::VecDeque::new();
    loop {
        let page: hyper_journal::Page = hyper_service_ipc::request(
            connector,
            &hyper_journal::Query {
                component: component.clone(),
                after,
                through,
                limit: 128,
            },
        )?;
        let current_losses = (
            page.discarded_records,
            page.rejected_sources,
            page.mirror_dropped_bytes,
        );
        if current_losses != losses {
            eprintln!(
                "journalctl: {} records discarded by retention; {} sources rejected; {} mirror bytes dropped",
                page.discarded_records, page.rejected_sources, page.mirror_dropped_bytes
            );
            losses = current_losses;
        }
        if initial && through.is_none() {
            through = Some(page.newest);
        }
        let advanced = page.next > after;
        after = page.next;
        for record in page.records {
            if initial {
                collected.push_back(record);
                if collected.len() > count {
                    collected.pop_front();
                }
            } else {
                print_record(&record);
            }
        }
        if advanced && (!initial || after < through.unwrap_or(0)) {
            continue;
        }
        if initial {
            for record in collected.drain(..) {
                print_record(&record);
            }
            initial = false;
            through = None;
        }
        if !follow {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    Ok(())
}
fn print_record(record: &hyper_journal::Record) {
    // Log bytes are data, never terminal control sequences.
    let text: String = record
        .text
        .chars()
        .flat_map(|c| {
            if c.is_control() && c != '\t' {
                c.escape_default().collect::<Vec<_>>()
            } else {
                vec![c]
            }
        })
        .collect();
    println!(
        "[{:>6}.{:03}] {}#{} {:?}: {}{}",
        record.monotonic_ns / 1_000_000_000,
        (record.monotonic_ns / 1_000_000) % 1000,
        record.source.component,
        record.source.generation,
        record.source.stream,
        text,
        if record.continued { " [partial]" } else { "" }
    );
}
fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("journalctl: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
