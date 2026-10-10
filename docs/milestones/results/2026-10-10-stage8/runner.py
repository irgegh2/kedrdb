#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Development OLTP diagnostic, not SMALL_OLTP_V1 acceptance or a market ranking.

Creates only a private synthetic PostgreSQL cluster and fresh KedrDB copies.
Both servers use reused TCP connections, SCRAM and durable serializable writes.
All trial orders, raw latencies, errors, retries and acknowledged audit rows survive.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import secrets
import selectors
import socket
import subprocess
import tempfile
import threading
import time

import psycopg
import migrate_postgres as migration

ROOT = Path(__file__).resolve().parents[1]
READ = "SELECT id,name,price_cents,stock,active FROM products WHERE id=%s"
UPDATE = "UPDATE products SET stock=%s WHERE id=%s"
EVENT = "INSERT INTO inventory_events (id,product_id,actor_id,new_stock) VALUES (%s,%s,%s,%s)"
SCHEMA = [
    "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL UNIQUE, name TEXT NOT NULL, active BOOLEAN NOT NULL)",
    "CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT NOT NULL, category_id BIGINT NOT NULL, price_cents BIGINT NOT NULL, stock INT NOT NULL, active BOOLEAN NOT NULL, description TEXT)",
    "CREATE INDEX products_category ON products(category_id)",
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), status TEXT NOT NULL, total_cents BIGINT NOT NULL)",
    "CREATE INDEX orders_user ON orders(user_id)",
    "CREATE TABLE order_items (id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES orders(id), product_id BIGINT NOT NULL REFERENCES products(id), quantity INT NOT NULL, price_cents BIGINT NOT NULL)",
    "CREATE INDEX order_items_order ON order_items(order_id)",
    "CREATE TABLE inventory_events (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL REFERENCES products(id), actor_id BIGINT NOT NULL REFERENCES users(id), new_stock INT NOT NULL)",
]


def run(command, **kw):
    p = subprocess.run(list(map(str, command)), capture_output=True, text=True, timeout=120, **kw)
    if p.returncode:
        raise RuntimeError(f"{Path(str(command[0])).name} failed: {p.stderr[-1500:]}")
    return p.stdout


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * p / 100) - 1)] / 1000


def rows(connection, table):
    """Pagination respects KedrDB's result budget; every value is consumed."""
    key = -1
    while True:
        page = connection.execute(f'SELECT * FROM "{table}" WHERE id>%s ORDER BY id LIMIT 256', (key,)).fetchall()
        if not page:
            return
        yield from page
        key = page[-1][0]


def product_read(cursor, key):
    cursor.execute(READ, (key,))
    got = cursor.fetchall()
    assert len(got) == 1
    row = got[0]
    assert (row[0], row[1], row[2], row[4]) == (key, f"Товар {key}", 100 + key % 100000, True)
    assert type(row[3]) is int and 0 <= row[3] <= 1000


def verify(connection, args, expected):
    # Static tables and all acknowledged audit values are checked, not just counts.
    assert list(rows(connection, "users")) == [(i, f"user{i}@example.test", f"Пользователь {i}", True) for i in range(1, args.users + 1)]
    assert list(rows(connection, "orders")) == [(i, i, "created", 2 * (100 + i % 100000)) for i in range(1, args.users + 1)]
    assert list(rows(connection, "order_items")) == [(i, i, i, 2, 100 + i % 100000) for i in range(1, args.users + 1)]
    seen = 0
    writes = {}
    for event in expected:
        writes.setdefault(event[1], set()).add(event[3])
    for row in rows(connection, "products"):
        i = seen + 1
        assert row[:4] == (i, f"Товар {i}", 1 + i % 100, 100 + i % 100000)
        assert row[5:] == (True, "x" * args.payload_bytes)
        assert row[4] in writes.get(i, {1000})
        seen += 1
    assert seen == args.products
    assert list(rows(connection, "inventory_events")) == sorted(expected)
    return {"static_tables_all_values": True, "audit_all_acknowledged_values": True,
            "products_all_static_values_and_committed_stock_domain": True,
            "audit_rows": len(expected),
            "limitation": "Final stock is checked against committed values, not reconstructed global commit order; concurrency/atomicity have separate correctness tests."}


def fixture(connection, args):
    for sql in SCHEMA:
        connection.execute(sql)
    generators = {
        "users": ((i, f"user{i}@example.test", f"Пользователь {i}", True) for i in range(1, args.users + 1)),
        "products": ((i, f"Товар {i}", 1 + i % 100, 100 + i % 100000, 1000, True, "x" * args.payload_bytes) for i in range(1, args.products + 1)),
        "orders": ((i, i, "created", 2 * (100 + i % 100000)) for i in range(1, args.users + 1)),
        "order_items": ((i, i, i, 2, 100 + i % 100000) for i in range(1, args.users + 1)),
    }
    for table, data in generators.items():
        with connection.cursor().copy(f"COPY {table} FROM STDIN") as copy:
            for row in data:
                copy.write_row(row)
    connection.execute("ANALYZE")
    connection.execute("CHECKPOINT")


def cpu_memory(pid):
    # Only numeric data for this server and its descendants is retained.
    parsed = {}
    for line in run(["/bin/ps", "-axo", "pid=,ppid=,rss=,time="]).splitlines():
        parts = line.split()
        if len(parts) != 4:
            continue
        ident, parent, rss = map(int, parts[:3])
        days, separator, clock = parts[3].rpartition("-")
        duration = (clock if separator else parts[3]).split(":")
        seconds = float(duration[-1]) + 60 * int(duration[-2])
        if len(duration) == 3:
            seconds += 3600 * int(duration[0])
        if separator:
            seconds += 86400 * int(days)
        parsed[ident] = (parent, rss, seconds)
    selected = {pid}
    while True:
        expanded = selected | {p for p, (parent, _, _) in parsed.items() if parent in selected}
        if expanded == selected:
            break
        selected = expanded
    return {str(p): {"rss_kib": parsed[p][1], "cpu_s": parsed[p][2]} for p in selected if p in parsed}


def trial(parameters, clients, repeat, args, server_pid, path):
    samples, events, failures = [], [], []
    gate = threading.Event()
    state = {}
    with ExitStack() as stack:
        connections = [stack.enter_context(psycopg.connect(**parameters, autocommit=True, prepare_threshold=0)) for _ in range(clients)]
        for c in connections:
            with c.cursor(binary=True) as cur:
                for i in range(10):
                    product_read(cur, 1 + i * (args.products - 1) // 10)
        before = cpu_memory(server_pid)
        cpu_before = time.process_time()

        def worker(client, connection):
            rng = random.Random(args.seed + repeat * 100000 + client)
            own, audit = [], []
            gate.wait()
            seq = 0
            with connection.cursor(binary=True) as cur:
                while time.perf_counter() < state["deadline"]:
                    seq += 1
                    key = rng.randint(1, args.products)
                    writing = seq % 20 == 0
                    actor, stock = rng.randint(1, args.users), rng.randint(0, 1000)
                    event = ((client + 1) * 1000000000000 + seq, key, actor, stock)
                    began = time.perf_counter_ns()
                    attempt, error = 0, None
                    success = False
                    for attempt in range(1, 6):
                        try:
                            if writing:
                                cur.execute("BEGIN")
                                cur.execute(UPDATE, (stock, key))
                                assert cur.rowcount == 1
                                cur.execute(EVENT, event)
                                assert cur.rowcount == 1
                                cur.execute("COMMIT")
                                audit.append(event)
                            else:
                                product_read(cur, key)
                            success = True
                            break
                        except Exception as e:
                            error = getattr(e, "sqlstate", None) or type(e).__name__
                            try:
                                cur.execute("ROLLBACK")
                            except Exception:
                                pass
                            if error not in ("40001", "40P01"):
                                break
                    own.append({"client": client, "sequence": seq, "write": writing,
                                "latency_us": (time.perf_counter_ns() - began) / 1000,
                                "success": success, "attempts": attempt,
                                "last_retry_state": error})
            return own, audit, time.perf_counter()

        with ThreadPoolExecutor(max_workers=clients) as pool:
            futures = [pool.submit(worker, i, c) for i, c in enumerate(connections)]
            began = time.perf_counter()
            state["deadline"] = began + args.seconds
            gate.set()
            finished = began
            for f in futures:
                own, audit, ended = f.result(timeout=args.seconds + 60)
                samples.extend(own)
                events.extend(audit)
                finished = max(finished, ended)
        client_cpu = time.process_time() - cpu_before
        after = cpu_memory(server_pid)  # client connections remain alive for PG backend accounting
        resources = {"server_processes_before": before, "server_processes_after": after,
                     "client_cpu_s": client_cpu,
                     "note": "RSS is per-process; summing PostgreSQL RSS double-counts shared memory. This is not PSS or a hard memory budget."}
    with psycopg.connect(**parameters, autocommit=True) as c:
        try:
            validation = verify(c, args, events)
        except Exception as e:
            validation = {"passed": False, "failure": type(e).__name__}
            failures.append("final_data_validation")
        else:
            validation["passed"] = True
        if parameters["dbname"] == "kedrdb":
            resources["kedr_metrics"] = json.loads(c.execute("SHOW KEDR METRICS").fetchone()[0])
        else:
            resources["pg_database_bytes"] = c.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    with path.open("w") as f:
        for sample in samples:
            f.write(json.dumps(sample, separators=(",", ":")) + "\n")
    errors = sum(not s["success"] for s in samples)
    latency = [s["latency_us"] for s in samples]
    elapsed = finished - began
    return {"clients": clients, "repeat": repeat, "transactions": len(samples), "errors": errors,
            "successful_tps": (len(samples) - errors) / elapsed, "elapsed_s": elapsed,
            "p50_ms": percentile(latency, 50), "p95_ms": percentile(latency, 95), "p99_ms": percentile(latency, 99),
            "retries": sum(s["attempts"] - 1 for s in samples), "writes": len(events),
            "raw_samples": path.name, "validation": validation, "resources": resources,
            "valid": errors == 0 and not failures, "meets_20ms_in_this_closed_loop_trial": errors == 0 and not failures and percentile(latency, 99) <= 20}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--pg-bin", type=Path, required=True)
    p.add_argument("--bin-dir", type=Path, default=ROOT / "target/release")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--products", type=int, default=1000)
    p.add_argument("--users", type=int, default=100)
    p.add_argument("--payload-bytes", type=int, default=128)
    p.add_argument("--seconds", type=int, default=3)
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--clients", default="1,8,32")
    p.add_argument("--seed", type=int, default=20261010)
    args = p.parse_args()
    clients = list(map(int, args.clients.split(",")))
    if not (0 < args.users <= args.products and 0 < args.payload_bytes <= 4096 and args.seconds > 0 and args.repeats > 0 and clients and all(0 < c <= 32 for c in clients)):
        p.error("invalid dataset/duration/client limits")
    args.output = args.output.resolve(); args.output.mkdir(parents=True, exist_ok=False)
    args.pg_bin = args.pg_bin.resolve(); args.bin_dir = args.bin_dir.resolve()
    kedr, server = args.bin_dir / "kedr", args.bin_dir / "kedr-server"
    summary = {"kind": "development-diagnostic-not-a-competitive-ranking", "started_utc": datetime.now(timezone.utc).isoformat(),
               "status": "running", "code_commit": run(["git", "rev-parse", "HEAD"]).strip(), "platform": platform.platform(),
               "psycopg": psycopg.__version__, "postgres": run([args.pg_bin / "postgres", "--version"]).strip(),
               "configuration": {k: v for k, v in vars(args).items() if k not in ("pg_bin", "bin_dir", "output")},
               "binary_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (kedr, server, args.pg_bin / "postgres")},
               "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (Path(__file__).resolve(), ROOT / "tools/migrate_postgres.py")},
               "limitations": ["macOS development environment, other applications may run", "closed-loop, not saturation/open-loop acceptance",
                               "small dataset; no 512 MiB/4 CPU hard isolation", "warm process reads; OS cache is not flushed", "short runs cannot establish background-maintenance behavior",
                               "only PostgreSQL, no MySQL/MariaDB yet", "client Python/GIL may cap throughput; client CPU is recorded", "no cold cache or larger-than-RAM test", "private PostgreSQL instance remains idle during Kedr trials"], "runs": []}
    summary["hardware"] = {key: run(["/usr/sbin/sysctl", "-n", key]).strip() for key in ("hw.memsize", "hw.ncpu", "machdep.cpu.brand_string")} if platform.system() == "Darwin" else {"cpu_count": os.cpu_count()}
    summary["postgres_build"] = run([args.pg_bin / "pg_config", "--configure"]).strip()
    def save():
        (args.output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    save()
    old_environment = dict(os.environ)
    with tempfile.TemporaryDirectory(prefix="kedr-stage8-") as directory:
        root = Path(directory); cluster = root / "pg"; sock = root / "socket"; sock.mkdir(mode=0o700)
        password = secrets.token_urlsafe(32)
        pw = root / "password"; pw.write_text(password + "\n"); pw.chmod(0o600)
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]
        run([args.pg_bin / "initdb", "-D", cluster, "-U", "benchmark", "-A", "scram-sha-256", "--pwfile", pw, "--no-locale", "--encoding=UTF8"])
        with (cluster / "postgresql.conf").open("a") as f:
            f.write(f"\nlisten_addresses='127.0.0.1'\nport={port}\nunix_socket_directories='{sock}'\nshared_buffers='256MB'\nfsync=on\nsynchronous_commit=on\nfull_page_writes=on\ndefault_transaction_isolation='serializable'\nstatement_timeout='5s'\n")
        started = False
        try:
            run([args.pg_bin / "pg_ctl", "-D", cluster, "-l", args.output / "postgres.log", "-w", "start"]); started = True
            pg_pid = int((cluster / "postmaster.pid").read_text().splitlines()[0])
            params = dict(host="127.0.0.1", port=port, user="benchmark", password=password, dbname="postgres", sslmode="disable", connect_timeout=5)
            with psycopg.connect(**params, autocommit=True) as c:
                c.execute("CREATE DATABASE seed")
            params["dbname"] = "seed"
            with psycopg.connect(**params, autocommit=True) as c:
                fixture(c, args); verify(c, args, [])
                summary["pg_settings"] = {key: c.execute("SHOW " + key).fetchone()[0] for key in ("server_version", "fsync", "synchronous_commit", "full_page_writes", "default_transaction_isolation", "shared_buffers", "wal_sync_method")}
                assert all(summary["pg_settings"][k] == "on" for k in ("fsync", "synchronous_commit", "full_page_writes"))
            for key in list(os.environ):
                if key.startswith("PG"): del os.environ[key]
            os.environ.update(PGHOST=str(sock), PGPORT=str(port), PGUSER="benchmark", PGPASSWORD=password, PGDATABASE="seed")
            job = root / "seed-job"; migration.snapshot(job, True); migration.import_snapshot(job, kedr)
            backup = root / "seed-backup"; run([kedr, "backup", job / "database", backup])
            summary["seed_sql_sha256"] = hashlib.sha256((job / "snapshot.sql").read_bytes()).hexdigest()
            auth = root / "auth"; run([server, "init-auth", auth, "admin"], input=password + "\n")
            schedule = [(engine, count, repeat) for repeat in range(1, args.repeats + 1) for count in clients for engine in ("postgres", "kedr")]
            random.Random(args.seed).shuffle(schedule); summary["trial_order"] = schedule; save()
            for engine, count, repeat in schedule:
                label = f"{engine}-c{count}-r{repeat}"; print("Measuring " + label, flush=True)
                if engine == "postgres":
                    database = f"trial_c{count}_r{repeat}"
                    control = dict(params, dbname="postgres")
                    with psycopg.connect(**control, autocommit=True) as c:
                        c.execute("CREATE DATABASE " + database + " TEMPLATE seed")
                    trial_params = dict(params, dbname=database)
                    result = trial(trial_params, count, repeat, args, pg_pid, args.output / (label + ".jsonl"))
                else:
                    data = root / label; run([kedr, "restore", backup, data])
                    with (args.output / (label + "-server.log")).open("w") as log:
                        proc = subprocess.Popen([str(server), "serve", "--data", str(data), "--auth", str(auth), "--listen", "127.0.0.1:0"], stdout=subprocess.PIPE, stderr=log, text=True)
                        try:
                            with selectors.DefaultSelector() as selector:
                                selector.register(proc.stdout, selectors.EVENT_READ)
                                if not selector.select(10):
                                    raise RuntimeError("Kedr startup timed out")
                                line = proc.stdout.readline()
                            match = re.search(r"127\.0\.0\.1:(\d+)", line)
                            if not match: raise RuntimeError("Kedr startup failed")
                            trial_params = dict(host="127.0.0.1", port=int(match.group(1)), user="admin", password=password, dbname="kedrdb", sslmode="disable", connect_timeout=5)
                            result = trial(trial_params, count, repeat, args, proc.pid, args.output / (label + ".jsonl"))
                        finally:
                            proc.terminate()
                            try:
                                proc.communicate(timeout=15)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.communicate(timeout=5)
                        assert proc.returncode == 0
                    result["integrity"] = run([kedr, "integrity-check", data]).strip()
                result["engine"] = engine; result["label"] = label; summary["runs"].append(result); save()
                print(json.dumps({k: result[k] for k in ("label", "successful_tps", "p99_ms", "errors", "valid")}), flush=True)
            summary["status"] = "completed"; save()
        except Exception as e:
            summary["status"] = "failed"; summary["failure_type"] = type(e).__name__; save(); raise
        finally:
            if started: run([args.pg_bin / "pg_ctl", "-D", cluster, "-m", "immediate", "-w", "stop"])
            os.environ.clear(); os.environ.update(old_environment)


if __name__ == "__main__":
    main()
