#!/usr/bin/env python3
"""Run a preliminary PostgreSQL baseline in a private temporary cluster.

This is a development harness, not the database engine. It never connects to an
existing server. PostgreSQL subprocesses are invoked without a shell, TCP is
disabled, and all databases belong to a newly initialized temporary cluster.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "benchmarks" / "stage0"


def positive(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-bin", type=Path)
    parser.add_argument("--products", type=positive, default=100000)
    parser.add_argument("--users", type=positive, default=10000)
    parser.add_argument("--payload-bytes", type=positive, default=128)
    parser.add_argument("--seconds", type=positive, default=5)
    parser.add_argument("--repeats", type=positive, default=3)
    parser.add_argument("--clients", default="1,8")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "stage0-baseline")
    args = parser.parse_args()
    clients = [positive(item) for item in args.clients.split(",")]
    found = shutil.which("pgbench")
    pg_bin = args.pg_bin or (Path(found).resolve().parent if found else None)
    required = ("initdb", "pg_ctl", "createdb", "psql", "pgbench")
    if pg_bin is None or any(not (pg_bin / name).is_file() for name in required):
        parser.error("specify --pg-bin with PostgreSQL server and client binaries")
    output = args.output.resolve()
    # A new output directory prevents silently overwriting previous evidence.
    output.mkdir(parents=True, exist_ok=False)
    env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
    env.update({"LC_ALL": "C", "PGCONNECT_TIMEOUT": "5", "PGAPPNAME": "stage0-baseline"})

    def run(name, arguments, *, input_text=None, timeout=120):
        completed = subprocess.run(
            [str(pg_bin / name), *map(str, arguments)],
            input=input_text, text=True, capture_output=True, env=env, timeout=timeout,
        )
        if completed.returncode:
            raise RuntimeError(f"{name} failed ({completed.returncode}): {completed.stderr or completed.stdout}")
        return completed.stdout + (completed.stderr if name == "pgbench" else "")

    summary = {
        "kind": "preliminary-development-baseline-not-a-competitive-ranking",
        "status": "running",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "postgresql_client_version": run("psql", ["--version"]).strip(),
        "configuration": {"products": args.products, "users": args.users, "payload_bytes": args.payload_bytes,
                          "seconds": args.seconds, "repeats": args.repeats,
                          "clients": clients, "random_seed": 20261009, "max_tries": 5},
        "source_checksums": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                             for path in [Path(__file__).resolve(), *sorted(FIXTURES.glob('*'))] if path.is_file()},
        "limitations": ["short closed-loop runs", "macOS development environment",
                        "existing installed PostgreSQL version", "not a tuned comparison",
                        "p99 and open-loop saturation not measured", "other local processes are running"],
        "runs": [],
    }
    started = False
    with tempfile.TemporaryDirectory(prefix="db-stage0-") as temporary:
        base = Path(temporary)
        cluster = base / "cluster"
        socket = base / "socket"
        socket.mkdir(mode=0o700)
        server_log = output / "server.log"
        try:
            run("initdb", ["-D", cluster, "-U", "stage0", "-A", "trust", "--no-locale", "--encoding=UTF8"])
            with (cluster / "postgresql.conf").open("a") as handle:
                handle.write(
                    f"\nlisten_addresses = ''\nunix_socket_directories = '{socket}'\n"
                    "port = 56197\nmax_connections = 64\nshared_buffers = '256MB'\n"
                    "fsync = on\nsynchronous_commit = on\nfull_page_writes = on\n"
                    "default_transaction_isolation = 'serializable'\n"
                )
            run("pg_ctl", ["-D", cluster, "-l", server_log, "-w", "start"], timeout=60)
            started = True
            connection = ["-h", socket, "-p", "56197", "-U", "stage0"]

            def sql(database, statement):
                return run("psql", [*connection, "-d", database, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", statement]).strip()

            summary["server_version"] = sql("postgres", "SHOW server_version")
            summary["settings"] = json.loads(sql("postgres", """
                SELECT json_object_agg(name, setting) FROM pg_settings
                WHERE name IN ('server_version','shared_buffers','fsync','synchronous_commit',
                  'full_page_writes','default_transaction_isolation','checkpoint_timeout',
                  'wal_sync_method','work_mem','max_connections','listen_addresses');
            """))
            for client_count in clients:
                for repeat in range(1, args.repeats + 1):
                    database = f"baseline_c{client_count}_r{repeat}"
                    label = f"c{client_count}-r{repeat}"
                    print(f"Preparing {label} in a new temporary database", flush=True)
                    run("createdb", [*connection, database])
                    fixture_args = [*connection, "-d", database, "-X", "-v", "ON_ERROR_STOP=1",
                                    "-v", f"product_count={args.products}", "-v", f"user_count={args.users}",
                                    "-v", f"payload_bytes={args.payload_bytes}",
                                    "-f", FIXTURES / "schema.sql"]
                    (output / f"{label}-setup.txt").write_text(run("psql", fixture_args))
                    benchmark_args = [*connection, "-n", "-M", "prepared", "-c", client_count,
                                      "-j", min(client_count, 4), "--random-seed=20261009",
                                      "--max-tries=5", "--failures-detailed",
                                      "-D", f"product_count={args.products}", "-D", f"user_count={args.users}", "-D", "seq=0"]
                    run("pgbench", [*benchmark_args, "-T", "1", "-f", FIXTURES / "read.pgbench", database])
                    sql(database, "CHECKPOINT")
                    print(f"Measuring {label}: {args.seconds}s, 95% read / 5% stock-write transactions", flush=True)
                    text = run("pgbench", [*benchmark_args, "-T", args.seconds, "-r", "-f", FIXTURES / "mixed.pgbench", database], timeout=args.seconds + 60)
                    (output / f"{label}-pgbench.txt").write_text(text)
                    match = re.search(r"^tps = ([\d.]+)", text, re.MULTILINE)
                    average = re.search(r"^latency average = ([\d.]+) ms", text, re.MULTILINE)
                    failures = re.search(r"^number of failed transactions: (\d+)", text, re.MULTILINE)
                    retries = re.search(r"^total number of retries: (\d+)", text, re.MULTILINE)
                    if match is None or average is None or failures is None:
                        raise RuntimeError("Could not parse required pgbench results")
                    checks = json.loads(sql(database, """
                        SELECT json_build_object(
                          'products', (SELECT COUNT(*) FROM products),
                          'users', (SELECT COUNT(*) FROM users),
                          'events', (SELECT COUNT(*) FROM inventory_events),
                          'invalid_stock', (SELECT COUNT(*) FROM products WHERE stock < 0 OR stock > 1000),
                          'invalid_event_stock', (SELECT COUNT(*) FROM inventory_events WHERE new_stock < 0 OR new_stock > 1000),
                          'orphaned_events', (SELECT COUNT(*) FROM inventory_events e
                             LEFT JOIN products p ON p.id=e.product_id LEFT JOIN users u ON u.id=e.actor_id
                             WHERE p.id IS NULL OR u.id IS NULL),
                          'database_bytes', pg_database_size(current_database()));
                    """))
                    if checks["products"] != args.products or checks["users"] != args.users or any(checks[key] for key in ("invalid_stock", "invalid_event_stock", "orphaned_events")):
                        raise RuntimeError(f"Invariant failure: {checks}")
                    result = {"clients": client_count, "repeat": repeat, "tps": float(match[1]),
                              "latency_average_ms": float(average[1]), "failed_transactions": int(failures[1]),
                              "total_retries": int(retries[1]) if retries else 0,
                              "checks": checks}
                    summary["runs"].append(result)
                    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
                    print(json.dumps(result, ensure_ascii=False), flush=True)
                    if result["failed_transactions"]:
                        raise RuntimeError("Failed transactions: this run cannot be counted as a successful baseline")
        except Exception as error:
            summary["status"] = "failed"
            summary["error"] = str(error)
            raise
        finally:
            if started or (cluster / "postmaster.pid").exists():
                # If stopping fails, preserve the cluster rather than remove a running server's files.
                try:
                    run("pg_ctl", ["-D", cluster, "-w", "-m", "fast", "stop"], timeout=60)
                    summary["temporary_cluster_stopped_and_removed"] = True
                except Exception:
                    preserved = output / "cluster-requiring-cleanup"
                    shutil.move(str(base), preserved)
                    summary["temporary_cluster_stopped_and_removed"] = False
                    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
                    raise RuntimeError(f"Could not stop temporary PostgreSQL; files preserved at {preserved}")
            (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    summary["status"] = "completed"
    summary["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    summary["temporary_cluster_stopped_and_removed"] = True
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(f"Evidence: {output / 'summary.json'}", flush=True)


if __name__ == "__main__":
    main()
