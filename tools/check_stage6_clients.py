#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Real server/client acceptance in isolated temporary directories. Not a benchmark."""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import selectors
import shutil
import subprocess
import tempfile
import time


def run(args, **kwargs):
    result = subprocess.run(args, capture_output=True, text=True, timeout=30, **kwargs)
    if result.returncode:
        raise RuntimeError(f"{Path(args[0]).name} failed: {result.stderr[-2000:]}")
    return result.stdout


def certificate(root):
    run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(root / "ca.key"), "-out", str(root / "ca.crt"), "-days", "1", "-subj", "/CN=Kedr acceptance CA"])
    run(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", str(root / "server.key"), "-out", str(root / "server.csr"), "-subj", "/CN=localhost"])
    (root / "server.ext").write_text("basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\n")
    run(["openssl", "x509", "-req", "-in", str(root / "server.csr"), "-CA", str(root / "ca.crt"), "-CAkey", str(root / "ca.key"), "-CAcreateserial", "-out", str(root / "server.crt"), "-days", "1", "-extfile", str(root / "server.ext")])
    for key in (root / "ca.key", root / "server.key"):
        key.chmod(0o600)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", type=Path, default=Path("target/debug"))
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--studio", action="store_true")
    args = parser.parse_args()
    binaries = args.bin_dir.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    result = {"server": "KedrDB 0.1.0", "checks": []}
    with tempfile.TemporaryDirectory(prefix="kedr-stage6-") as directory:
        root = Path(directory)
        password = secrets.token_urlsafe(32)
        run([str(binaries / "kedr"), "create-db", str(root / "data")])
        run([str(binaries / "kedr-server"), "init-auth", str(root / "auth"), "admin"], input=password + "\n")
        auth = json.loads((root / "auth").read_text())
        assert password not in (root / "auth").read_text()
        assert set(auth) == {"version", "username", "iterations", "salt", "stored_key", "server_key"}
        assert (root / "auth").stat().st_mode & 0o077 == 0
        certificate(root)
        server = subprocess.Popen([str(binaries / "kedr-server"), "serve", "--data", str(root / "data"), "--auth", str(root / "auth"), "--listen", "127.0.0.1:0", "--tls-cert", str(root / "server.crt"), "--tls-key", str(root / "server.key")], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(server.stdout, selectors.EVENT_READ)
                if not selector.select(10):
                    raise RuntimeError("server startup timed out")
                line = server.stdout.readline()
            match = re.search(r"127\.0\.0\.1:(\d+)", line)
            if not match:
                raise RuntimeError("server failed to announce listener")
            port = int(match.group(1))
            pgpass = root / "pgpass"
            pgpass.write_text(f"127.0.0.1:{port}:kedrdb:admin:{password}\nlocalhost:{port}:kedrdb:admin:{password}\n")
            pgpass.chmod(0o600)
            environment = dict(os.environ, PGPASSFILE=str(pgpass), PGSSLMODE="verify-full", PGSSLROOTCERT=str(root / "ca.crt"), PGCONNECT_TIMEOUT="5")
            command = [args.psql, "-X", "-w", "-h", "127.0.0.1", "-p", str(port), "-U", "admin", "-d", "kedrdb", "-v", "ON_ERROR_STOP=1", "-At"]
            sql = """CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT NOT NULL, stock INT, active BOOLEAN);
INSERT INTO products (id,name,stock,active) VALUES (1,'Кедровый блокнот',10,true),(2,'Лампа для стола',13,true),(3,'Кружка',16,false),(4,'Подставка для ноутбука',19,NULL);
BEGIN;
UPDATE products SET stock=stock-1 WHERE id=1;
ROLLBACK;
SELECT id,stock FROM products ORDER BY id;
SHOW COLUMNS FROM products;
"""
            output = run(command, input=sql, env=environment)
            assert "1|10" in output and "4|19" in output and "id|BIGINT" in output
            result["psql"] = run([args.psql, "--version"]).strip()
            result["checks"].extend(["SCRAM verifier permissions", "psql verified TLS CRUD/transaction/NULL/catalog"])
            wrong_root = dict(environment, PGSSLROOTCERT=str(root / "server.crt"))
            rejected = subprocess.run(command, input="SELECT 1;", env=wrong_root, capture_output=True, text=True, timeout=10)
            assert rejected.returncode != 0
            result["checks"].append("psql rejects untrusted CA")
            wrong_name = dict(environment, PGHOSTADDR="127.0.0.1")
            rejected = subprocess.run(command + ["-h", "wrong.invalid"], input="SELECT 1;", env=wrong_name, capture_output=True, text=True, timeout=10)
            assert rejected.returncode != 0 and "certificate" in rejected.stderr.lower()
            result["checks"].append("psql rejects certificate hostname mismatch")
            # The listener guard is checked before opening the data directory.
            unsafe = subprocess.run([str(binaries / "kedr-server"), "serve", "--data", str(root / "data"), "--auth", str(root / "auth"), "--listen", "0.0.0.0:0"], capture_output=True, text=True, timeout=10)
            assert unsafe.returncode != 0 and "requires TLS" in unsafe.stderr
            result["checks"].append("remote listener requires TLS")
            try:
                import psycopg
            except ImportError:
                pass
            else:
                result["psycopg"] = psycopg.__version__
                with psycopg.connect(host="127.0.0.1", port=port, user="admin", password=password, dbname="kedrdb", sslmode="verify-full", sslrootcert=str(root / "ca.crt"), autocommit=True, connect_timeout=5) as connection:
                    with connection.cursor(binary=True) as cursor:
                        cursor.execute("INSERT INTO products (id,name,stock,active) VALUES (%s,%s,%s,%s)", (5, "Драйвер", 20, None))
                        cursor.execute("SELECT id,name,stock,active FROM products WHERE id=%s", (5,))
                        assert cursor.fetchone() == (5, "Драйвер", 20, None)
                        cursor.execute("UPDATE products SET stock=%s WHERE id=%s", (21, 5))
                        assert cursor.rowcount == 1
                        cursor.execute("DELETE FROM products WHERE id=%s", (5,))
                        assert cursor.rowcount == 1
                result["checks"].append("psycopg verified TLS binary parameters/CRUD/NULL")
            if args.studio:
                screenshot = args.output.resolve() / "studio-real.png"
                gui_environment = dict(os.environ, KEDR_STUDIO_SCREENSHOT=str(screenshot))
                gui = [str(binaries / "kedr-studio"), "--connect-test", str(port), "--test-ca", str(root / "ca.crt")]
                if os.environ.get("DISPLAY") is None and shutil.which("xvfb-run"):
                    gui = ["xvfb-run", "-a"] + gui
                    gui_environment["LIBGL_ALWAYS_SOFTWARE"] = "1"
                gui_output = run(gui, input=password + "\n", env=gui_environment)
                assert "KEDR_STUDIO_TEST_RESULT connected=true rows=4 tables=1 error=false" in gui_output
                assert screenshot.is_file() and screenshot.stat().st_size > 10000
                try:
                    from PIL import Image
                except ImportError:
                    pass
                else:
                    with Image.open(screenshot) as image:
                        assert any(high - low > 100 for low, high in image.convert("RGB").getextrema()), "blank native frame"
                result["checks"].append("native Studio authenticated TLS catalog/query screenshot")
                # The default GUI starts its own authenticated local server, with persistent data.
                local_root = root / "local-managed"
                identity = None
                for attempt, tables in ((1, 0), (2, 1)):
                    local_image = args.output.resolve() / f"studio-auto-{attempt}.png"
                    local_env = dict(os.environ, KEDR_STUDIO_SCREENSHOT=str(local_image))
                    local_gui = [str(binaries / "kedr-studio"), "--local-data", str(local_root)]
                    if os.environ.get("DISPLAY") is None and shutil.which("xvfb-run"):
                        local_gui = ["xvfb-run", "-a"] + local_gui
                        local_env["LIBGL_ALWAYS_SOFTWARE"] = "1"
                    local_output = run(local_gui, env=local_env)
                    assert f"KEDR_STUDIO_LOCAL_RESULT connected=true tables={tables} error=false" in local_output
                    assert local_image.is_file() and local_image.stat().st_size > 10000
                    current = (local_root / "data" / ".identity").read_bytes()
                    if identity is None:
                        identity = current
                        run([str(binaries / "kedr"), "sql", str(local_root / "data"), "CREATE TABLE persisted (id BIGINT PRIMARY KEY, name TEXT)"])
                        run([str(binaries / "kedr"), "sql", str(local_root / "data"), "INSERT INTO persisted (id,name) VALUES (1,'сохранено')"])
                    else:
                        assert current == identity
                        output = run([str(binaries / "kedr"), "sql", str(local_root / "data"), "SELECT name FROM persisted"])
                        assert "сохранено" in output
                    assert {p.name for p in local_root.iterdir()} == {"data", ".studio.lock"}
                result["checks"].append("native Studio auto-start/authentication, restart preserves identity/catalog/acknowledged row and releases ownership")
        finally:
            server.terminate()
            try:
                stdout, stderr = server.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.communicate()
                raise RuntimeError("server did not stop after SIGTERM")
            assert password not in stdout + stderr
            if server.returncode != 0:
                raise RuntimeError(f"server exit {server.returncode}: {stderr[-2000:]}")
        output = run([str(binaries / "kedr"), "sql", str(root / "data"), "SELECT COUNT(*) FROM products"])
        assert '4' in output
        result["checks"].append("SIGTERM releases lock and durable rows reopen")
    (args.output / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
