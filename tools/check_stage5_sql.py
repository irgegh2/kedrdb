#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Compare Kedr SQL to PostgreSQL in a new private temporary cluster.

Requires psycopg 3 and PostgreSQL binaries. Existing servers are never used.
This checks correctness, not performance or PostgreSQL wire compatibility.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import selectors
import subprocess
import tempfile
import time

import psycopg

ROOT = Path(__file__).resolve().parents[1]
OIDS = {23: "int4", 20: "int8", 25: "text", 16: "bool"}


class Kedr:
    def __init__(self, binary, database, log):
        self.binary, self.database, self.log = binary, database, log
        self.process = None
        self.start()

    def start(self):
        self.process = subprocess.Popen([str(self.binary), "sql-stream", str(self.database)],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.log, text=True, bufsize=1)

    def query(self, sql):
        self.process.stdin.write(sql + "\n")
        self.process.stdin.flush()
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            if not selector.select(40):
                raise RuntimeError("Kedr statement timed out: " + sql)
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("Kedr exited during: " + sql)
        return json.loads(line)

    def stop(self, kill=False):
        if not self.process:
            return
        if kill:
            self.process.kill()
        elif self.process.poll() is None:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
            raise
        self.process.stdout.close()
        self.process = None


def pg_query(connection, sql):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, prepare=False)
            if cursor.description:
                columns = [{"name": c.name, "type": OIDS.get(c.type_code, str(c.type_code))}
                           for c in cursor.description]
                rows = [list(r) for r in cursor.fetchall()]
                return {"columns": columns, "rows": rows, "affected": len(rows)}
            return {"columns": [], "rows": [], "affected": max(0, cursor.rowcount)}
    except psycopg.Error as error:
        return {"error": error.sqlstate}


def comparable(result):
    if "error" in result:
        return {"error": result["error"]}
    return {k: result[k] for k in ("columns", "rows", "affected")}


def manual_queries():
    # Fixed semantic templates parameterized by boundaries and NULLs. These are
    # independent cases, not 1,000 copies of a single happy-path statement.
    values = ["NULL", "-9223372036854775808", "-2147483649", "-2147483648", "-101", "-99", "-31", "-17", "-7", "-2", "-1", "0", "1", "2", "7", "10", "17", "31",
              "99", "100", "1000", "2147483647", "2147483648", "9223372036854775807"]
    for column in ["id", "n"]:
        for operator in ["=", "<>", "<", "<=", ">", ">="]:
            for value in values:
                for suffix in ["", " AND b=true", " AND s IS NOT NULL"]:
                    yield f"SELECT id,n,s,b FROM data WHERE {column}{operator}{value}{suffix} ORDER BY id"
    for column in ["id", "n", "s", "b"]:
        for direction in ["ASC", "DESC"]:
            for nulls in ["FIRST", "LAST"]:
                for limit in [0, 1, 3, 20, 100]:
                    # id tie-break is outside the dialect, so rows with tied
                    # values are compared as an unordered multiset below.
                    yield f"SELECT id,n,s,b FROM data ORDER BY {column} {direction} NULLS {nulls} LIMIT {limit}"
    for text in ["''", "'a'", "'z'", "'Кедр'", "'quote''slash\\'", "NULL"]:
        for operator in ["=", "<>", "<", "<=", ">", ">="]:
            for projection in ["id", "id,s", "COUNT(*)"]:
                suffix = "" if projection == "COUNT(*)" else " ORDER BY id"
                yield f"SELECT {projection} FROM data WHERE s{operator}{text}{suffix}"
    for condition in ["n IS NULL", "n IS NOT NULL", "b", "b=NULL", "b<>false", "NULL AND true", "NULL AND false"]:
        for projection in ["id", "id,n", "COUNT(*)"]:
            yield f"SELECT {projection} FROM data WHERE {condition}" + ("" if projection == "COUNT(*)" else " ORDER BY id")


def generated_query(rng, ordinal):
    key = rng.randrange(1, 81)
    choice = rng.randrange(20)
    if choice == 0:
        return f"UPDATE data SET n={rng.randrange(-100,101)},b={'true' if key%2 else 'false'} WHERE id={key}"
    if choice == 1:
        return f"SELECT id,s FROM data WHERE id={key}"
    if choice == 2:
        return f"SELECT COUNT(*) FROM data WHERE n>={rng.randrange(-120,121)} AND b=true"
    if choice == 3:
        return f"SELECT d.id,r.label FROM data AS d INNER JOIN refs AS r ON d.id=r.data_id WHERE d.id>={key} ORDER BY d.id LIMIT {rng.randrange(0,10)}"
    if choice == 4:
        return "SELECT id,n,s,b FROM data WHERE n IS NULL ORDER BY id"
    if choice == 5:
        return f"SELECT id FROM data WHERE s='v{key}' ORDER BY id"
    if choice == 6:
        return f"SELECT id,n FROM data WHERE n<>{rng.randrange(-120,121)} ORDER BY id DESC LIMIT {rng.randrange(0,10)}"
    if choice == 7:
        return f"SELECT id FROM data WHERE id={key} AND NULL AND false"
    if choice == 8:
        return f"SELECT COUNT(*) FROM data WHERE id>{key} AND n IS NOT NULL"
    if choice == 9:
        return f"SELECT n+1 AS increment FROM data WHERE id={key}"
    return f"SELECT id,n,s,b FROM data WHERE id>={key} ORDER BY id LIMIT {rng.randrange(0,20)}"


def contract_queries():
    return [
        "CREATE TABLE parents (id BIGINT PRIMARY KEY,name TEXT NOT NULL UNIQUE,n INT)",
        "INSERT INTO parents (id,name,n) VALUES (1,'first',10),(2,'second',20)",
        "INSERT INTO parents (id,name,n) VALUES (3,'third',30),(4,'first',40)",
        "SELECT id,name,n FROM parents ORDER BY id",
        "INSERT INTO parents (id,name) VALUES (1,'duplicate')",
        "INSERT INTO parents (id,n) VALUES (3,3)",
        "CREATE TABLE children (id BIGINT PRIMARY KEY,parent_id BIGINT REFERENCES parents(id) ON UPDATE NO ACTION ON DELETE NO ACTION)",
        "INSERT INTO children (id,parent_id) VALUES (1,1),(2,NULL)",
        "INSERT INTO children (id,parent_id) VALUES (3,999)",
        "DELETE FROM parents WHERE id=1",
        "UPDATE parents SET id=9 WHERE id=1",
        "UPDATE parents SET name='first' WHERE id=2",
        "DROP TABLE parents",
        "BEGIN",
        "ALTER TABLE parents ADD COLUMN note TEXT",
        "UPDATE parents SET note='private' WHERE id=1",
        "INSERT INTO parents (id,name,n) VALUES (1,'failure',1)",
        "SELECT id FROM parents",
        "ROLLBACK",
        "SELECT note FROM parents",
        "SELECT id,name,n FROM parents ORDER BY id",
        "BEGIN READ ONLY",
        "INSERT INTO parents (id,name) VALUES (3,'forbidden')",
        "ROLLBACK",
        "ALTER TABLE parents ADD COLUMN note TEXT",
        "SELECT id,note FROM parents ORDER BY id",
        "CREATE TABLE links (id BIGINT PRIMARY KEY,parent_id BIGINT REFERENCES links(id))",
        "INSERT INTO links (id,parent_id) VALUES (1,2),(2,1)",
        "DELETE FROM links",
        "DROP TABLE links",
        "DELETE FROM children",
        "DROP TABLE children",
        "DROP TABLE parents",
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=ROOT/"target/release/kedr")
    parser.add_argument("--generated", type=int, default=100000)
    parser.add_argument("--manual", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=20261009)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.generated < 0 or args.manual < 0:
        parser.error("counts must be nonnegative")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = {k:v for k,v in os.environ.items() if not k.startswith("PG")}
    env.update({"LC_ALL":"C", "PGCONNECT_TIMEOUT":"5"})
    summary = {"stage":5,"kind":"SQL correctness differential, not a benchmark",
               "status":"running","seed":args.seed,"manual_requested":args.manual,
               "generated_requested":args.generated,"matched":0,"restarts":0,
               "started_at_utc":datetime.now(timezone.utc).isoformat(),
               "psycopg_version":psycopg.__version__}
    paths = [Path(__file__), ROOT/"Cargo.lock", *sorted((ROOT/"crates").rglob("*.rs"))]
    summary["source_sha256"] = {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    started = False
    base = Path(tempfile.mkdtemp(prefix="kedr-sql-", dir="/tmp"))
    connection = kedr = None
    stderr_log = (output/"kedr-stderr.txt").open("w")
    transcript = (output/"transcript.jsonl").open("w")
    digest = hashlib.sha256()
    def run(name, argv):
        return subprocess.run([str(args.pg_bin/name),*map(str,argv)],env=env,text=True,capture_output=True,timeout=60,check=True)
    try:
        cluster, socket = base/"pg", base/"socket"
        socket.mkdir(mode=0o700)
        run("initdb",["-D",cluster,"-U","kedr_oracle","-A","trust","--no-locale","--encoding=UTF8"])
        with (cluster/"postgresql.conf").open("a") as f:
            f.write(f"\nlisten_addresses=''\nunix_socket_directories='{socket}'\nport=56219\nfsync=on\nsynchronous_commit=on\nfull_page_writes=on\ndefault_transaction_isolation='serializable'\n")
        run("pg_ctl",["-D",cluster,"-l",output/"postgres.log","-w","start"])
        started = True
        connection = psycopg.connect(host=str(socket),port=56219,user="kedr_oracle",dbname="postgres",autocommit=True)
        summary["postgres_version"] = connection.execute("SHOW server_version").fetchone()[0]
        reserved = [r[0] for r in connection.execute("SELECT word FROM pg_get_keywords() WHERE catcode='R' ORDER BY word").fetchall()]
        (output/"pg-reserved-keywords.json").write_text(json.dumps(reserved,indent=2)+"\n")
        database = base/"kedr"
        subprocess.run([str(args.binary),"create-db",str(database)],capture_output=True,text=True,check=True)
        kedr = Kedr(args.binary,database,stderr_log)
        def check(sql, label, unordered=False):
            pg, ours = pg_query(connection,sql), comparable(kedr.query(sql))
            # Only test ORDER BY on a unique key, or return all tied rows and
            # compare the multiset. LIMIT through a tie group is ambiguous.
            if unordered and "error" not in pg and "error" not in ours:
                pg["rows"].sort(key=lambda r:json.dumps(r,ensure_ascii=False))
                ours["rows"].sort(key=lambda r:json.dumps(r,ensure_ascii=False))
            record={"case":summary["matched"],"label":label,"sql":sql,"postgres":pg,"kedr":ours}
            line=json.dumps(record,ensure_ascii=False,separators=(",",":"))+"\n"
            transcript.write(line);digest.update(line.encode())
            if pg!=ours:
                (output/"mismatch.json").write_text(json.dumps(record,ensure_ascii=False,indent=2)+"\n")
                raise AssertionError(f"SQL mismatch at {label}: {sql}")
            summary["matched"]+=1
        check("CREATE TABLE data (id BIGINT PRIMARY KEY,n INT,s TEXT,b BOOLEAN)","setup")
        for i in range(1,81):
            n="NULL" if i%5==0 else str(i-40)
            s="NULL" if i%7==0 else f"'v{i}'"
            b="NULL" if i%9==0 else ("true" if i%2 else "false")
            check(f"INSERT INTO data (id,n,s,b) VALUES ({i},{n},{s},{b})","setup")
        check("CREATE INDEX n_idx ON data (n)","setup")
        check("CREATE INDEX s_idx ON data (s)","setup")
        check("CREATE TABLE refs (id BIGINT PRIMARY KEY,data_id BIGINT REFERENCES data(id),label TEXT)","setup")
        for i in range(1,21):
            check(f"INSERT INTO refs (id,data_id,label) VALUES ({i},{i},'r{i}')","setup")
        contracts=0
        for sql in contract_queries():
            check(sql,"contract-"+str(contracts))
            contracts+=1
        summary["contract_matched"]=contracts
        manual=0
        for sql in manual_queries():
            # Restrict nonunique sort cases to the complete result; truncating
            # tied values has no deterministic PostgreSQL ordering contract.
            unordered="ORDER BY id" not in sql and "COUNT(*)" not in sql
            if unordered and "LIMIT" in sql and not sql.endswith("LIMIT 100"):
                continue
            if manual>=args.manual:
                break
            check(sql,"manual-"+str(manual),unordered)
            manual+=1
        if manual<args.manual:
            raise AssertionError(f"only {manual} semantic cases defined, need {args.manual}")
        summary["manual_matched"]=manual
        rng=random.Random(args.seed)
        for i in range(args.generated):
            check(generated_query(rng,i),"generated-"+str(i))
            if i>0 and i%5000==0:
                # Kill an idle session after acknowledged commits, then verify
                # the whole relational state against the oracle after recovery.
                kedr.stop(kill=True)
                kedr.start()
                summary["restarts"]+=1
                check("SELECT id,n,s,b FROM data ORDER BY id","after-SIGKILL")
            if (i+1)%1000==0:
                print(f"SQL differential: {i+1}/{args.generated} generated matched",flush=True)
        summary.update(status="passed",generated_matched=args.generated,transcript_sha256=digest.hexdigest())
    except BaseException as e:
        summary.update(status="failed",error=str(e),transcript_sha256=digest.hexdigest())
        raise
    finally:
        transcript.close()
        if kedr:
            kedr.stop()
        stderr_log.close()
        if connection:
            connection.close()
        summary["finished_at_utc"]=datetime.now(timezone.utc).isoformat()
        (output/"summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n")
        stopped = not started
        if started:
            try:
                run("pg_ctl",["-D",base/"pg","-m","fast","-w","stop"])
                stopped=True
            except BaseException:
                print(f"Temporary PostgreSQL could not stop; preserve {base}",flush=True)
        if stopped:
            import shutil
            shutil.rmtree(base)


if __name__=="__main__":
    main()
