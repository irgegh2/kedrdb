#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Offline PostgreSQL 16..18 migration for the explicitly bounded Kedr SQL profile.

Connection settings come from libpq environment/service files (never a CLI DSN).
Snapshot creates private files in a NEW job directory; it never modifies source data.
Use a maintenance window: stop ALL application/background/admin writes and DDL.
SHARE locks protect existing tables; they cannot protect new objects elsewhere.
Resume only imports the completed saved snapshot, never opens a new source snapshot.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

TYPES = {23: ("INT", "int4"), 20: ("BIGINT", "int8"), 25: ("TEXT", "text"), 16: ("BOOLEAN", "bool")}
HEADER = "-- KedrDB SQL export 1\n"
FOOTER = "-- KedrDB SQL export end\n"


class Incompatible(Exception):
    pass


def require(condition, code):
    if not condition:
        raise Incompatible(code)


def ident(name):
    require(isinstance(name, str) and 0 < len(name.encode()) <= 63 and "\0" not in name, "identifier_limit")
    return '"' + name.replace('"', '""') + '"'


def literal(value):
    if value is None:
        return "NULL"
    if type(value) is bool:
        return str(value).lower()
    if type(value) is int:
        return str(value)
    require(type(value) is str, "value_type")
    return "'" + value.replace("'", "''") + "'"


def sync_dir(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fresh_file(path):
    return os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w", encoding="utf-8", newline="\n")


def digest(path):
    require(path.is_file() and not path.is_symlink(), "snapshot_not_regular")
    hash_ = hashlib.sha256()
    count = 0
    with path.open("rb") as stream:
        while chunk := stream.read(65536):
            count += len(chunk)
            require(count <= 64 * 1024**3, "snapshot_size_limit")
            hash_.update(chunk)
    return hash_.hexdigest()


def connect():
    import psycopg
    connection = psycopg.connect(connect_timeout=10, autocommit=True)
    require(160000 <= connection.info.server_version < 190000, "source_version_not_16_17_18")
    # Local Unix sockets are permitted. TCP requires authenticated certificates.
    require(connection.info.host.startswith("/") or
            (connection.pgconn.ssl_in_use and connection.info.get_parameters().get("sslmode") == "verify-full"), "source_requires_socket_or_verify_full_tls")
    return connection


def schema(connection):
    """Read catalog with bounded object/column counts before reading application rows."""
    def query(sql, args=()):
        return connection.execute(sql, args or None).fetchall()
    require(not query("SELECT 1 FROM pg_namespace WHERE nspname NOT IN ('public','information_schema') AND nspname NOT LIKE 'pg_%' LIMIT 1"), "multiple_schemas")
    require(not query("SELECT 1 FROM pg_extension WHERE extname <> 'plpgsql' LIMIT 1"), "extensions")
    require(not query("SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' LIMIT 1"), "routines")
    require(not query("SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public' AND t.typtype IN ('d','e','r','m') LIMIT 1"), "custom_types")
    require(not query("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind NOT IN ('r','i') LIMIT 1"), "views_sequences_partitions_foreign_tables")
    relations = query("SELECT c.oid,c.relname,c.relrowsecurity,c.relforcerowsecurity,c.relpersistence,c.relacl FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r' ORDER BY c.relname COLLATE \"C\" LIMIT 129")
    require(len(relations) <= 128, "table_limit")
    require(not query("SELECT 1 FROM pg_inherits LIMIT 1"), "inheritance")
    db_collation = query("SELECT datcollate,datlocprovider FROM pg_database WHERE datname=current_database()")[0]
    tables = []
    for oid, name, rls, forced, persistence, acl in relations:
        ident(name)
        require(not rls and not forced and persistence == 'p' and acl is None, "rls_persistence_custom_acl")
        require(not query("SELECT 1 FROM pg_trigger WHERE tgrelid=%s AND NOT tgisinternal LIMIT 1", (oid,)), "triggers")
        require(not query("SELECT 1 FROM pg_rewrite WHERE ev_class=%s LIMIT 1", (oid,)), "rules")
        attrs = query("SELECT a.attnum,a.attname,a.atttypid,a.attnotnull,a.attidentity,a.attgenerated,a.atthasdef,co.collname,a.attacl FROM pg_attribute a LEFT JOIN pg_collation co ON co.oid=a.attcollation WHERE a.attrelid=%s AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum LIMIT 257", (oid,))
        require(0 < len(attrs) <= 256, "column_limit")
        cols = []
        for num, cname, typ, nonnull, identity, generated, default, collation, cacl in attrs:
            ident(cname)
            require(typ in TYPES and not identity and not generated and not default and cacl is None, "types_defaults_identity_generated_acl")
            require(typ != 25 or collation in ('C','POSIX') or (collation == 'default' and db_collation[0] in ('C','POSIX') and db_collation[1] == 'c'), "text_collation_not_bytewise")
            cols.append(dict(num=num, name=cname, oid=typ, nullable=not nonnull, primary=False, unique=False, foreign=None))
        constraints = query("SELECT contype,conkey,confrelid,confkey,confupdtype,confdeltype,confmatchtype,condeferrable,condeferred,convalidated FROM pg_constraint WHERE conrelid=%s AND contype <> 'n' ORDER BY oid LIMIT 1025", (oid,))
        require(len(constraints) <= 1024, "constraint_limit")
        pk = []
        for kind, keys, parent, parentkeys, update, delete, match, deferred, initially, valid in constraints:
            require(kind in ('p','u','f') and len(keys) == 1 and not deferred and not initially and valid, "constraint_profile")
            c = next((c for c in cols if c['num'] == keys[0]), None)
            require(c is not None, "constraint_column")
            if kind == 'p':
                require(c['oid'] == 20 and not c['nullable'], "primary_requires_explicit_bigint")
                c['primary'] = True
                pk.append(c['name'])
            elif kind == 'u':
                c['unique'] = True
            else:
                require(len(parentkeys) == 1 and update in ('a','r') and delete in ('a','r') and match == 's' and c['oid'] == 20, "foreign_actions_or_type")
                require(c['foreign'] is None, 'multiple_foreign_constraints_on_column')
                c['foreign'] = [parent, parentkeys[0]]
        require(len(pk) == 1, "exactly_one_bigint_primary")
        indexes = []
        rows = query("SELECT ci.relname,i.indkey::smallint[],i.indisunique,i.indisprimary,i.indisvalid,i.indisready,i.indnkeyatts,i.indnatts,i.indpred IS NOT NULL,i.indexprs IS NOT NULL,i.indoption::smallint[],am.amname,i.indnullsnotdistinct,EXISTS(SELECT 1 FROM pg_constraint c WHERE c.conindid=i.indexrelid AND c.contype IN ('p','u')),i.indcollation::oid[],i.indclass::oid[] FROM pg_index i JOIN pg_class ci ON ci.oid=i.indexrelid JOIN pg_am am ON am.oid=ci.relam WHERE i.indrelid=%s ORDER BY ci.relname COLLATE \"C\" LIMIT 257", (oid,))
        require(len(rows) <= 256, "index_limit")
        for iname, keys, unique, primary, valid, ready, nkey, nattr, predicate, expr, options, method, nullsame, constraint, collations, classes in rows:
            require(valid and ready and nkey == nattr == 1 and not predicate and not expr and not any(options) and method == 'btree' and not nullsame, "index_profile")
            require(not query("SELECT 1 FROM pg_opclass WHERE oid=%s AND NOT opcdefault", (classes[0],)), "custom_index_opclass")
            c = next((c for c in cols if c['num'] == keys[0]), None)
            require(c is not None, "index_column")
            if c['oid'] == 25:
                coll = query("SELECT collname FROM pg_collation WHERE oid=%s", (collations[0],))[0][0]
                require(coll in ('C','POSIX') or coll == 'default' and db_collation[0] in ('C','POSIX') and db_collation[1] == 'c', "index_collation")
            if not primary and not constraint:
                ident(iname)
                require(not iname.startswith('__u_'), "reserved_internal_index_name")
                indexes.append(dict(name=iname, column=c['name'], unique=unique))
        tables.append(dict(oid=oid, name=name, columns=cols, primary=pk[0], indices=indexes))
    by_id = {t['oid']: t for t in tables}
    for table in tables:
        for c in table['columns']:
            if c['foreign']:
                parent_id, num = c['foreign']
                parent = by_id.get(parent_id)
                require(parent is not None, "foreign_outside_profile")
                pc = next((p for p in parent['columns'] if p['num'] == num), None)
                require(pc is not None and pc['primary'], "foreign_must_reference_primary")
                c['foreign'] = [parent['name'], pc['name']]
    ordered = []
    remaining = sorted(tables, key=lambda t: t['name'].encode())
    while remaining:
        names = {t['name'] for t in ordered}
        candidate = next((t for t in remaining if all(not c['foreign'] or c['foreign'][0] == t['name'] or c['foreign'][0] in names for c in t['columns'])), None)
        require(candidate is not None, "cyclic_table_dependencies")
        ordered.append(candidate)
        remaining.remove(candidate)
    return ordered


def definitions(tables):
    for table in tables:
        cols = []
        for c in table['columns']:
            text = ident(c['name']) + ' ' + TYPES[c['oid']][0]
            if c['primary']:
                text += ' PRIMARY KEY'
            else:
                if not c['nullable']:
                    text += ' NOT NULL'
                if c['unique']:
                    text += ' UNIQUE'
            if c['foreign']:
                text += ' REFERENCES ' + ident(c['foreign'][0]) + ' (' + ident(c['foreign'][1]) + ')'
            cols.append(text)
        yield 'CREATE TABLE ' + ident(table['name']) + ' (' + ', '.join(cols) + ');\n'
    for table in tables:
        for index in table['indices']:
            yield 'CREATE ' + ('UNIQUE ' if index['unique'] else '') + 'INDEX ' + ident(index['name']) + ' ON ' + ident(table['name']) + ' (' + ident(index['column']) + ');\n'


def row_limits(table, row):
    require(len(row) == len(table['columns']), "row_width")
    size = 3
    for c, value in zip(table['columns'], row):
        if value is None:
            require(c['nullable'], "nullability_mismatch")
            size += 1
        elif c['oid'] == 25:
            require(type(value) is str, "text_type")
            size += 5 + len(value.encode())
            indexed = c['unique'] or any(i['column'] == c['name'] for i in table['indices'])
            require(not indexed or 17 + 1 + len(value.encode()) + value.count('\0') + 2 + 8 <= 512, "indexed_text_limit")
        elif c['oid'] == 16:
            require(type(value) is bool, "bool_type")
            size += 2
        else:
            require(type(value) is int and -(2**(31 if c['oid'] == 23 else 63)) <= value < 2**(31 if c['oid'] == 23 else 63), "integer_range")
            size += 5 if c['oid'] == 23 else 9
    require(size <= 8192, "encoded_row_limit")


def check_row_sizes(connection, table):
    # Check sizes INSIDE PostgreSQL before fetching TEXT into client memory.
    # SHARE locks make these checks stable for the subsequent cursor reads.
    costs = []
    predicates = []
    for c in table['columns']:
        col = ident(c['name'])
        if c['oid'] == 25:
            cost = '5 + octet_length(' + col + ')'
            indexed = c['unique'] or any(i['column'] == c['name'] for i in table['indices'])
            if indexed:
                predicates.append('octet_length(' + col + ') > 484')
        else:
            cost = str(2 if c['oid'] == 16 else 5 if c['oid'] == 23 else 9)
        costs.append('CASE WHEN ' + col + ' IS NULL THEN 1 ELSE ' + cost + ' END')
    predicates.append('3 + ' + ' + '.join(costs) + ' > 8192')
    query = 'SELECT 1 FROM "public".' + ident(table['name']) + ' WHERE ' + ' OR '.join('(' + p + ')' for p in predicates) + ' LIMIT 1'
    require(connection.execute(query).fetchone() is None, 'row_or_index_size_limit')


def snapshot(job, write_stop_confirmed=False):
    require(write_stop_confirmed, "explicit_maintenance_write_stop_required")
    import psycopg.sql
    began = time.monotonic()
    with connect() as connection:
        # Catalog preflight is read-only. No destination exists for incompatibility.
        connection.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        tables = schema(connection)
        connection.execute("ROLLBACK")
        # Names are learned before BEGIN: acquire ALL SHARE locks before the first
        # SELECT that fixes the repeatable-read snapshot. No INSERT/UPDATE/DDL.
        connection.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        connection.execute("SET LOCAL lock_timeout='5s'")
        connection.execute("SET LOCAL statement_timeout='5min'")
        if tables:
            names = psycopg.sql.SQL(', ').join(psycopg.sql.Identifier('public', t['name']) for t in tables)
            connection.execute(psycopg.sql.SQL('LOCK TABLE {} IN SHARE MODE').format(names))
        locked = schema(connection)
        require(locked == tables, "schema_changed_after_preflight")
        snapshot_id = connection.execute("SELECT pg_current_snapshot()::text").fetchone()[0]
        for table in tables:
            check_row_sizes(connection, table)
        os.mkdir(job, 0o700)
        sync_dir(job.parent)
        pending = job / 'snapshot.pending'
        rows = 0
        with fresh_file(pending) as output:
            output.write(HEADER)
            for definition in definitions(tables):
                require(len(definition.encode()) <= 65536, "schema_statement_limit")
                output.write(definition)
            for number, table in enumerate(tables):
                columns = ', '.join(ident(c['name']) for c in table['columns'])
                query = 'SELECT ' + columns + ' FROM "public".' + ident(table['name']) + ' ORDER BY ' + ident(table['primary'])
                with connection.cursor(name=f'kedr_snapshot_{number}') as cursor:
                    cursor.execute(query)
                    while chunk := cursor.fetchmany(64):
                        for row in chunk:
                            row_limits(table, row)
                            statement = 'INSERT INTO ' + ident(table['name']) + ' (' + columns + ') VALUES (' + ', '.join(literal(v) for v in row) + ');\n'
                            require(len(statement.encode()) <= 65536, "row_statement_limit")
                            output.write(statement)
                            rows += 1
            require(schema(connection) == tables, "schema_changed_during_snapshot")
            output.write(FOOTER)
            output.flush()
            os.fsync(output.fileno())
        connection.execute("ROLLBACK")
        os.rename(pending, job / 'snapshot.sql')
        sync_dir(job)
        manifest = dict(migration_version=1, state='snapshot_complete', source_version=connection.info.server_version,
                        snapshot_id=snapshot_id, sha256=digest(job / 'snapshot.sql'), rows=rows, tables=len(tables),
                        adapter='postgres-16-18-bounded-v1', comparison='all_canonical_schema_and_value_bytes',
                        created_utc=datetime.now(timezone.utc).isoformat(), snapshot_ms=round((time.monotonic()-began)*1000))
        with fresh_file(job / 'manifest.pending') as output:
            json.dump(manifest, output, indent=2)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        os.rename(job / 'manifest.pending', job / 'manifest.json')
        sync_dir(job)
        print(json.dumps({k: manifest[k] for k in ('state','rows','tables','source_version','snapshot_ms')}))
        return manifest


def load(job):
    manifest_path = job / 'manifest.json'
    require(not manifest_path.is_symlink() and manifest_path.stat().st_size <= 16384, "manifest_limit_or_symlink")
    manifest = json.loads(manifest_path.read_text())
    require(manifest.get('migration_version') == 1 and manifest.get('state') == 'snapshot_complete' and manifest.get('adapter') == 'postgres-16-18-bounded-v1', "migration_manifest_version")
    require(digest(job / 'snapshot.sql') == manifest['sha256'], "snapshot_checksum_mismatch")
    return manifest


def import_snapshot(job, binary):
    manifest = load(job)
    require(job.is_dir() and not job.is_symlink(), "job_directory")
    target = job / 'database'
    export = job / 'snapshot.sql'
    if target.exists():
        require(not target.is_symlink() and (target / '.migration-incomplete').is_file(), "existing_target_is_not_pending_migration")
        if (target / '.import-incomplete').exists():
            subprocess.run([str(binary), 'resume-import', str(export), str(target)], check=True)
    else:
        subprocess.run([str(binary), 'import-migration', str(export), str(target)], check=True)
    # Direct, byte-for-byte streaming comparison of normalized schema AND ALL
    # typed values, not count/hash only. The target remains guarded until success.
    subprocess.run([str(binary), 'complete-migration', str(export), str(target)], check=True)
    with fresh_file(job / 'verified.json') as output:
        json.dump(dict(migration_version=1,state='verified',rows=manifest['rows'],tables=manifest['tables'],
                       sha256=manifest['sha256'],comparison=manifest['comparison'],application_verified=False,
                       source_modified=False),output,indent=2)
        output.write('\n');output.flush();os.fsync(output.fileno())
    sync_dir(job)
    print(json.dumps(dict(event='migration_complete',rows=manifest['rows'],tables=manifest['tables'],application_verified=False)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['preflight','snapshot','import'])
    parser.add_argument('--job', type=Path)
    parser.add_argument('--kedr', type=Path, default=Path('target/release/kedr'))
    parser.add_argument('--writes-stopped', action='store_true', help='Confirm ALL writers/DDL are stopped for the maintenance window')
    args = parser.parse_args()
    try:
        if args.command == 'preflight':
            with connect() as connection:
                connection.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                tables = schema(connection)
                connection.execute("ROLLBACK")
                print(json.dumps(dict(profile='postgres-16-18-bounded-v1',compatible=True,tables=len(tables),data_scanned=False,application_verified=False)))
        else:
            require(args.job is not None, 'job_required')
            if args.command == 'snapshot':
                snapshot(args.job, args.writes_stopped)
            else:
                import_snapshot(args.job, args.kedr.resolve())
    except Exception as error:
        # PostgreSQL errors can echo queries and connection secrets; only codes.
        code = str(error) if isinstance(error, Incompatible) else getattr(error, 'sqlstate', None) or type(error).__name__
        print(json.dumps(dict(event='migration_failed',code=code)),file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
