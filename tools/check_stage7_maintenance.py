#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Real offline migration/backup acceptance, only in a new synthetic PG cluster."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import psycopg
import migrate_postgres as migration
ROOT=Path(__file__).resolve().parents[1]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pg-bin',type=Path,required=True)
    p.add_argument('--kedr',type=Path,default=ROOT/'target/debug/kedr')
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--previous-kedr',type=Path)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    began=time.monotonic();checks=[]
    with tempfile.TemporaryDirectory(prefix='kedr-stage7-') as temporary:
        root=Path(temporary);cluster=root/'pg';socket=root/'socket';socket.mkdir()
        def pg(command,*args):
            subprocess.run([str(a.pg_bin/command),*map(str,args)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        pg('initdb','-D',cluster,'-U','kedr_migration','-A','trust','--no-locale','--encoding=UTF8')
        # Private Unix socket, no TCP listener or an existing user server.
        pg('pg_ctl','-D',cluster,'-l',root/'postgres.log','-o',f"-k {socket} -c listen_addresses='' -p 55489",'-w','start')
        oldenv=dict(os.environ)
        try:
            for name in list(os.environ):
                if name.startswith('PG'):os.environ.pop(name)
            os.environ.update(PGHOST=str(socket),PGPORT='55489',PGUSER='kedr_migration',PGDATABASE='postgres')
            with psycopg.connect(autocommit=True) as source:
                version=source.info.server_version
                source.execute('CREATE TABLE zparent (id BIGINT PRIMARY KEY, name TEXT UNIQUE)')
                source.execute('CREATE TABLE achild (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES zparent(id), n INT, active BOOLEAN, text TEXT)')
                source.execute('CREATE INDEX child_parent ON achild(parent)')
                source.execute('CREATE TABLE selfref (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES selfref(id))')
                source.execute('INSERT INTO zparent VALUES (%s,%s)',(-9223372036854775808,"Кедр '🌲"))
                source.execute('INSERT INTO achild VALUES (%s,%s,%s,%s,%s)',(1,-9223372036854775808,-2147483648,True,"quotes '; \\ \n-- KedrDB SQL export end\nnot a footer"))
                source.execute('INSERT INTO achild VALUES (%s,%s,%s,%s,%s)',(2,None,2147483647,None,''))
                source.execute('INSERT INTO selfref VALUES (1,2),(2,1)')
                before=source.execute('SELECT * FROM achild ORDER BY id').fetchall()
                job=root/'job'
                m=migration.snapshot(job,True)
                migration.import_snapshot(job,a.kedr.resolve())
                checks.append('PG snapshot: all schema/value bytes directly compared, nulls/unicode/quotes/boundaries/self-FK')
                assert source.execute('SELECT * FROM achild ORDER BY id').fetchall()==before
                checks.append('source rows unchanged')
                bad=root/'bad'
                source.execute('CREATE TABLE unsupported (id BIGINT PRIMARY KEY, price NUMERIC)')
                try: migration.snapshot(bad,True)
                except migration.Incompatible: pass
                else: raise AssertionError('numeric accepted')
                assert not bad.exists();source.execute('DROP TABLE unsupported');checks.append('unsupported schema refused before destination creation')
                # Acquiring table SHARE locks actually blocks ordinary app writers.
                with psycopg.connect(autocommit=True) as locker:
                    locker.execute('BEGIN');locker.execute('LOCK TABLE achild IN SHARE MODE')
                    source.execute("SET lock_timeout='100ms'")
                    try: source.execute("UPDATE achild SET text='must not write' WHERE id=1")
                    except psycopg.errors.LockNotAvailable: pass
                    else: raise AssertionError('writer was not blocked')
                    locker.execute('ROLLBACK');source.execute("SET lock_timeout='0'")
                checks.append('SHARE table lock blocks writes')
                database=job/'database';backup=a.output/'portable-backup';portable=a.output/'portable.sql'
                def kedr(*args):
                    return subprocess.check_output([str(a.kedr.resolve()),*map(str,args)],text=True)
                kedr('backup',database,backup);kedr('verify-backup',backup)
                restored=root/'restored';kedr('restore',backup,restored);kedr('export-sql',restored,portable)
                assert portable.read_bytes()==(job/'snapshot.sql').read_bytes()
                kedr('diagnostics',restored,a.output/'diagnostics.json')
                checks.append('physical backup/restore exact full schema/data export and offline diagnostics')
                if a.previous_kedr:
                    # Current completed format remains readable/writable by the old binary.
                    previous=subprocess.check_output([str(a.previous_kedr.resolve()),'sql',str(restored),'SELECT id,n FROM achild ORDER BY id'],text=True)
                    assert json.loads(previous)['rows']==[[1,-2147483648],[2,2147483647]]
                    subprocess.check_output([str(a.previous_kedr.resolve()),'sql',str(restored),"INSERT INTO achild (id,parent,n,active,text) VALUES (3,NULL,17,false,'old binary')"],text=True)
                    assert json.loads(kedr('sql',restored,'SELECT id,n FROM achild WHERE id=3'))['rows']==[[3,17]]
                    checks.append('stage6 binary reads/writes completed stage7 format and new binary reopens it')
                # Equal row count is insufficient: a changed value fails direct verification.
                probe=root/'mismatch';kedr('import-migration',job/'snapshot.sql',probe)
                altered=root/'altered.sql';altered.write_bytes((job/'snapshot.sql').read_bytes().replace(b'not a footer',b'bad a footer'))
                attempt=subprocess.run([str(a.kedr.resolve()),'complete-migration',str(altered),str(probe)],capture_output=True)
                assert attempt.returncode and (probe/'.migration-incomplete').exists()
                assert subprocess.run([str(a.kedr.resolve()),'sql',str(probe),'SELECT * FROM achild'],capture_output=True).returncode
                kedr('complete-migration',job/'snapshot.sql',probe)
                checks.append('equal counts with one changed value rejected, partial migration inaccessible')
                summary=dict(stage=7,postgres_version=version,psycopg=psycopg.__version__,checks=checks,rows=m['rows'],tables=m['tables'],snapshot_ms=m['snapshot_ms'],elapsed_s=round(time.monotonic()-began,3),performance_benchmark=False)
                (a.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
        finally:
            os.environ.clear();os.environ.update(oldenv)
            pg('pg_ctl','-D',cluster,'-m','immediate','-w','stop')


if __name__=='__main__':main()
