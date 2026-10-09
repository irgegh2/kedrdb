// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    Error,
    engine::StorageEngine,
    page::PAGE_SIZE,
    record::primary_key,
    storage::{StorageIo, write_all_at},
    wal::WalConfig,
};
use kedr_testkit::{
    MemoryStorage, SeededRng,
    crash::{CrashControl, CrashStorage, Persistence},
};
const LIMIT: usize = 8 * 1024 * 1024;
const CACHE: usize = 4 * PAGE_SIZE;
fn memory(bytes: &[u8]) -> MemoryStorage {
    let mut io = MemoryStorage::new(LIMIT);
    write_all_at(&mut io, 0, bytes).unwrap();
    io.sync().unwrap();
    io
}
type Engine = StorageEngine<CrashStorage, CrashStorage>;
fn open(data: &[u8], wal: &[u8], control: &CrashControl) -> Engine {
    StorageEngine::recover(
        CrashStorage::new(memory(data), control.clone()),
        CrashStorage::new(memory(wal), control.clone()),
        WalConfig::first([6; 16]),
        CACHE,
    )
    .unwrap()
    .0
}
fn change(engine: &mut Engine) -> kedr_core::Result<()> {
    engine.transaction(|tx| {
        tx.replace(&primary_key(-1), b"changed")?;
        tx.insert(&primary_key(1), &vec![1; 4096])?;
        tx.insert(&primary_key(2), &vec![2; 4096])?;
        tx.delete(&primary_key(-2))?;
        Ok(())
    })
}
#[test]
fn seeded_two_file_crashes_preserve_acknowledgements_or_diagnose_corrupt_tail() {
    let mut engine = StorageEngine::create(
        MemoryStorage::new(LIMIT),
        MemoryStorage::new(LIMIT),
        [6; 16],
        CACHE,
    )
    .unwrap();
    engine
        .transaction(|tx| {
            tx.insert(&primary_key(-1), b"base")?;
            tx.insert(&primary_key(-2), b"delete only on commit")?;
            tx.insert(&primary_key(-3), b"acknowledged sentinel")
        })
        .unwrap();
    let (d, w) = engine.into_io();
    let data = d.durable_bytes().to_vec();
    let wal = w.durable_bytes().to_vec();
    let control = CrashControl::default();
    let mut engine = open(&data, &wal, &control);
    control.arm(None);
    change(&mut engine).unwrap();
    let trace = control.trace();
    let cases = std::env::var("KEDR_CRASH_CASES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128);
    let seed = 0x4b45445243524153;
    let mut rng = SeededRng::new(seed);
    let mut recovered = 0;
    let mut diagnosed = 0;
    let mut acknowledged = 0;
    // Cover every call, before and after, with total loss of unsynced writes;
    // then sample independent torn/reordered persistence across the two files.
    for case in 0..trace.len() * 2 + cases + 1 {
        let deterministic = case < trace.len() * 2;
        let successful_control = case == trace.len() * 2 + cases;
        let cut = if successful_control {
            trace.len() + 1
        } else if deterministic {
            case / 2 + 1
        } else {
            rng.next_u64() as usize % trace.len() + 1
        };
        let after = if deterministic {
            case % 2 == 1
        } else {
            rng.next_u64() & 1 == 1
        };
        let policy = if deterministic {
            Persistence::Lost
        } else {
            match rng.next_u64() % 3 {
                0 => Persistence::Lost,
                1 => Persistence::OrderedPrefix,
                _ => Persistence::ReorderedTorn,
            }
        };
        let control = CrashControl::default();
        let mut engine = open(&data, &wal, &control);
        control.arm(Some((cut as u64, after)));
        let ack = change(&mut engine).is_ok();
        acknowledged += usize::from(ack);
        let (d, w) = engine.into_io();
        let mut di = d.crash_image(policy, rng.next_u64());
        let mut wi = w.crash_image(policy, rng.next_u64());
        let context = format!(
            "seed={seed:x} case={case} cut={cut} after={after} policy={policy:?} ack={ack}"
        );
        for _ in 0..2 {
            match StorageEngine::recover(memory(&di), memory(&wi), WalConfig::first([6; 16]), CACHE)
            {
                Ok((mut db, _)) => {
                    assert_eq!(
                        db.get(&primary_key(-3)).unwrap(),
                        Some(b"acknowledged sentinel".to_vec()),
                        "{context}"
                    );
                    let changed = db.get(&primary_key(1)).unwrap().is_some();
                    assert_eq!(
                        db.get(&primary_key(2)).unwrap().is_some(),
                        changed,
                        "{context}"
                    );
                    assert_eq!(
                        db.get(&primary_key(-2)).unwrap().is_none(),
                        changed,
                        "{context}"
                    );
                    assert_eq!(
                        db.get(&primary_key(-1)).unwrap(),
                        Some(if changed {
                            b"changed".to_vec()
                        } else {
                            b"base".to_vec()
                        }),
                        "{context}"
                    );
                    assert!(
                        !ack || changed,
                        "acknowledged transaction missing: {context}"
                    );
                    db.integrity_check().unwrap();
                    let (d, w) = db.into_io();
                    di = d.durable_bytes().to_vec();
                    wi = w.durable_bytes().to_vec();
                    recovered += 1;
                }
                Err(Error::CorruptWal { .. }) if !ack && !deterministic => diagnosed += 1,
                Err(error) => panic!("unexpected recovery refusal {error}: {context}"),
            }
        }
    }
    println!(
        "seed={seed:x} random_cases={cases} deterministic_io_cuts={} successful_recoveries={recovered} corrupt_tail_diagnostics={diagnosed} acknowledged_cases={acknowledged}",
        trace.len() * 2
    );
    assert!(recovered > 0 && acknowledged > 0);
}
