// SPDX-License-Identifier: Apache-2.0
use std::{
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
// Fixed logarithmic buckets. Percentiles are upper bounds in microseconds,
// cumulative since process start, including lock waits and I/O, excluding transfer.
const BOUNDS: [u64; 18] = [
    10,
    25,
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10000,
    25000,
    50000,
    100000,
    250000,
    500000,
    1000000,
    5000000,
    u64::MAX,
];
pub(crate) struct Metrics {
    pub connections: AtomicU64,
    pub rejected: AtomicU64,
    pub authentication_errors: AtomicU64,
    pub protocol_errors: AtomicU64,
    histogram: Mutex<Histogram>,
    began: Instant,
}
#[derive(Default)]
struct Histogram {
    buckets: [u64; 18],
    count: u64,
    errors: u64,
    total_us: u64,
}
impl Default for Metrics {
    fn default() -> Self {
        Self {
            connections: 0.into(),
            rejected: 0.into(),
            authentication_errors: 0.into(),
            protocol_errors: 0.into(),
            histogram: Mutex::default(),
            began: Instant::now(),
        }
    }
}
pub(crate) fn event(value: serde_json::Value) {
    eprintln!("{value}");
}
impl Metrics {
    pub fn query(&self, began: Instant, state: Option<&str>) {
        let us = began.elapsed().as_micros().min(u64::MAX as u128) as u64;
        if let Ok(mut h) = self.histogram.lock() {
            let bucket = BOUNDS.iter().position(|b| us <= *b).unwrap_or(17);
            h.buckets[bucket] = h.buckets[bucket].saturating_add(1);
            h.count = h.count.saturating_add(1);
            h.total_us = h.total_us.saturating_add(us);
            h.errors = h.errors.saturating_add(u64::from(state.is_some()));
        }
        if state.is_some() || us >= 250000 {
            event(
                serde_json::json!({"event":if state.is_some(){"query_error"}else{"slow_query"},"elapsed_us":us,"sqlstate":state}),
            );
        }
    }
    pub fn snapshot(&self) -> serde_json::Value {
        let h = self.histogram.lock().unwrap();
        let percentile = |p: u64| -> Option<u64> {
            if h.count == 0 {
                return None;
            }
            let rank = h.count.saturating_mul(p).div_ceil(100);
            let mut sum = 0;
            for (i, b) in h.buckets.iter().enumerate() {
                sum += b;
                if sum >= rank {
                    return Some(BOUNDS[i]);
                }
            }
            None
        };
        serde_json::json!({"metrics_version":1,"scope":"process_cumulative","uptime_ms":self.began.elapsed().as_millis(),"connections_total":self.connections.load(Ordering::Relaxed),"connections_rejected":self.rejected.load(Ordering::Relaxed),"authentication_errors":self.authentication_errors.load(Ordering::Relaxed),"protocol_errors":self.protocol_errors.load(Ordering::Relaxed),"queries":h.count,"query_errors":h.errors,"query_total_us":h.total_us,"p50_upper_us":percentile(50),"p95_upper_us":percentile(95),"p99_upper_us":percentile(99),"latency_bucket_upper_us":BOUNDS,"latency_bucket_counts":h.buckets})
    }
}
pub(crate) fn system(root: &Path) -> serde_json::Value {
    #[cfg(target_os = "linux")]
    let rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|n| n.parse::<u64>().ok())
        });
    #[cfg(not(target_os = "linux"))]
    let rss = std::process::Command::new("/bin/ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok());
    let data_bytes = std::fs::metadata(root.join("data.kdr"))
        .ok()
        .map(|m| m.len());
    let wal_bytes = std::fs::read_dir(root.join("wal"))
        .ok()
        .and_then(|entries| {
            let mut size = 0u64;
            for (i, e) in entries.enumerate() {
                if i >= 1024 {
                    return None;
                }
                size = size.checked_add(e.ok()?.metadata().ok()?.len())?;
            }
            Some(size)
        });
    let disk_available_kib = std::process::Command::new("/bin/df")
        .arg("-Pk")
        .arg(root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| {
            s.lines()
                .last()
                .and_then(|l| l.split_whitespace().nth(3))
                .and_then(|n| n.parse::<u64>().ok())
        });
    serde_json::json!({"rss_kib":rss,"data_bytes":data_bytes,"wal_bytes":wal_bytes,"disk_available_kib":disk_available_kib})
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_histogram_counts_errors_and_has_monotonic_percentiles() {
        let m = Metrics::default();
        assert!(m.snapshot()["p50_upper_us"].is_null());
        for i in 0..100 {
            m.query(Instant::now(), (i == 7).then_some("22003"));
        }
        let s = m.snapshot();
        assert_eq!(s["queries"], 100);
        assert_eq!(s["query_errors"], 1);
        assert!(s["p50_upper_us"].as_u64().unwrap() <= s["p99_upper_us"].as_u64().unwrap());
        assert_eq!(
            s["latency_bucket_counts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .sum::<u64>(),
            100
        );
    }
}
