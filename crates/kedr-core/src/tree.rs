// SPDX-License-Identifier: Apache-2.0
//! Single-owner B+ tree storage. Stage 2: clean checkpoints, NOT atomic commits.
//! Any mutating I/O failure poisons the handle. WAL integration is a later layer.

mod format;
mod store;

use crate::{
    Error, Result,
    page::{MAX_RECORD_SIZE, PAGE_SIZE},
    record::MAX_KEY_SIZE,
    storage::StorageIo,
};
use format::{Entry, Node};
pub(crate) use store::read_frontier;
pub use store::{CacheStats, PageStore};

const MAX_DEPTH: usize = 64;
pub type KeyValue = (Vec<u8>, Vec<u8>);
struct ScanResult {
    rows: Vec<KeyValue>,
    bytes: usize,
    limit: usize,
}

pub struct BPlusTree<I: StorageIo> {
    store: PageStore<I>,
}

impl<I: StorageIo> BPlusTree<I> {
    pub(crate) fn io_mut(&mut self) -> &mut I {
        self.store.io_mut()
    }
    pub fn create(io: I, database_id: [u8; 16], cache_budget: usize) -> Result<Self> {
        Ok(Self {
            store: PageStore::create(io, database_id, cache_budget)?,
        })
    }
    pub fn open(io: I, cache_budget: usize) -> Result<Self> {
        let mut tree = Self {
            store: PageStore::open(io, cache_budget)?,
        };
        // Fast open checks the root; full graph/free-list validation is explicit.
        if matches!(tree.node(tree.store.root())?, Node::Free(_)) {
            return Err(Error::CorruptStore {
                reason: "root references free page",
            });
        }
        Ok(tree)
    }
    pub fn cache_stats(&self) -> CacheStats {
        self.store.stats()
    }
    pub fn page_count(&self) -> u64 {
        self.store.page_count()
    }
    pub fn database_id(&self) -> [u8; 16] {
        self.store.database_id()
    }
    pub fn checkpoint_lsn(&self) -> u64 {
        self.store.checkpoint_lsn()
    }
    pub fn checkpoint(&mut self) -> Result<()> {
        self.store.checkpoint()
    }
    pub fn into_io(self) -> I {
        self.store.into_io()
    }

    fn node(&mut self, id: u64) -> Result<Node> {
        self.store.io_mut().check_interrupt()?;
        let guard = self.store.pin(id)?;
        Node::decode(guard.bytes(), id, guard.page_count())
    }
    fn write_node(&mut self, id: u64, node: &Node) -> Result<()> {
        self.store.write(id, &node.encode(id)?)
    }
    fn check_key(key: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(Error::InvalidArgument {
                message: "key length must be 1..512",
            });
        }
        Ok(())
    }
    fn leaf_path(&mut self, key: &[u8]) -> Result<(u64, Vec<(u64, usize)>)> {
        let mut id = self.store.root();
        let mut path = Vec::new();
        let mut expected_level = None;
        for _ in 0..MAX_DEPTH {
            let node = self.node(id)?;
            if expected_level.is_some_and(|level| level != node.level()) {
                return Err(Error::CorruptStore {
                    reason: "child level mismatch",
                });
            }
            match node {
                Node::Leaf(_) => return Ok((id, path)),
                Node::Branch {
                    level,
                    keys,
                    children,
                } => {
                    let at = keys.partition_point(|k| k.as_slice() <= key);
                    path.push((id, at));
                    id = children[at];
                    expected_level = Some(level - 1);
                }
                Node::Free(_) => {
                    return Err(Error::CorruptStore {
                        reason: "tree references free page",
                    });
                }
            }
        }
        Err(Error::CorruptStore {
            reason: "tree depth or cycle",
        })
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Self::check_key(key)?;
        let mut id = self.store.root();
        let mut expected_level = None;
        for _ in 0..MAX_DEPTH {
            let node = self.node(id)?;
            if expected_level.is_some_and(|level| level != node.level()) {
                return Err(Error::CorruptStore {
                    reason: "child level mismatch",
                });
            }
            match node {
                Node::Leaf(mut entries) => {
                    return Ok(entries
                        .binary_search_by(|e| e.key.as_slice().cmp(key))
                        .ok()
                        .map(|at| std::mem::take(&mut entries[at].value)));
                }
                Node::Branch {
                    level,
                    keys,
                    children,
                } => {
                    id = children[keys.partition_point(|k| k.as_slice() <= key)];
                    expected_level = Some(level - 1);
                }
                Node::Free(_) => {
                    return Err(Error::CorruptStore {
                        reason: "tree references free page",
                    });
                }
            }
        }
        Err(Error::CorruptStore {
            reason: "tree depth or cycle",
        })
    }

    /// Insert rejects duplicates. replace is an upsert. Logical errors are atomic;
    /// stage-2 I/O failures may leave partial disk changes and poison this handle.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value, false)
    }
    pub fn replace(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value, true)
    }
    fn put(&mut self, key: &[u8], value: &[u8], replace: bool) -> Result<()> {
        Self::check_key(key)?;
        if value.len() > MAX_RECORD_SIZE {
            return Err(Error::RecordTooLarge {
                length: value.len(),
                limit: MAX_RECORD_SIZE,
            });
        }
        let (leaf, path) = self.leaf_path(key)?;
        let Node::Leaf(mut entries) = self.node(leaf)? else {
            unreachable!("validated path")
        };
        match entries.binary_search_by(|e| e.key.as_slice().cmp(key)) {
            Ok(at) => {
                if !replace {
                    return Err(Error::DuplicateKey);
                }
                entries[at].value = value.to_vec();
            }
            Err(at) => entries.insert(
                at,
                Entry {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
            ),
        }
        let result = self.put_inner(leaf, entries, path);
        if result.is_err() {
            self.store.poison();
        }
        result
    }
    fn put_inner(
        &mut self,
        leaf: u64,
        entries: Vec<Entry>,
        mut path: Vec<(u64, usize)>,
    ) -> Result<()> {
        let mut node = Node::Leaf(entries);
        let mut left = leaf;
        loop {
            if node.encoded_len() <= PAGE_SIZE {
                return self.write_node(left, &node);
            }
            let (left_node, separator, right_node) = node.split()?;
            let right = self.store.allocate()?;
            self.write_node(left, &left_node)?;
            self.write_node(right, &right_node)?;
            let Some((parent, at)) = path.pop() else {
                let level = left_node.level() + 1;
                if usize::from(level) >= MAX_DEPTH {
                    return Err(Error::CorruptStore {
                        reason: "depth limit",
                    });
                }
                let root = self.store.allocate()?;
                self.write_node(
                    root,
                    &Node::Branch {
                        level,
                        keys: vec![separator],
                        children: vec![left, right],
                    },
                )?;
                self.store.set_root(root);
                return Ok(());
            };
            let Node::Branch {
                level,
                mut keys,
                mut children,
            } = self.node(parent)?
            else {
                return Err(Error::CorruptStore {
                    reason: "non-branch ancestor",
                });
            };
            if children.get(at) != Some(&left) {
                return Err(Error::CorruptStore {
                    reason: "path changed",
                });
            }
            keys.insert(at, separator);
            children.insert(at + 1, right);
            node = Node::Branch {
                level,
                keys,
                children,
            };
            left = parent;
        }
    }

    /// Underfull leaves are kept. Empty leaves/subtrees are pruned and reclaimed.
    /// Separator keys are lower bounds, not necessarily the current minimum.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        Self::check_key(key)?;
        let (leaf, mut path) = self.leaf_path(key)?;
        let Node::Leaf(mut entries) = self.node(leaf)? else {
            unreachable!("validated path")
        };
        let Ok(at) = entries.binary_search_by(|e| e.key.as_slice().cmp(key)) else {
            return Ok(false);
        };
        entries.remove(at);
        let result = (|| {
            if !entries.is_empty() || path.is_empty() {
                return self.write_node(leaf, &Node::Leaf(entries));
            }
            let mut removed = leaf;
            while let Some((parent, at)) = path.pop() {
                let Node::Branch {
                    level,
                    mut keys,
                    mut children,
                } = self.node(parent)?
                else {
                    return Err(Error::CorruptStore {
                        reason: "non-branch ancestor",
                    });
                };
                if children.get(at) != Some(&removed) {
                    return Err(Error::CorruptStore {
                        reason: "path changed",
                    });
                }
                children.remove(at);
                if !keys.is_empty() {
                    keys.remove(if at == 0 { 0 } else { at - 1 });
                }
                self.store.free(removed)?;
                if children.is_empty() {
                    removed = parent;
                    continue;
                }
                if path.is_empty() && children.len() == 1 {
                    self.store.set_root(children[0]);
                    self.store.free(parent)?;
                } else {
                    self.write_node(
                        parent,
                        &Node::Branch {
                            level,
                            keys,
                            children,
                        },
                    )?;
                }
                return Ok(());
            }
            // The root itself has become empty. Keep one leaf as the new root.
            self.write_node(removed, &Node::Leaf(Vec::new()))?;
            self.store.set_root(removed);
            Ok(())
        })();
        if result.is_err() {
            self.store.poison();
        }
        result.map(|()| true)
    }

    /// start inclusive, end exclusive; bounded result, ordered by encoded key.
    pub fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<KeyValue>> {
        if limit > 100_000 {
            return Err(Error::InvalidArgument {
                message: "range limit exceeds 100000",
            });
        }
        if let Some(key) = start {
            Self::check_key(key)?;
        }
        if let Some(key) = end {
            Self::check_key(key)?;
        }
        if limit == 0 || start.zip(end).is_some_and(|(s, e)| s >= e) {
            return Ok(Vec::new());
        }
        let mut output = ScanResult {
            rows: Vec::new(),
            bytes: 0,
            limit,
        };
        self.scan(self.store.root(), (start, end), &mut output, 0, None)?;
        Ok(output.rows)
    }
    fn scan(
        &mut self,
        id: u64,
        bounds: (Option<&[u8]>, Option<&[u8]>),
        output: &mut ScanResult,
        depth: usize,
        expected_level: Option<u16>,
    ) -> Result<()> {
        let (start, end) = bounds;
        if depth >= MAX_DEPTH {
            return Err(Error::CorruptStore {
                reason: "tree depth or cycle",
            });
        }
        let node = self.node(id)?;
        if expected_level.is_some_and(|level| level != node.level()) {
            return Err(Error::CorruptStore {
                reason: "child level mismatch",
            });
        }
        match node {
            Node::Leaf(entries) => {
                for entry in entries {
                    if start.is_some_and(|s| entry.key.as_slice() < s) {
                        continue;
                    }
                    if end.is_some_and(|e| entry.key.as_slice() >= e) {
                        break;
                    }
                    let bytes = output.bytes
                        + entry.key.len()
                        + entry.value.len()
                        + std::mem::size_of::<KeyValue>();
                    if bytes > 8 * 1024 * 1024 {
                        return Err(Error::LimitExceeded {
                            limit: 8 * 1024 * 1024,
                            requested: bytes as u64,
                        });
                    }
                    output.bytes = bytes;
                    output.rows.push((entry.key, entry.value));
                    if output.rows.len() >= output.limit {
                        break;
                    }
                }
            }
            Node::Branch {
                level,
                keys,
                children,
            } => {
                for (at, child) in children.into_iter().enumerate() {
                    if at < keys.len() && start.is_some_and(|s| keys[at].as_slice() <= s) {
                        continue;
                    }
                    if at > 0 && end.is_some_and(|e| keys[at - 1].as_slice() >= e) {
                        break;
                    }
                    self.scan(child, bounds, output, depth + 1, Some(level - 1))?;
                    if output.rows.len() >= output.limit {
                        break;
                    }
                }
            }
            Node::Free(_) => {
                return Err(Error::CorruptStore {
                    reason: "tree references free page",
                });
            }
        }
        Ok(())
    }

    /// Checks all ownership, bounds and levels with a bit per file page.
    pub fn integrity_check(&mut self) -> Result<IntegrityReport> {
        let count = self.store.page_count();
        let words = usize::try_from(count.div_ceil(64)).map_err(|_| Error::Allocation {
            requested: usize::MAX,
        })?;
        if words > 8 * 1024 * 1024 {
            return Err(Error::LimitExceeded {
                limit: 64 * 1024 * 1024,
                requested: (words as u64).saturating_mul(8),
            });
        }
        let mut seen = Vec::new();
        seen.try_reserve_exact(words)
            .map_err(|_| Error::Allocation {
                requested: words.saturating_mul(8),
            })?;
        seen.resize(words, 0u64);
        let mut report = IntegrityReport {
            pages: count,
            tree_pages: 0,
            free_pages: 0,
            records: 0,
            height: 0,
        };
        self.check_node(
            self.store.root(),
            (None, None),
            None,
            &mut seen,
            &mut report,
            0,
        )?;
        let mut free = self.store.free_head();
        while free != 0 {
            mark(&mut seen, free, count)?;
            let Node::Free(next) = self.node(free)? else {
                return Err(Error::CorruptStore {
                    reason: "free chain points to live node",
                });
            };
            report.free_pages += 1;
            free = next;
        }
        if report.tree_pages + report.free_pages != count - 2 {
            return Err(Error::CorruptStore {
                reason: "unowned pages",
            });
        }
        Ok(report)
    }
    fn check_node(
        &mut self,
        id: u64,
        bounds: (Option<&[u8]>, Option<&[u8]>),
        expected: Option<u16>,
        seen: &mut [u64],
        report: &mut IntegrityReport,
        depth: usize,
    ) -> Result<()> {
        let (low, high) = bounds;
        if depth >= MAX_DEPTH {
            return Err(Error::CorruptStore {
                reason: "tree depth limit",
            });
        }
        mark(seen, id, self.store.page_count())?;
        let node = self.node(id)?;
        if expected.is_some_and(|v| v != node.level()) {
            return Err(Error::CorruptStore {
                reason: "child level mismatch",
            });
        }
        report.tree_pages += 1;
        report.height = report.height.max(depth + 1);
        match node {
            Node::Leaf(entries) => {
                for entry in &entries {
                    check_bounds(&entry.key, low, high)?;
                }
                report.records += entries.len() as u64;
            }
            Node::Branch {
                level,
                keys,
                children,
            } => {
                for key in &keys {
                    check_bounds(key, low, high)?;
                }
                for (at, child) in children.iter().enumerate() {
                    let child_low = if at == 0 {
                        low
                    } else {
                        Some(keys[at - 1].as_slice())
                    };
                    let child_high = if at == keys.len() {
                        high
                    } else {
                        Some(keys[at].as_slice())
                    };
                    self.check_node(
                        *child,
                        (child_low, child_high),
                        Some(level - 1),
                        seen,
                        report,
                        depth + 1,
                    )?;
                }
            }
            Node::Free(_) => {
                return Err(Error::CorruptStore {
                    reason: "tree references free page",
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityReport {
    pub pages: u64,
    pub tree_pages: u64,
    pub free_pages: u64,
    pub records: u64,
    pub height: usize,
}
fn mark(seen: &mut [u64], id: u64, count: u64) -> Result<()> {
    if id < 2 || id >= count {
        return Err(Error::CorruptStore {
            reason: "page reference out of bounds",
        });
    }
    let word = &mut seen[(id / 64) as usize];
    let mask = 1u64 << (id % 64);
    if *word & mask != 0 {
        return Err(Error::CorruptStore {
            reason: "page cycle or duplicate ownership",
        });
    }
    *word |= mask;
    Ok(())
}
fn check_bounds(key: &[u8], low: Option<&[u8]>, high: Option<&[u8]>) -> Result<()> {
    if low.is_some_and(|v| key < v) || high.is_some_and(|v| key >= v) {
        return Err(Error::CorruptStore {
            reason: "key outside parent bounds",
        });
    }
    Ok(())
}
