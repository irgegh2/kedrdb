// SPDX-License-Identifier: Apache-2.0
use super::format::{Node, get16, get64, put16, put64};
use crate::{
    Error, Result,
    checksum::crc32c,
    page::PAGE_SIZE,
    storage::{StorageIo, read_exact_at, write_all_at},
};

#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub frames: usize,
    pub allocated_bytes: usize,
}
#[derive(Clone, PartialEq, Eq)]
struct Metadata {
    database_id: [u8; 16],
    generation: u64,
    root: u64,
    count: u64,
    free: u64,
}
impl Metadata {
    fn encode(&self) -> [u8; PAGE_SIZE] {
        let mut bytes = [0; PAGE_SIZE];
        bytes[..8].copy_from_slice(b"KEDRFILE");
        put16(&mut bytes, 8, 1);
        put16(&mut bytes, 10, 96);
        bytes[12..16].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        bytes[16..32].copy_from_slice(&self.database_id);
        put64(&mut bytes, 32, self.generation);
        put64(&mut bytes, 40, self.root);
        put64(&mut bytes, 48, self.count);
        put64(&mut bytes, 56, self.free);
        let checksum = crc32c(&bytes);
        bytes[80..84].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }
    fn decode(bytes: &[u8; PAGE_SIZE]) -> Result<Self> {
        let bad = |reason| Error::CorruptStore { reason };
        if &bytes[..8] != b"KEDRFILE"
            || get16(bytes, 8) != 1
            || get16(bytes, 10) != 96
            || u32::from_le_bytes(bytes[12..16].try_into().expect("width")) != PAGE_SIZE as u32
        {
            return Err(bad("file header version, magic or size"));
        }
        if bytes[64..80]
            .iter()
            .chain(bytes[84..].iter())
            .any(|b| *b != 0)
        {
            return Err(bad("file header reserved fields"));
        }
        let mut clean = *bytes;
        clean[80..84].fill(0);
        if crc32c(&clean) != u32::from_le_bytes(bytes[80..84].try_into().expect("width")) {
            return Err(bad("file header checksum"));
        }
        let metadata = Self {
            database_id: bytes[16..32].try_into().expect("width"),
            generation: get64(bytes, 32),
            root: get64(bytes, 40),
            count: get64(bytes, 48),
            free: get64(bytes, 56),
        };
        if metadata.database_id == [0; 16]
            || metadata.generation == 0
            || metadata.count < 3
            || metadata.root < 2
            || metadata.root >= metadata.count
            || metadata.free != 0 && (metadata.free < 2 || metadata.free >= metadata.count)
            || metadata.root == metadata.free
        {
            return Err(bad("file metadata bounds"));
        }
        Ok(metadata)
    }
}

struct Frame {
    id: Option<u64>,
    dirty: bool,
    pinned: bool,
    stamp: u64,
    bytes: [u8; PAGE_SIZE],
}
/// Bounded page cache. A pinned guard exclusively borrows the store and unpins
/// on drop; no I/O or implicit checkpoint is performed by Drop.
pub struct PageStore<I: StorageIo> {
    io: I,
    metadata: Metadata,
    active_header: u64,
    frames: Vec<Frame>,
    clock: u64,
    stats: CacheStats,
    poisoned: bool,
}
pub struct PageGuard<'a, I: StorageIo> {
    store: &'a mut PageStore<I>,
    at: usize,
}
impl<I: StorageIo> PageGuard<'_, I> {
    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.store.frames[self.at].bytes
    }
    pub fn page_count(&self) -> u64 {
        self.store.metadata.count
    }
}
impl<I: StorageIo> Drop for PageGuard<'_, I> {
    fn drop(&mut self) {
        self.store.frames[self.at].pinned = false;
    }
}

impl<I: StorageIo> PageStore<I> {
    fn frames(budget: usize) -> Result<Vec<Frame>> {
        let count = budget / std::mem::size_of::<Frame>();
        if count == 0 || count > 65536 {
            return Err(Error::InvalidArgument {
                message: "cache budget must fit 1..65536 page frames",
            });
        }
        let mut frames = Vec::new();
        frames
            .try_reserve_exact(count)
            .map_err(|_| Error::Allocation { requested: budget })?;
        for _ in 0..count {
            frames.push(Frame {
                id: None,
                dirty: false,
                pinned: false,
                stamp: 0,
                bytes: [0; PAGE_SIZE],
            });
        }
        Ok(frames)
    }
    pub fn create(mut io: I, database_id: [u8; 16], budget: usize) -> Result<Self> {
        if database_id == [0; 16] {
            return Err(Error::InvalidArgument {
                message: "database ID must be nonzero",
            });
        }
        let frames = Self::frames(budget)?;
        if io.len()? != 0 {
            return Err(Error::InvalidArgument {
                message: "new store requires empty exclusive storage",
            });
        }
        let metadata = Metadata {
            database_id,
            generation: 1,
            root: 2,
            count: 3,
            free: 0,
        };
        let bytes = metadata.encode();
        write_all_at(&mut io, 0, &bytes)?;
        write_all_at(&mut io, PAGE_SIZE as u64, &bytes)?;
        write_all_at(
            &mut io,
            2 * PAGE_SIZE as u64,
            &Node::Leaf(Vec::new()).encode(2)?,
        )?;
        io.sync()?;
        Ok(Self {
            io,
            metadata,
            active_header: 0,
            frames,
            clock: 0,
            stats: CacheStats::default(),
            poisoned: false,
        })
    }
    pub fn open(mut io: I, budget: usize) -> Result<Self> {
        let frames = Self::frames(budget)?;
        let length = io.len()?;
        if length < 3 * PAGE_SIZE as u64 || length % PAGE_SIZE as u64 != 0 {
            return Err(Error::CorruptStore {
                reason: "file length",
            });
        }
        let mut bytes0 = [0; PAGE_SIZE];
        let mut bytes1 = [0; PAGE_SIZE];
        read_exact_at(&mut io, 0, &mut bytes0)?;
        read_exact_at(&mut io, PAGE_SIZE as u64, &mut bytes1)?;
        let (metadata, active_header) = match (Metadata::decode(&bytes0), Metadata::decode(&bytes1))
        {
            (Ok(a), Ok(b)) => {
                if a.database_id != b.database_id || a.generation == b.generation && a != b {
                    return Err(Error::CorruptStore {
                        reason: "inconsistent metadata copies",
                    });
                }
                if a.generation >= b.generation {
                    (a, 0)
                } else {
                    (b, 1)
                }
            }
            (Ok(a), Err(_)) => (a, 0),
            (Err(_), Ok(b)) => (b, 1),
            (Err(_), Err(_)) => {
                return Err(Error::CorruptStore {
                    reason: "both metadata copies invalid",
                });
            }
        };
        if metadata.count.checked_mul(PAGE_SIZE as u64) != Some(length) {
            return Err(Error::CorruptStore {
                reason: "file length differs from checkpoint; recovery required",
            });
        }
        Ok(Self {
            io,
            metadata,
            active_header,
            frames,
            clock: 0,
            stats: CacheStats::default(),
            poisoned: false,
        })
    }
    pub fn database_id(&self) -> [u8; 16] {
        self.metadata.database_id
    }
    pub fn page_count(&self) -> u64 {
        self.metadata.count
    }
    pub(super) fn root(&self) -> u64 {
        self.metadata.root
    }
    pub(super) fn set_root(&mut self, id: u64) {
        self.metadata.root = id;
    }
    pub(super) fn free_head(&self) -> u64 {
        self.metadata.free
    }
    pub(super) fn poison(&mut self) {
        self.poisoned = true;
    }
    fn healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            frames: self.frames.len(),
            allocated_bytes: self.frames.capacity() * std::mem::size_of::<Frame>(),
            ..self.stats
        }
    }
    pub fn into_io(self) -> I {
        self.io
    }
    fn flush_frame(&mut self, at: usize) -> Result<()> {
        if self.frames[at].dirty {
            let offset = self.frames[at].id.expect("dirty frame has ID") * PAGE_SIZE as u64;
            if let Err(error) = write_all_at(&mut self.io, offset, &self.frames[at].bytes) {
                self.poison();
                return Err(error);
            }
            self.frames[at].dirty = false;
        }
        Ok(())
    }
    fn load(&mut self, id: u64) -> Result<usize> {
        self.healthy()?;
        if id < 2 || id >= self.metadata.count {
            return Err(Error::CorruptStore {
                reason: "cache page ID bounds",
            });
        }
        self.clock = self.clock.checked_add(1).ok_or(Error::InvalidArgument {
            message: "cache clock exhausted",
        })?;
        if let Some(at) = self.frames.iter().position(|frame| frame.id == Some(id)) {
            self.stats.hits += 1;
            self.frames[at].stamp = self.clock;
            return Ok(at);
        }
        self.stats.misses += 1;
        let at = self
            .frames
            .iter()
            .enumerate()
            .filter(|(_, frame)| !frame.pinned)
            .min_by_key(|(_, frame)| (frame.id.is_some(), frame.stamp))
            .map(|(at, _)| at)
            .ok_or(Error::InvalidArgument {
                message: "all cache frames pinned",
            })?;
        self.flush_frame(at)?;
        if self.frames[at].id.is_some() {
            self.stats.evictions += 1;
        }
        // Failed reads never leave a half-loaded frame associated with an ID.
        self.frames[at].id = None;
        read_exact_at(
            &mut self.io,
            id * PAGE_SIZE as u64,
            &mut self.frames[at].bytes,
        )?;
        self.frames[at].id = Some(id);
        self.frames[at].stamp = self.clock;
        Ok(at)
    }
    pub fn pin(&mut self, id: u64) -> Result<PageGuard<'_, I>> {
        let at = self.load(id)?;
        self.frames[at].pinned = true;
        Ok(PageGuard { store: self, at })
    }
    pub(super) fn write(&mut self, id: u64, bytes: &[u8; PAGE_SIZE]) -> Result<()> {
        let at = self.load(id)?;
        self.frames[at].bytes = *bytes;
        self.frames[at].dirty = true;
        Ok(())
    }
    pub(super) fn allocate(&mut self) -> Result<u64> {
        self.healthy()?;
        if self.metadata.free != 0 {
            let id = self.metadata.free;
            let next = {
                let guard = self.pin(id)?;
                match Node::decode(guard.bytes(), id, guard.page_count())? {
                    Node::Free(next) => next,
                    _ => {
                        return Err(Error::CorruptStore {
                            reason: "allocator encountered live page",
                        });
                    }
                }
            };
            self.metadata.free = next;
            return Ok(id);
        }
        let id = self.metadata.count;
        let count = id.checked_add(1).ok_or(Error::CorruptStore {
            reason: "page counter exhausted",
        })?;
        let length = count
            .checked_mul(PAGE_SIZE as u64)
            .ok_or(Error::CorruptStore {
                reason: "file size exhausted",
            })?;
        if let Err(error) = self.io.set_len(length) {
            self.poison();
            return Err(error);
        }
        self.metadata.count = count;
        Ok(id)
    }
    pub(super) fn free(&mut self, id: u64) -> Result<()> {
        self.healthy()?;
        if id == self.metadata.root {
            return Err(Error::CorruptStore {
                reason: "cannot free current root",
            });
        }
        let guard = self.pin(id)?;
        if matches!(
            Node::decode(guard.bytes(), id, guard.page_count())?,
            Node::Free(_)
        ) {
            return Err(Error::CorruptStore {
                reason: "double free",
            });
        }
        drop(guard);
        let bytes = Node::Free(self.metadata.free).encode(id)?;
        self.write(id, &bytes)?;
        self.metadata.free = id;
        Ok(())
    }
    pub fn checkpoint(&mut self) -> Result<()> {
        self.healthy()?;
        let result = (|| {
            let generation =
                self.metadata
                    .generation
                    .checked_add(1)
                    .ok_or(Error::CorruptStore {
                        reason: "generation exhausted",
                    })?;
            for at in 0..self.frames.len() {
                self.flush_frame(at)?;
            }
            self.io.sync()?;
            let mut next = self.metadata.clone();
            next.generation = generation;
            let inactive = 1 - self.active_header;
            write_all_at(&mut self.io, inactive * PAGE_SIZE as u64, &next.encode())?;
            self.io.sync()?;
            self.metadata = next;
            self.active_header = inactive;
            Ok(())
        })();
        if result.is_err() {
            self.poison();
        }
        result
    }
}
