// SPDX-License-Identifier: Apache-2.0
use crate::{
    Error, Result,
    page::PAGE_SIZE,
    storage::StorageIo,
    wal::{MAX_IMAGES, PageImage},
};

pub(super) struct PrivateIo<I: StorageIo> {
    pub base: I,
    pub images: Vec<PageImage>,
    pub length: u64,
    base_length: u64,
    limit: usize,
}
impl<I: StorageIo> PrivateIo<I> {
    pub fn new(base: I, length: u64, limit: usize) -> Result<Self> {
        if limit == 0 || limit > MAX_IMAGES {
            return Err(Error::InvalidArgument {
                message: "private page limit must be 1..256",
            });
        }
        Ok(Self {
            base,
            images: Vec::new(),
            length,
            base_length: length,
            limit,
        })
    }
}
impl<I: StorageIo> StorageIo for PrivateIo<I> {
    fn allows_wal_pages(&self) -> bool {
        true
    }
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        if offset >= self.length || buffer.is_empty() {
            return Ok(0);
        }
        let id = offset / PAGE_SIZE as u64;
        let inside = (offset % PAGE_SIZE as u64) as usize;
        let count = buffer
            .len()
            .min(PAGE_SIZE - inside)
            .min((self.length - offset).min(usize::MAX as u64) as usize);
        if let Some(image) = self.images.iter().find(|image| image.page_id == id) {
            buffer[..count].copy_from_slice(&image.bytes[inside..inside + count]);
            return Ok(count);
        }
        if offset < self.base_length {
            let count = count.min((self.base_length - offset).min(usize::MAX as u64) as usize);
            return self.base.read_at(offset, &mut buffer[..count]);
        }
        buffer[..count].fill(0);
        Ok(count)
    }
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
        if !offset.is_multiple_of(PAGE_SIZE as u64) || bytes.len() != PAGE_SIZE {
            return Err(Error::InvalidArgument {
                message: "private writes must be full aligned pages",
            });
        }
        let end = offset
            .checked_add(PAGE_SIZE as u64)
            .ok_or(Error::InvalidRange {
                offset,
                length: bytes.len(),
            })?;
        let id = offset / PAGE_SIZE as u64;
        if let Some(image) = self.images.iter_mut().find(|image| image.page_id == id) {
            image.bytes.copy_from_slice(bytes);
        } else {
            if self.images.len() == self.limit {
                return Err(Error::LimitExceeded {
                    limit: self.limit * std::mem::size_of::<PageImage>(),
                    requested: ((self.images.len() + 1) * std::mem::size_of::<PageImage>()) as u64,
                });
            }
            if self.images.len() == self.images.capacity() {
                let next = self
                    .images
                    .capacity()
                    .saturating_mul(2)
                    .max(4)
                    .min(self.limit);
                self.images
                    .try_reserve_exact(next - self.images.len())
                    .map_err(|_| Error::Allocation {
                        requested: next * std::mem::size_of::<PageImage>(),
                    })?;
            }
            self.images.push(PageImage {
                page_id: id,
                bytes: bytes.try_into().expect("page width"),
            });
        }
        self.length = self.length.max(end);
        Ok(bytes.len())
    }
    fn sync(&mut self) -> Result<()> {
        Ok(())
    } // Deliberately does not persist base.
    fn set_len(&mut self, length: u64) -> Result<()> {
        if !length.is_multiple_of(PAGE_SIZE as u64) {
            return Err(Error::InvalidArgument {
                message: "private length must be page aligned",
            });
        }
        self.length = length;
        self.images
            .retain(|image| image.page_id < length / PAGE_SIZE as u64);
        Ok(())
    }
    fn len(&mut self) -> Result<u64> {
        Ok(self.length)
    }
}
