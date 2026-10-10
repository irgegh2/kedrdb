// SPDX-License-Identifier: Apache-2.0
//! Portable CRC32C (Castagnoli). Detects accidental corruption, not tampering.

const fn make_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = (value >> 1) ^ if value & 1 == 1 { 0x82f6_3b78 } else { 0 };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}
const TABLE: [u32; 256] = make_table();

const fn make_slices() -> [[u32; 256]; 8] {
    let mut slices = [TABLE; 8];
    let mut slice = 1;
    while slice < 8 {
        let mut index = 0;
        while index < 256 {
            let value = slices[slice - 1][index];
            slices[slice][index] = (value >> 8) ^ TABLE[(value & 255) as usize];
            index += 1;
        }
        slice += 1;
    }
    slices
}
static SLICES: [[u32; 256]; 8] = make_slices();

pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut value = u32::MAX;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let first = value ^ u32::from_le_bytes(chunk[..4].try_into().expect("chunk width"));
        value = SLICES[7][(first & 255) as usize]
            ^ SLICES[6][((first >> 8) & 255) as usize]
            ^ SLICES[5][((first >> 16) & 255) as usize]
            ^ SLICES[4][(first >> 24) as usize]
            ^ SLICES[3][chunk[4] as usize]
            ^ SLICES[2][chunk[5] as usize]
            ^ SLICES[1][chunk[6] as usize]
            ^ SLICES[0][chunk[7] as usize];
    }
    for byte in chunks.remainder() {
        value = (value >> 8) ^ TABLE[((value as u8) ^ byte) as usize];
    }
    !value
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn chunked_crc_matches_independent_bitwise_reference_at_all_boundaries() {
        fn reference(bytes: &[u8]) -> u32 {
            let mut value = u32::MAX;
            for byte in bytes {
                value ^= u32::from(*byte);
                for _ in 0..8 {
                    value = (value >> 1) ^ if value & 1 == 1 { 0x82f6_3b78 } else { 0 };
                }
            }
            !value
        }
        let data: Vec<u8> = (0..32784).map(|i| ((i * 73 + i / 7) & 255) as u8).collect();
        for offset in 0..8 {
            for len in
                (0..=260).chain([511, 512, 513, 8191, 8192, 8193, 16383, 16384, 16385, 32768])
            {
                let bytes = &data[offset..offset + len];
                assert_eq!(
                    crc32c(bytes),
                    reference(bytes),
                    "offset={offset}, len={len}"
                );
            }
        }
    }

    #[test]
    fn standard_crc32c_vectors() {
        // RFC 3720 B.4 numerical vectors, also verified by Google's CRC32C tests:
        // https://github.com/google/crc32c/blob/main/src/crc32c_unittest.cc
        assert_eq!(crc32c(&[0; 32]), 0x8a91_36aa);
        assert_eq!(crc32c(&[0xff; 32]), 0x62a8_ab43);
        assert_eq!(
            crc32c(&std::array::from_fn::<_, 32, _>(|i| i as u8)),
            0x46dd_794e
        );
        assert_eq!(
            crc32c(&std::array::from_fn::<_, 32, _>(|i| 31 - i as u8)),
            0x113f_db5c
        );
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c(b""), 0);
    }
}
