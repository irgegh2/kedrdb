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

pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut value = u32::MAX;
    for byte in bytes {
        value = (value >> 8) ^ TABLE[((value as u8) ^ byte) as usize];
    }
    !value
}

#[cfg(test)]
mod tests {
    use super::crc32c;

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
