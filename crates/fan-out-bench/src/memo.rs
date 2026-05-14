//! Memo program payload encoding for sender attribution.
//!
//! SPL Memo program validates UTF-8. We use 1 byte from ASCII printable range
//! (0x21..0x7E, '!' to '~') to encode sender_id 0..93. Decoder: byte - b'!'.

const BASE: u8 = b'!';
const MAX_SENDER_ID: u8 = b'~' - BASE; // = 93

#[derive(Debug, thiserror::Error)]
pub enum MemoError {
    #[error("sender_id {0} exceeds max 93")]
    SenderIdTooLarge(u8),
    #[error("memo byte {0:#x} out of valid range 0x21..0x7E")]
    InvalidMemoByte(u8),
    #[error("memo data must be exactly 1 byte, got {0}")]
    WrongLength(usize),
}

pub fn encode(sender_id: u8) -> Result<[u8; 1], MemoError> {
    if sender_id > MAX_SENDER_ID {
        return Err(MemoError::SenderIdTooLarge(sender_id));
    }
    Ok([BASE + sender_id])
}

pub fn decode(memo: &[u8]) -> Result<u8, MemoError> {
    if memo.len() != 1 {
        return Err(MemoError::WrongLength(memo.len()));
    }
    let byte = memo[0];
    if !(b'!'..=b'~').contains(&byte) {
        return Err(MemoError::InvalidMemoByte(byte));
    }
    Ok(byte - BASE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip_full_range() {
        for sender_id in 0..=MAX_SENDER_ID {
            let memo = encode(sender_id).unwrap();
            assert_eq!(memo.len(), 1);
            let decoded = decode(&memo).unwrap();
            assert_eq!(decoded, sender_id);
        }
    }

    #[test]
    fn encode_zero_is_exclamation_mark() {
        assert_eq!(encode(0).unwrap(), [b'!']);
    }

    #[test]
    fn encode_max_is_tilde() {
        assert_eq!(encode(MAX_SENDER_ID).unwrap(), [b'~']);
    }

    #[test]
    fn encode_rejects_too_large() {
        assert!(matches!(encode(94), Err(MemoError::SenderIdTooLarge(94))));
        assert!(matches!(encode(255), Err(MemoError::SenderIdTooLarge(255))));
    }

    #[test]
    fn decode_rejects_wrong_length() {
        assert!(matches!(decode(&[]), Err(MemoError::WrongLength(0))));
        assert!(matches!(decode(b"!!"), Err(MemoError::WrongLength(2))));
    }

    #[test]
    fn decode_rejects_out_of_range() {
        assert!(matches!(decode(&[0x20]), Err(MemoError::InvalidMemoByte(0x20)))); // space
        assert!(matches!(decode(&[0x7F]), Err(MemoError::InvalidMemoByte(0x7F)))); // DEL
        assert!(matches!(decode(&[0xFF]), Err(MemoError::InvalidMemoByte(0xFF))));
    }

    #[test]
    fn all_encoded_bytes_are_valid_utf8() {
        for sender_id in 0..=MAX_SENDER_ID {
            let memo = encode(sender_id).unwrap();
            assert!(std::str::from_utf8(&memo).is_ok(), "sender_id {} produced invalid utf8", sender_id);
        }
    }
}
