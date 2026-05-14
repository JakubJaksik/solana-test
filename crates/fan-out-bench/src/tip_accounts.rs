//! Static tip account lists per sender + RR rotator.
//!
//! Source: spec §5.2 — adresy zebrane z official docs każdego sendera.

use crate::config::SenderKind;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct TipAccountRotator {
    accounts: Vec<Pubkey>,
    cursor: AtomicUsize,
}

impl TipAccountRotator {
    pub fn new(accounts: Vec<Pubkey>) -> Self {
        Self {
            accounts,
            cursor: AtomicUsize::new(0),
        }
    }

    pub fn next(&self) -> Option<Pubkey> {
        if self.accounts.is_empty() {
            return None;
        }
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.accounts.len();
        Some(self.accounts[idx])
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
}

pub fn tip_accounts_for(kind: SenderKind) -> Vec<Pubkey> {
    let strs: &[&str] = match kind {
        SenderKind::Mock => &[],
        SenderKind::Helius => HELIUS,
        SenderKind::Jito | SenderKind::JitoBundle => JITO,
        SenderKind::Nozomi => NOZOMI,
        SenderKind::Syncro => SYNCRO,
        SenderKind::Astralane => ASTRALANE,
        SenderKind::Slot0 => SLOT0,
        SenderKind::AllenharkQuic | SenderKind::AllenharkHttps => ALLENHARK,
        SenderKind::Nextblock | SenderKind::NextblockQuic => NEXTBLOCK,
        SenderKind::Bloxroute => BLOXROUTE,
        SenderKind::BlockrazorHttp | SenderKind::BlockrazorGrpc => BLOCKRAZOR,
        SenderKind::Triton | SenderKind::Harmonic => &[],
    };
    strs.iter()
        .map(|s| Pubkey::from_str(s).expect("hardcoded tip pubkey must parse"))
        .collect()
}

const HELIUS: &[&str] = &[
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE",
    "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
    "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn",
    "2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD",
    "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ",
    "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF",
    "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT",
    "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey",
    "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or",
];

const JITO: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

const NOZOMI: &[&str] = &[
    "TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq",
    "noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4",
    "noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE",
    "noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo",
    "noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ",
    "nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L",
    "nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z",
    "nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu",
    "noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7",
    "nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP",
    "nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P",
    "nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge",
    "nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3",
    "nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ",
    "nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk",
    "nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne",
    "nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb",
];

const SYNCRO: &[&str] = &[
    "BPZrtYhdoAhiHWV5EgGLoV7bZFbMamBZurGDq4DmST8v",
    "7D5pdbkV75Sr73M1YFNZwXMed6DenwkdfbJwVWrX6drQ",
    "ELpn2NryEW4B3psG36eSjF45YcGMQpGGuu9J2AgAccbV",
    "FnckAPC9PitnRpGZM2M4WLwb3w9odRLJ7EDRZDngjvd6",
    "3ZnDTgvVfwzqwWoqAUmDkgVtXvXqjmeb5t9zxD5pMbmv",
    "3SLDFcdCzMbcFNguZhzmV4zqEAUvcPoKY13akpE4Tq1p",
    "48tT6LJqrsoFrLpzZSHkjGdGTWtsJ1PvjgWZjh8qF1RK",
    "7GM9fpVMHHcrK4cgzfVdzJvjiy1bSyfwSYzhxvgbfVLg",
    "CBd8GE3ffMJKf3iCCcNNBEifMxH1WpgtTzRnXPxxbjGE",
];

const ASTRALANE: &[&str] = &[
    "astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF",
    "astra4uejePWneqNaJKuFFA8oonqCE1sqF6b45kDMZm",
    "astra9xWY93QyfG6yM8zwsKsRodscjQ2uU2HKNL5prk",
    "astraRVUuTHjpwEVvNBeQEgwYx9w9CFyfxjYoobCZhL",
    "astraEJ2fEj8Xmy6KLG7B3VfbKfsHXhHrNdCQx7iGJK",
    "astraubkDw81n4LuutzSQ8uzHCv4BhPVhfvTcYv8SKC",
    "astraZW5GLFefxNPAatceHhYjfA1ciq9gvfEg2S47xk",
    "astrawVNP4xDBKT7rAdxrLYiTSTdqtUr63fSMduivXK",
];

const SLOT0: &[&str] = &[
    "6fQaVhYZA4w3MBSXjJ81Vf6W1EDYeUPXpgVQ6UQyU1Av",
    "4HiwLEP2Bzqj3hM2ENxJuzhcPCdsafwiet3oGkMkuQY4",
    "7toBU3inhmrARGngC7z6SjyP85HgGMmCTEwGNRAcYnEK",
    "8mR3wB1nh4D6J9RUCugxUpc6ya8w38LPxZ3ZjcBhgzws",
    "6SiVU5WEwqfFapRuYCndomztEwDjvS5xgtEof3PLEGm9",
    "TpdxgNJBWZRL8UXF5mrEsyWxDWx9HQexA9P1eTWQ42p",
    "D8f3WkQu6dCF33cZxuAsrKHrGsqGP2yvAHf8mX6RXnwf",
    "GQPFicsy3P3NXxB5piJohoxACqTvWE9fKpLgdsMduoHE",
    "Ey2JEr8hDkgN8qKJGrLf2yFjRhW7rab99HVxwi5rcvJE",
    "4iUgjMT8q2hNZnLuhpqZ1QtiV8deFPy2ajvvjEpKKgsS",
    "3Rz8uD83QsU8wKvZbgWAPvCNDU6Fy8TSZTMcPm3RB6zt",
    "DiTmWENJsHQdawVUUKnUXkconcpW4Jv52TnMWhkncF6t",
    "HRyRhQ86t3H4aAtgvHVpUJmw64BDrb61gRiKcdKUXs5c",
    "7y4whZmw388w1ggjToDLSBLv47drw5SUXcLk6jtmwixd",
    "J9BMEWFbCBEjtQ1fG5Lo9kouX1HfrKQxeUxetwXrifBw",
    "8U1JPQh3mVQ4F5jwRdFTBzvNRQaYFQppHQYoH38DJGSQ",
    "Eb2KpSC8uMt9GmzyAEm5Eb1AAAgTjRaXWFjKyFXHZxF3",
    "FCjUJZ1qozm1e8romw216qyfQMaaWKxWsuySnumVCCNe",
    "ENxTEjSQ1YabmUpXAdCgevnHQ9MHdLv8tzFiuiYJqa13",
    "6rYLG55Q9RpsPGvqdPNJs4z5WTxJVatMB8zV3WJhs5EK",
    "Cix2bHfqPcKcM233mzxbLk14kSggUUiz2A87fJtGivXr",
];

const ALLENHARK: &[&str] = &[
    "hark1zxc5Rz3K8Kquz79WPWFEgNCFeJnsMJ16f22uNP",
    "harkm2BTWxZuszoNpZnfe84jRbQTg6KGHaQBmWzDGQQ",
    "hark4CwtTnN2y9FaxjcFBAJdJqQrpouu5pgEixfqdEz",
    "harkoJfnM6dxrJydx5eVmDVwAgwC94KbhuxF69UbXwP",
    "hark6hUDUTekc1DGxWdJcuyDZwf6pJdCxd4SXAVtta6",
    "harkoTvFpKSrEQduYrNHXCurARVT19Ud3BnFhVxabos",
    "harkEpXoJv5qVzHaN7HSuUAd6PHjyMcFMcDYBMDJCEQ",
    "harkyXDdZSoJGyCxa24t2QXx1poPyp8YfghbtpzGSzK",
    "harkR2YJ4Dpt4UDJTcBirjnSPBhNpQFcoFkNpCkVqNk",
    "harkRBygM8pHYe4K8eBjfxyEX19oJn3LepFjvNbLbyi",
    "harkYFxB6DuUFNwDLvA5CQ66KpfRvFgUoVypMagNcmd",
];

const NEXTBLOCK: &[&str] = &[
    "NextbLoCkVtMGcV47JzewQdvBpLqT9TxQFozQkN98pE",
    "NexTbLoCkWykbLuB1NkjXgFWkX9oAtcoagQegygXXA2",
    "NeXTBLoCKs9F1y5PJS9CKrFNNLU1keHW71rfh7KgA1X",
    "NexTBLockJYZ7QD7p2byrUa6df8ndV2WSd8GkbWqfbb",
    "neXtBLock1LeC67jYd1QdAa32kbVeubsfPNTJC1V5At",
    "nEXTBLockYgngeRmRrjDV31mGSekVPqZoMGhQEZtPVG",
    "NEXTbLoCkB51HpLBLojQfpyVAMorm3zzKg7w9NFdqid",
    "nextBLoCkPMgmG8ZgJtABeScP35qLa2AMCNKntAP7Xc",
];

const BLOXROUTE: &[&str] = &[
    "HWEoBxYs7ssKuudEjzjmpfJVX7Dvi7wescFsVx2L5yoY",
    "95cfoy472fcQHaw4tPGBTKpn6ZQnfEPfBgDQx6gcRmRg",
    "3UQUKjhMKaY2S6bjcQD6yHB7utcZt5bfarRCmctpRtUd",
    "FogxVNs6Mm2w9rnGL1vkARSwJxvLE8mujTv3LK8RnUhF",
];

const BLOCKRAZOR: &[&str] = &[
    "Gywj98ophM7GmkDdaWs4isqZnDdFCW7B46TXmKfvyqSm",
    "FjmZZrFvhnqqb9ThCuMVnENaM3JGVuGWNyCAxRJcFpg9",
    "6No2i3aawzHsjtThw81iq1EXPJN6rh8eSJCLaYZfKDTG",
    "A9cWowVAiHe9pJfKAj3TJiN9VpbzMUq6E4kEvf5mUT22",
    "68Pwb4jS7eZATjDfhmTXgRJjCiZmw1L7Huy4HNpnxJ3o",
    "4ABhJh5rZPjv63RBJBuyWzBK3g9gWMUQdTZP2kiW31V9",
    "B2M4NG5eyZp5SBQrSdtemzk5TqVuaWGQnowGaCBt8GyM",
    "5jA59cXMKQqZAVdtopv8q3yyw9SYfiE3vUCbt7p8MfVf",
    "5YktoWygr1Bp9wiS1xtMtUki1PeYuuzuCF98tqwYxf61",
    "295Avbam4qGShBYK7E9H5Ldew4B3WyJGmgmXfiWdeeyV",
    "EDi4rSy2LZgKJX74mbLTFk4mxoTgT6F7HxxzG2HBAFyK",
    "BnGKHAC386n4Qmv9xtpBVbRaUTKixjBe3oagkPFKtoy6",
    "Dd7K2Fp7AtoN8xCghKDRmyqr5U169t48Tw5fEd3wT9mq",
    "AP6qExwrbRgBAVaehg4b5xHENX815sMabtBzUzVB4v8S",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_hardcoded_addresses_parse() {
        for kind in [
            SenderKind::Helius, SenderKind::Jito, SenderKind::Nozomi,
            SenderKind::Syncro, SenderKind::Astralane, SenderKind::Slot0,
            SenderKind::AllenharkQuic, SenderKind::Nextblock, SenderKind::Bloxroute,
            SenderKind::BlockrazorHttp,
        ] {
            let accounts = tip_accounts_for(kind);
            assert!(!accounts.is_empty(), "{:?} should have tip accounts", kind);
        }
    }

    #[test]
    fn triton_and_harmonic_have_no_vendor_tip() {
        assert!(tip_accounts_for(SenderKind::Triton).is_empty());
        assert!(tip_accounts_for(SenderKind::Harmonic).is_empty());
    }

    #[test]
    fn mock_has_no_tip_accounts() {
        assert!(tip_accounts_for(SenderKind::Mock).is_empty());
    }

    #[test]
    fn rotator_cycles_in_rr() {
        use solana_sdk::pubkey::Pubkey;
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let c = Pubkey::new_unique();
        let rotator = TipAccountRotator::new(vec![a, b, c]);
        assert_eq!(rotator.next(), Some(a));
        assert_eq!(rotator.next(), Some(b));
        assert_eq!(rotator.next(), Some(c));
        assert_eq!(rotator.next(), Some(a));
    }

    #[test]
    fn rotator_empty_returns_none() {
        let rotator = TipAccountRotator::new(vec![]);
        assert_eq!(rotator.next(), None);
    }

    #[test]
    fn expected_counts_match_spec() {
        assert_eq!(tip_accounts_for(SenderKind::Helius).len(), 10);
        assert_eq!(tip_accounts_for(SenderKind::Jito).len(), 8);
        assert_eq!(tip_accounts_for(SenderKind::Nozomi).len(), 17);
        assert_eq!(tip_accounts_for(SenderKind::Syncro).len(), 9);
        assert_eq!(tip_accounts_for(SenderKind::Astralane).len(), 8);
        assert_eq!(tip_accounts_for(SenderKind::Slot0).len(), 21);
        assert_eq!(tip_accounts_for(SenderKind::AllenharkQuic).len(), 11);
        assert_eq!(tip_accounts_for(SenderKind::Nextblock).len(), 8);
        assert_eq!(tip_accounts_for(SenderKind::Bloxroute).len(), 4);
        assert_eq!(tip_accounts_for(SenderKind::BlockrazorHttp).len(), 14);
    }
}
