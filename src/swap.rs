//! Uniswap V3 exactInputSingle calldata builder + ping-pong direction state.

use crate::config::SwapConfig;
use alloy::primitives::{
    Address, U256,
    aliases::{U24, U160},
};
use alloy::sol;
use alloy::sol_types::SolCall;
use std::sync::atomic::{AtomicU8, Ordering};
use thiserror::Error;

sol! {
    #[allow(missing_docs)]
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(ExactInputSingleParams params) external payable returns (uint256 amountOut);
}

/// Errors that can occur in the swap module.
#[derive(Debug, Error)]
pub enum SwapError {
    #[error("invalid address in config: {0}")]
    BadAddress(String),
    #[error("amount parse error: {0}")]
    BadAmount(String),
}

/// Direction of a swap in the ping-pong cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapDirection {
    AtoB,
    BtoA,
}

/// Atomic ping-pong direction state for alternating swaps.
pub struct PingPongState {
    state: AtomicU8,
}

impl PingPongState {
    /// Initialize state based on current token balances (legacy — only checks non-zero).
    /// Starts BtoA when only token B has balance; otherwise starts AtoB.
    ///
    /// Preferuj `initialize_with_amounts` żeby uniknąć reverta "insufficient balance"
    /// gdy niezerowy balance < wymagany amount_in.
    pub fn initialize(balance_a: U256, balance_b: U256) -> Self {
        let dir = if balance_a.is_zero() && !balance_b.is_zero() {
            1
        } else {
            0
        };
        Self {
            state: AtomicU8::new(dir),
        }
    }

    /// Initialize state sprawdzając czy balance STARCZY na planowany `amount_in`.
    /// Preferuje AtoB jeśli wallet ma dość tokena A. Fallback BtoA jeśli tylko B starczy.
    /// Jeśli żaden kierunek nie ma wystarczająco — zwraca None (caller powinien zabić run).
    pub fn initialize_with_amounts(
        balance_a: U256,
        amount_in_a: U256,
        balance_b: U256,
        amount_in_b: U256,
    ) -> Option<Self> {
        let a_ok = balance_a >= amount_in_a;
        let b_ok = balance_b >= amount_in_b;
        let dir = match (a_ok, b_ok) {
            (true, _) => 0,     // AtoB preferred
            (false, true) => 1, // BtoA fallback
            (false, false) => return None,
        };
        Some(Self {
            state: AtomicU8::new(dir),
        })
    }

    /// Return the current swap direction.
    pub fn current_direction(&self) -> SwapDirection {
        if self.state.load(Ordering::SeqCst) == 0 {
            SwapDirection::AtoB
        } else {
            SwapDirection::BtoA
        }
    }

    /// Flip the direction to the opposite side.
    pub fn advance(&self) {
        let cur = self.state.load(Ordering::SeqCst);
        self.state.store(1 - cur, Ordering::SeqCst);
    }
}

/// Builds `exactInputSingle` calldata for Uniswap V3 SwapRouter02.
pub struct SwapEncoder {
    token_a: Address,
    token_b: Address,
    fee: u32,
    amount_in_a: U256,
    amount_in_b: U256,
    recipient: Address,
}

impl SwapEncoder {
    /// Construct a new encoder from config and a recipient address.
    pub fn new(cfg: &SwapConfig, recipient: Address) -> Result<Self, SwapError> {
        let token_a: Address =
            cfg.token_a
                .parse()
                .map_err(|e: <Address as std::str::FromStr>::Err| {
                    SwapError::BadAddress(e.to_string())
                })?;
        let token_b: Address =
            cfg.token_b
                .parse()
                .map_err(|e: <Address as std::str::FromStr>::Err| {
                    SwapError::BadAddress(e.to_string())
                })?;
        let amount_in_a = U256::from_str_radix(&cfg.amount_in_a, 10)
            .map_err(|e| SwapError::BadAmount(e.to_string()))?;
        let amount_in_b = U256::from_str_radix(&cfg.amount_in_b, 10)
            .map_err(|e| SwapError::BadAmount(e.to_string()))?;
        Ok(Self {
            token_a,
            token_b,
            fee: cfg.pool_fee_tier,
            amount_in_a,
            amount_in_b,
            recipient,
        })
    }

    /// Encode an `exactInputSingle` call for the given direction.
    /// `_block_idx` is reserved for future deadline / nonce derivation.
    pub fn encode(&self, direction: SwapDirection, _block_idx: u64) -> Result<Vec<u8>, SwapError> {
        let (token_in, token_out, amount_in) = match direction {
            SwapDirection::AtoB => (self.token_a, self.token_b, self.amount_in_a),
            SwapDirection::BtoA => (self.token_b, self.token_a, self.amount_in_b),
        };
        let params = ExactInputSingleParams {
            tokenIn: token_in,
            tokenOut: token_out,
            fee: U24::from(self.fee),
            recipient: self.recipient,
            amountIn: amount_in,
            amountOutMinimum: U256::ZERO,
            sqrtPriceLimitX96: U160::ZERO,
        };
        Ok(exactInputSingleCall { params }.abi_encode())
    }
}
