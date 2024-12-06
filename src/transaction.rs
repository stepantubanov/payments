use std::collections::{hash_map::Entry, HashMap};

use anyhow::{anyhow, ensure, Context};
use rust_decimal::Decimal;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TransactionId(pub(crate) u32);

/// Current state of a transaction.
#[derive(Debug)]
pub(crate) enum TransactionStatus {
    Deposited,
    Withdrawn,
    Disputed,
    Resolved,
    Chargedback,
}

#[derive(Debug)]
pub(crate) struct TransactionState {
    amount: Decimal,
    status: TransactionStatus,
}

#[derive(Default)]
pub(crate) struct TransactionDb {
    transactions: HashMap<TransactionId, TransactionState>,
}

impl TransactionDb {
    pub(crate) fn deposit(
        &mut self,
        transaction_id: TransactionId,
        amount: Decimal,
    ) -> anyhow::Result<()> {
        match self.transactions.entry(transaction_id) {
            Entry::Occupied(_) => Err(anyhow!("transaction already exists")),
            Entry::Vacant(entry) => {
                entry.insert_entry(TransactionState {
                    amount,
                    status: TransactionStatus::Deposited,
                });
                Ok(())
            }
        }
    }

    pub(crate) fn withdraw(
        &mut self,
        transaction_id: TransactionId,
        amount: Decimal,
    ) -> anyhow::Result<()> {
        match self.transactions.entry(transaction_id) {
            Entry::Occupied(_) => Err(anyhow!("transaction already exists")),
            Entry::Vacant(entry) => {
                entry.insert_entry(TransactionState {
                    amount,
                    status: TransactionStatus::Withdrawn,
                });
                Ok(())
            }
        }
    }

    /// Returns disputed amount.
    pub(crate) fn dispute(&mut self, transaction_id: TransactionId) -> anyhow::Result<Decimal> {
        let state = self
            .transactions
            .get_mut(&transaction_id)
            .context("transaction does not exist")?;

        // note: If we want to be able to dispute the same transaction after it's been resolved, then
        // need to match against `TransactionStatus::Resolved` too.
        ensure!(
            matches!(state.status, TransactionStatus::Deposited),
            "transaction ({:?}) can't be disputed",
            state.status
        );
        state.status = TransactionStatus::Disputed;
        Ok(state.amount)
    }

    /// Returns resolved amount.
    pub(crate) fn resolve(&mut self, transaction_id: TransactionId) -> anyhow::Result<Decimal> {
        let state = self
            .transactions
            .get_mut(&transaction_id)
            .context("transaction does not exist")?;

        ensure!(
            matches!(state.status, TransactionStatus::Disputed),
            "transaction ({:?}) isn't under dispute",
            state.status
        );
        state.status = TransactionStatus::Resolved;
        Ok(state.amount)
    }

    /// Returns amount charged back.
    pub(crate) fn chargeback(&mut self, transaction_id: TransactionId) -> anyhow::Result<Decimal> {
        let state = self
            .transactions
            .get_mut(&transaction_id)
            .context("transaction does not exist")?;

        ensure!(
            matches!(state.status, TransactionStatus::Disputed),
            "transaction ({:?}) isn't under dispute",
            state.status
        );
        state.status = TransactionStatus::Chargedback;
        Ok(state.amount)
    }
}
