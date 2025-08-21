use std::collections::{hash_map::Entry, HashMap};

use anyhow::{anyhow, ensure, Context};
use rust_decimal::Decimal;

use crate::client::AuthorizedWithdrawal;

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
    ) -> anyhow::Result<PersistedTx<Deposit>> {
        ensure!(amount > Decimal::ZERO, "deposit amount must be > 0");

        match self.transactions.entry(transaction_id) {
            Entry::Occupied(_) => Err(anyhow!("transaction already exists")),
            Entry::Vacant(entry) => {
                entry.insert_entry(TransactionState {
                    amount,
                    status: TransactionStatus::Deposited,
                });
                Ok(PersistedTx {
                    transaction_id,
                    amount,
                    state: Deposit,
                })
            }
        }
    }

    pub(crate) fn withdraw(
        &mut self,
        withdrawal: AuthorizedWithdrawal,
    ) -> anyhow::Result<PersistedTx<Withdrawal>> {
        match self.transactions.entry(withdrawal.transaction_id()) {
            Entry::Occupied(_) => Err(anyhow!("transaction already exists")),
            Entry::Vacant(entry) => {
                entry.insert_entry(TransactionState {
                    amount: *withdrawal.amount(),
                    status: TransactionStatus::Withdrawn,
                });
                Ok(PersistedTx {
                    transaction_id: withdrawal.transaction_id(),
                    amount: *withdrawal.amount(),
                    state: Withdrawal,
                })
            }
        }
    }

    /// Returns disputed amount.
    pub(crate) fn dispute(
        &mut self,
        transaction_id: TransactionId,
    ) -> anyhow::Result<PersistedTx<Dispute>> {
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
        Ok(PersistedTx {
            transaction_id,
            amount: state.amount,
            state: Dispute,
        })
    }

    /// Returns resolved amount.
    pub(crate) fn resolve(
        &mut self,
        transaction_id: TransactionId,
    ) -> anyhow::Result<PersistedTx<Resolve>> {
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
        Ok(PersistedTx {
            transaction_id,
            amount: state.amount,
            state: Resolve,
        })
    }

    /// Returns amount charged back.
    pub(crate) fn chargeback(
        &mut self,
        transaction_id: TransactionId,
    ) -> anyhow::Result<PersistedTx<Chargeback>> {
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
        Ok(PersistedTx {
            transaction_id,
            amount: state.amount,
            state: Chargeback,
        })
    }
}

/// note: each state type could have additional fields specific to that state.
#[allow(dead_code)]
pub(crate) struct PersistedTx<S> {
    transaction_id: TransactionId,
    amount: Decimal,
    state: S,
}

impl<S> PersistedTx<S> {
    pub(crate) fn amount(&self) -> Decimal {
        self.amount
    }
}

pub(crate) struct Deposit;
pub(crate) struct Withdrawal;
pub(crate) struct Dispute;
pub(crate) struct Resolve;
pub(crate) struct Chargeback;
