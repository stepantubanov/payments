use anyhow::{ensure, Context};
use indexmap::IndexMap;
use rust_decimal::Decimal;

use crate::transaction::{
    Chargeback, Deposit, Dispute, PersistedTx, Resolve, TransactionId, Withdrawal,
};

#[derive(Default, Debug)]
pub(crate) struct AccountState {
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

pub(crate) struct AuthorizedWithdrawal {
    transaction_id: TransactionId,
    amount: Decimal,
}

impl AuthorizedWithdrawal {
    pub(crate) fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    pub(crate) fn amount(&self) -> &Decimal {
        &self.amount
    }
}

impl AccountState {
    pub(crate) fn deposit(&mut self, deposit: PersistedTx<Deposit>) -> anyhow::Result<()> {
        let new_available = self
            .available
            .checked_add(deposit.amount())
            .context("available amount overflow")?;
        let new_total = self
            .total
            .checked_add(deposit.amount())
            .context("total amount overflow")?;

        // note: Only update after both calculations succeeded.
        self.available = new_available;
        self.total = new_total;
        Ok(())
    }

    pub(crate) fn authorize_withdrawal(
        &self,
        transaction_id: TransactionId,
        amount: Decimal,
    ) -> anyhow::Result<AuthorizedWithdrawal> {
        ensure!(amount > Decimal::ZERO, "withdrawal amount should be > 0");

        // This was not mentioned in the requirements (forbid withdrawals for locked accounts), but seems like it would make sense.
        // ensure!(!self.locked, "account is locked");

        // This is directly from requirements.
        ensure!(self.available >= amount, "not enough funds");
        Ok(AuthorizedWithdrawal {
            transaction_id,
            amount,
        })
    }

    pub(crate) fn withdraw(&mut self, withdrawal: PersistedTx<Withdrawal>) -> anyhow::Result<()> {
        // note: available cannot be negative?
        let new_available = self
            .available
            .checked_sub(withdrawal.amount())
            .context("available amount underflow")?;
        let new_total = self
            .total
            .checked_sub(withdrawal.amount())
            .context("total amount underflow")?;

        self.available = new_available;
        self.total = new_total;
        Ok(())
    }

    pub(crate) fn dispute_deposit(&mut self, disputed: PersistedTx<Dispute>) -> anyhow::Result<()> {
        // note: available cannot be negative?
        self.available = self
            .available
            .checked_sub(disputed.amount())
            .context("available amount underflow")?;
        self.held = self
            .held
            .checked_add(disputed.amount())
            .context("held amount overflow")?;
        Ok(())
    }

    pub(crate) fn resolve_dispute(&mut self, resolved: PersistedTx<Resolve>) -> anyhow::Result<()> {
        // note: held cannot be negative?
        let new_held = self
            .held
            .checked_sub(resolved.amount())
            .context("held amount underflow")?;
        let new_available = self
            .available
            .checked_add(resolved.amount())
            .context("available amount overflow")?;

        self.held = new_held;
        self.available = new_available;
        Ok(())
    }

    pub(crate) fn chargeback(
        &mut self,
        chargedback: PersistedTx<Chargeback>,
    ) -> anyhow::Result<()> {
        // note: held cannot be negative?
        let new_held = self
            .held
            .checked_sub(chargedback.amount())
            .context("held amount underflow")?;
        let new_total = self
            .total
            .checked_sub(chargedback.amount())
            .context("total amount underflow")?;

        self.held = new_held;
        self.total = new_total;
        self.locked = true;
        Ok(())
    }

    pub(crate) fn available(&self) -> Decimal {
        self.available
    }

    pub(crate) fn held(&self) -> Decimal {
        self.held
    }

    pub(crate) fn total(&self) -> Decimal {
        self.total
    }

    pub(crate) fn is_locked(&self) -> bool {
        self.locked
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ClientId(pub(crate) u16);

#[derive(Default)]
pub(crate) struct ClientDb {
    clients: IndexMap<ClientId, AccountState>,
}

impl ClientDb {
    pub(crate) fn get_mut(&mut self, client_id: ClientId) -> &mut AccountState {
        self.clients.entry(client_id).or_default()
    }

    /// Clients are returned in deterministic order.
    pub(crate) fn all(&self) -> impl Iterator<Item = (ClientId, &AccountState)> + use<'_> {
        self.clients.iter().map(|(id, state)| (*id, state))
    }
}
