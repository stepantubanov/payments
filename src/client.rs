use std::collections::HashMap;

use anyhow::ensure;
use rust_decimal::Decimal;

#[derive(Default, Debug)]
pub(crate) struct AccountState {
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

impl AccountState {
    pub(crate) fn deposit(&mut self, amount: Decimal) {
        self.available += amount;
        self.total += amount;
    }

    pub(crate) fn check_withdrawal(&self, amount: Decimal) -> anyhow::Result<()> {
        // This was not mentioned in the requirements (forbid withdrawals for locked accounts), but seems like it would make sense.
        // ensure!(!self.locked, "account is locked");

        // This is directly from requirements.
        ensure!(self.available >= amount, "not enough funds");
        Ok(())
    }

    pub(crate) fn withdraw(&mut self, amount: Decimal) {
        self.available -= amount;
        self.total -= amount;
    }

    pub(crate) fn dispute_deposit(&mut self, amount: Decimal) {
        self.available -= amount;
        self.held += amount;
    }

    pub(crate) fn resolve_dispute(&mut self, amount: Decimal) {
        self.held -= amount;
        self.available += amount;
    }

    pub(crate) fn chargeback(&mut self, amount: Decimal) {
        self.held -= amount;
        self.total -= amount;
        self.locked = true;
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
    clients: HashMap<ClientId, AccountState>,
}

impl ClientDb {
    pub(crate) fn get_mut(&mut self, client_id: ClientId) -> &mut AccountState {
        self.clients.entry(client_id).or_default()
    }

    pub(crate) fn all(&self) -> impl Iterator<Item = (ClientId, &AccountState)> + use<'_> {
        self.clients.iter().map(|(id, state)| (*id, state))
    }
}
