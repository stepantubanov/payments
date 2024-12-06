use anyhow::{bail, Context};
use rust_decimal::Decimal;

use crate::{
    client::{ClientDb, ClientId},
    transaction::{TransactionDb, TransactionId},
};

mod client;
mod transaction;

// note: Ideally we don't want "dispute/resolve/chargeback" to have `amount` field. And
// we also want it to be non-optional for "deposit/withdrawal". This can be done with an
// enum, howevever I couldn't get it to work quickly with csv deserialiazer. Another option
// is to just have this type as serialize/deserialize intermediate type and build an enum
// from it (as fallible operation).
#[derive(Debug, serde::Deserialize)]
struct Operation {
    #[serde(rename = "type")]
    op_type: OperationType,
    client: ClientId,
    tx: TransactionId,
    amount: Option<Decimal>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum OperationType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

fn process_operation(
    clients: &mut ClientDb,
    transactions: &mut TransactionDb,
    operation: &Operation,
) -> anyhow::Result<()> {
    let client = clients.get_mut(operation.client);
    match operation.op_type {
        OperationType::Deposit => {
            let amount = operation.amount.context("no amount for deposit")?;
            transactions.deposit(operation.tx, amount)?;
            client.deposit(amount);
        }
        OperationType::Withdrawal => {
            let amount = operation.amount.context("no amount for withdrawal")?;
            client.check_withdrawal(amount)?;
            transactions.withdraw(operation.tx, amount)?;
            client.withdraw(amount);
        }
        OperationType::Dispute => {
            // We can validate dispute doesn't have `amount` specified.
            // ensure!(operation.amount.is_none(), "amount isn't expected for dispute");
            let amount = transactions.dispute(operation.tx)?;
            client.dispute_deposit(amount);
        }
        OperationType::Resolve => {
            // We can validate resolve doesn't have `amount` specified.
            // ensure!(operation.amount.is_none(), "amount isn't expected for resolve");
            let amount = transactions.resolve(operation.tx)?;
            client.resolve_dispute(amount);
        }
        OperationType::Chargeback => {
            // We can validate chargeback doesn't have `amount` specified.
            // ensure!(operation.amount.is_none(), "amount isn't expected for chargeback");
            let amount = transactions.chargeback(operation.tx)?;
            client.chargeback(amount);
        }
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct ClientRow {
    client: ClientId,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

fn main() -> anyhow::Result<()> {
    let Some(input_path) = std::env::args().nth(1) else {
        bail!("first arg should be input filename");
    };

    let mut clients = ClientDb::default();
    let mut transactions = TransactionDb::default();

    // buffered reading, does not load entire file
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .has_headers(true)
        .from_path(input_path)
        .unwrap();
    for (idx, result) in reader.deserialize().enumerate() {
        let operation: Operation = result.unwrap();
        if let Err(error) = process_operation(&mut clients, &mut transactions, &operation) {
            eprintln!("row #{idx}: {error}");
        }
    }
    drop(reader);

    // buffered writing
    let mut writer = csv::WriterBuilder::new().from_writer(std::io::stdout());
    writer.write_record(&["client", "available", "held", "total", "locked"])?;
    for (client, state) in clients.all() {
        writer.serialize(ClientRow {
            client,
            available: state.available(),
            held: state.held(),
            total: state.total(),
            locked: state.is_locked(),
        })?;
    }
    writer.flush()?;
    drop(writer);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chargeback() {
        let mut clients = ClientDb::default();
        let mut transactions = TransactionDb::default();

        // deposit 5.0
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Deposit,
                client: ClientId(123),
                tx: TransactionId(999),
                amount: Some(5.into()),
            },
        )
        .unwrap();

        // deposit 2.0
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Deposit,
                client: ClientId(123),
                tx: TransactionId(256),
                amount: Some(2.into()),
            },
        )
        .unwrap();

        // dispute
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Dispute,
                client: ClientId(123),
                tx: TransactionId(256),
                amount: None,
            },
        )
        .unwrap();

        let client = clients.get_mut(ClientId(123));
        assert_eq!(client.available(), Decimal::from(5));
        assert_eq!(client.held(), Decimal::from(2));
        assert_eq!(client.total(), Decimal::from(7));
        assert_eq!(client.is_locked(), false);

        // chargeback
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Chargeback,
                client: ClientId(123),
                tx: TransactionId(256),
                amount: None,
            },
        )
        .unwrap();

        let client = clients.get_mut(ClientId(123));
        assert_eq!(client.available(), Decimal::from(5));
        assert_eq!(client.held(), Decimal::from(0));
        assert_eq!(client.total(), Decimal::from(5));
        assert_eq!(client.is_locked(), true);
    }

    #[test]
    fn test_resolve() {
        let mut clients = ClientDb::default();
        let mut transactions = TransactionDb::default();

        // deposit 5.0
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Deposit,
                client: ClientId(123),
                tx: TransactionId(999),
                amount: Some(5.into()),
            },
        )
        .unwrap();

        // deposit 2.0
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Deposit,
                client: ClientId(123),
                tx: TransactionId(256),
                amount: Some(2.into()),
            },
        )
        .unwrap();

        // dispute
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Dispute,
                client: ClientId(123),
                tx: TransactionId(256),
                amount: None,
            },
        )
        .unwrap();

        let client = clients.get_mut(ClientId(123));
        assert_eq!(client.available(), Decimal::from(5));
        assert_eq!(client.held(), Decimal::from(2));
        assert_eq!(client.total(), Decimal::from(7));
        assert_eq!(client.is_locked(), false);

        // resolve
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Resolve,
                client: ClientId(123),
                tx: TransactionId(256),
                amount: None,
            },
        )
        .unwrap();

        let client = clients.get_mut(ClientId(123));
        assert_eq!(client.available(), Decimal::from(7));
        assert_eq!(client.held(), Decimal::from(0));
        assert_eq!(client.total(), Decimal::from(7));
        assert_eq!(client.is_locked(), false);
    }

    // todo: test error scenarios
}
