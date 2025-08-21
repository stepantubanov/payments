use std::{fs::File, io};

use anyhow::{bail, ensure, Context};
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
            let deposit = transactions.deposit(operation.tx, amount)?;
            client.deposit(deposit)?;
        }
        OperationType::Withdrawal => {
            let amount = operation.amount.context("no amount for withdrawal")?;
            let authorized_withdrawal = client.authorize_withdrawal(operation.tx, amount)?;
            let withdrawal = transactions.withdraw(authorized_withdrawal)?;
            client.withdraw(withdrawal)?;
        }
        OperationType::Dispute => {
            ensure!(
                operation.amount.is_none(),
                "amount isn't expected for dispute"
            );
            let disputed = transactions.dispute(operation.tx)?;
            client.dispute_deposit(disputed)?;
        }
        OperationType::Resolve => {
            ensure!(
                operation.amount.is_none(),
                "amount isn't expected for resolve"
            );
            let resolved = transactions.resolve(operation.tx)?;
            client.resolve_dispute(resolved)?;
        }
        OperationType::Chargeback => {
            ensure!(
                operation.amount.is_none(),
                "amount isn't expected for chargeback"
            );
            let chargedback = transactions.chargeback(operation.tx)?;
            client.chargeback(chargedback)?;
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

fn process_csv<R: io::Read, W: io::Write>(reader: R, writer: W) -> anyhow::Result<()> {
    let mut clients = ClientDb::default();
    let mut transactions = TransactionDb::default();

    // read & update client accounts
    {
        let mut reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .has_headers(true)
            .from_reader(reader);
        for (idx, result) in reader.deserialize().enumerate() {
            let operation: Operation = result.unwrap();
            if let Err(error) = process_operation(&mut clients, &mut transactions, &operation) {
                eprintln!("row #{idx}: {error}");
            }
        }
    }

    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(writer);
    for (client, state) in clients.all() {
        const DECIMAL_PLACES: u32 = 4;

        writer.serialize(ClientRow {
            client,
            available: state.available().round_dp(DECIMAL_PLACES),
            held: state.held().round_dp(DECIMAL_PLACES),
            total: state.total().round_dp(DECIMAL_PLACES),
            locked: state.is_locked(),
        })?;
    }
    writer.flush()?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let Some(input_path) = std::env::args().nth(1) else {
        bail!("first arg should be input filename");
    };

    process_csv(
        File::open(&input_path).with_context(|| format!("cannot open file '{input_path}'"))?,
        io::stdout(),
    )
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use similar_asserts::assert_eq;

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

    #[test]
    fn test_example() {
        const INPUT: &str = indoc! {"
            type, client, tx, amount
            deposit, 1, 1, 1.0
            deposit, 2, 2, 2.0
            deposit, 1, 3, 2.0
            withdrawal, 1, 4, 1.5
            withdrawal, 2, 5, 3.0
        "};

        const OUTPUT: &str = indoc! {"
            client,available,held,total,locked
            1,1.5,0,1.5,false
            2,2,0,2,false
        "};

        let mut output = Vec::new();
        process_csv(INPUT.as_bytes(), &mut output).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert_eq!(output, OUTPUT);
    }

    #[test]
    fn test_precision() {
        const INPUT: &str = indoc! {"
            type, client, tx, amount
            deposit, 1, 1, 1000.2303
            deposit, 1, 2, 2001.1533
        "};

        const OUTPUT: &str = indoc! {"
            client,available,held,total,locked
            1,3001.3836,0,3001.3836,false
        "};

        let mut output = Vec::new();
        process_csv(INPUT.as_bytes(), &mut output).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert_eq!(output, OUTPUT);
    }

    #[test]
    fn test_output_rounding() {
        const INPUT: &str = indoc! {"
            type, client, tx, amount
            deposit, 1, 1, 9.1333333
        "};

        const OUTPUT: &str = indoc! {"
            client,available,held,total,locked
            1,9.1333,0,9.1333,false
        "};

        let mut output = Vec::new();
        process_csv(INPUT.as_bytes(), &mut output).unwrap();

        let output = String::from_utf8(output).unwrap();
        assert_eq!(output, OUTPUT);
    }

    #[test]
    fn test_negative_deposit() {
        let mut clients = ClientDb::default();
        let mut transactions = TransactionDb::default();
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Deposit,
                client: ClientId(123),
                tx: TransactionId(999),
                amount: Some((-1_i32).into()),
            },
        )
        .unwrap_err();
    }

    #[test]
    fn test_invalid_dispute() {
        let mut clients = ClientDb::default();
        let mut transactions = TransactionDb::default();
        process_operation(
            &mut clients,
            &mut transactions,
            &Operation {
                op_type: OperationType::Dispute,
                client: ClientId(123),
                tx: TransactionId(999),
                amount: None,
            },
        )
        .unwrap_err();
    }
}
