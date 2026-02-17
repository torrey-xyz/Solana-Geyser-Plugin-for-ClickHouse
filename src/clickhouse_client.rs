use std::fmt;

use clickhouse::{Client, Row};
use serde::Serialize;
use thiserror::Error;

#[derive(Clone)]
pub struct ClickhouseConnection {
    pub client: Client,
    database: String,
    table: String,
}

impl fmt::Debug for ClickhouseConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickhouseConnection")
            .field("database", &self.database)
            .field("table", &self.table)
            .finish()
    }
}

#[derive(Row, Serialize)]
pub struct AccountRow {
    pub slot: u64,
    pub pubkey: String,
    pub owner: String,
    pub lamports: u64,
    pub executable: u8,
    pub rent_epoch: u64,
    pub data: String,
    pub updated_at_unix_ms: i64,
    pub txn_signature: Option<String>,
    pub write_version: u64,
}

impl ClickhouseConnection {
    pub fn new(clickhouse_url: &str, database: &str, table: &str) -> Self {
        let client = Client::default()
            .with_url(clickhouse_url)
            .with_database(database);

        Self {
            client,
            database: database.to_string(),
            table: table.to_string(),
        }
    }

    pub async fn ensure_schema(&self) -> Result<(), ClickhouseError> {
        let create_db = format!("CREATE DATABASE IF NOT EXISTS {}", self.database);
        self.client.query(&create_db).execute().await?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (
                slot UInt64,
                pubkey String,
                owner String,
                lamports UInt64,
                executable UInt8,
                rent_epoch UInt64,
                data String,
                updated_at_unix_ms Int64,
                txn_signature Nullable(String),
                write_version UInt64
            ) ENGINE = ReplacingMergeTree(write_version)
            ORDER BY (pubkey, slot)",
            self.database, self.table
        );
        self.client.query(&create_table).execute().await?;
        Ok(())
    }

    pub async fn insert_accounts(&self, rows: &[AccountRow]) -> Result<(), ClickhouseError> {
        if rows.is_empty() {
            return Ok(());
        }

        let table = format!("{}.{}", self.database, self.table);
        let mut insert = self.client.insert::<AccountRow>(&table).await?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ClickhouseError {
    #[error("clickhouse operation failed: {0}")]
    Operation(#[from] clickhouse::error::Error),
}