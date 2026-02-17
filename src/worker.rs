use std::sync::Arc;
use tokio::{
    sync::mpsc::Receiver,
    time::{Duration, sleep, timeout}
};
use log::{error, info};
use thiserror::Error;

use crate::clickhouse_client::{AccountRow, ClickhouseConnection};

#[derive(Debug)]
pub struct AccountUpdate {
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

pub struct Worker {
    conn: Arc<ClickhouseConnection>,
    receiver: Receiver<AccountUpdate>,
    batch_size: usize,
    batch_timeout: Duration,
    max_retries: u32,
}

impl Worker {
    pub fn new(
        conn: Arc<ClickhouseConnection>,
        receiver: Receiver<AccountUpdate>,
        batch_size: usize,
        batch_timeout: Duration,
        max_retries: u32,
    ) -> Self {
        Self {
            conn,
            receiver,
            batch_size,
            batch_timeout,
            max_retries,
        }
    }

    pub async fn run(&mut self) {
        let mut batch = Vec::with_capacity(self.batch_size);

        loop {
            match timeout(self.batch_timeout, self.receiver.recv()).await {
                Ok(Some(update)) => {
                    batch.push(update);
                    if batch.len() >= self.batch_size {
                        self.process_batch(std::mem::take(&mut batch)).await;
                    }
                }
                Ok(None) => {
                    // Channel closed, process remaining items and exit
                    if !batch.is_empty() {
                        self.process_batch(std::mem::take(&mut batch)).await;
                    }
                    info!("Channel closed, worker shutting down");
                    break;
                }
                Err(_) => {
                    // Timeout reached, process current batch if any
                    if !batch.is_empty() {
                        self.process_batch(std::mem::take(&mut batch)).await;
                    }
                }
            }
        }
    }

    async fn process_batch(&self, batch: Vec<AccountUpdate>) {
        let mut retries = 0;
        while retries < self.max_retries {
            match self.insert_batch(&batch).await {
                Ok(_) => {
                    info!("Successfully inserted batch of {} records", batch.len());
                    break;
                }
                Err(e) => {
                    retries += 1;
                    error!(
                        "Batch insert failed (attempt {}/{}): {}",
                        retries, self.max_retries, e
                    );
                    if retries < self.max_retries {
                        sleep(Duration::from_secs(1 << retries)).await;
                    }
                }
            }
        }
    }

    async fn insert_batch(&self, batch: &[AccountUpdate]) -> Result<(), WorkerError> {
        let rows: Vec<AccountRow> = batch
            .iter()
            .map(|update| AccountRow {
                slot: update.slot,
                pubkey: update.pubkey.clone(),
                owner: update.owner.clone(),
                lamports: update.lamports,
                executable: update.executable,
                rent_epoch: update.rent_epoch,
                data: update.data.clone(),
                updated_at_unix_ms: update.updated_at_unix_ms,
                txn_signature: update.txn_signature.clone(),
                write_version: update.write_version,
            })
            .collect();

        self.conn.insert_accounts(&rows).await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("clickhouse error: {0}")]
    Clickhouse(#[from] crate::clickhouse_client::ClickhouseError),
}