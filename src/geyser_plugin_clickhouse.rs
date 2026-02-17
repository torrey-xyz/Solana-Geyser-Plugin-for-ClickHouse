use std::{
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::sync::mpsc::{self, Sender};
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, Result,
};
use solana_sdk::clock::Slot;
use log::{info, LevelFilter, Log};
use chrono::Utc;

use crate::{
    config::PluginConfig,
    clickhouse_client::ClickhouseConnection,
    worker::{AccountUpdate, Worker},
};

#[derive(Debug)]
struct ClickhousePlugin {
    conn: Option<Arc<ClickhouseConnection>>,
    config: PluginConfig,
    sender: Option<Sender<AccountUpdate>>,
    worker_handle: Option<JoinHandle<()>>,
}


impl Default for ClickhousePlugin {
    fn default() -> Self {
        Self {
            conn: None,
            config: PluginConfig::default(),
            sender: None,
            worker_handle: None,
        }
    }
}


impl GeyserPlugin for ClickhousePlugin {
    fn name(&self) -> &'static str {
        "Clickhouse Plugin"
    }

    #[allow(unused_variables)]
    fn setup_logger(&self, logger: &'static dyn Log, level: LevelFilter) -> Result<()> {
        log::set_max_level(level);
        if let Err(err) = log::set_logger(logger) {
            return Err(GeyserPluginError::Custom(Box::new(err)));
        }
        Ok(())
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> Result<()> {
        info!("ClickhousePlugin loaded with config file: {}", config_file);
        if self.sender.is_some() || self.worker_handle.is_some() {
            self.on_unload();
        }

        let config = PluginConfig::load(config_file)
            .map_err(|e| GeyserPluginError::ConfigFileReadError { msg: e.to_string() })?;

        let conn = Arc::new(ClickhouseConnection::new(
            &config.clickhouse_url,
            &config.clickhouse_database,
            &config.clickhouse_table,
        ));

        if config.create_schema {
            let rt =
                tokio::runtime::Runtime::new().map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
            rt.block_on(conn.ensure_schema())
                .map_err(|e| custom_error(format!("failed to initialize schema: {e}")))?;
        }

        self.conn = Some(conn.clone());
        self.config = config.clone();

        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        self.sender = Some(sender);

        // Spawn worker thread to handle batched inserts.
        let handle = thread::Builder::new()
            .name("clickhouse-worker".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

                let mut worker = Worker::new(
                    conn,
                    receiver,
                    config.batch_size,
                    Duration::from_millis(config.batch_timeout_ms),
                    config.max_retries,
                );
                runtime.block_on(async move {
                    worker.run().await;
                });
            });
        self.worker_handle = Some(handle.map_err(|e| GeyserPluginError::Custom(Box::new(e)))?);

        Ok(())
    }

    fn on_unload(&mut self) {
        self.sender.take();
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
        info!("Clickhouse Plugin unloaded")
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions<'_>,
        slot: Slot,
        _is_startup: bool,
    ) -> Result<()> {
        if self.conn.is_none() {
            return Err(custom_error("plugin is not initialized"));
        }

        if let ReplicaAccountInfoVersions::V0_0_3(account_info) = account {
            if let Some(sender) = &self.sender {
                let update = AccountUpdate {
                    pubkey: hex::encode(account_info.pubkey),
                    lamports: account_info.lamports,
                    owner: hex::encode(account_info.owner),
                    executable: if account_info.executable { 1 } else { 0 },
                    rent_epoch: account_info.rent_epoch,
                    data: hex::encode(account_info.data),
                    slot,
                    updated_at_unix_ms: Utc::now().timestamp_millis(),
                    txn_signature: None,
                    write_version: account_info.write_version,
                };

                sender
                    .try_send(update)
                    .map_err(|e| custom_error(format!("channel send error: {e}")))?;
            }
        } else {
            return Err(custom_error("unsupported replica account version"));
        }

        Ok(())
    }
    /// Check if the plugin is interested in account data
    /// Default is true -- if the plugin is not interested in
    /// account data, please return false.
    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    /// Check if the plugin is interested in transaction data
    /// Default is false -- if the plugin is interested in
    /// transaction data, please return true.
    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    /// Check if the plugin is interested in entry data
    /// Default is false -- if the plugin is interested in
    /// entry data, return true.
    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}



#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = ClickhousePlugin::default();
    let plugin = Box::new(plugin);
    Box::into_raw(plugin)
}

fn custom_error(msg: impl Into<String>) -> GeyserPluginError {
    GeyserPluginError::Custom(Box::new(std::io::Error::other(msg.into())))
}
