use std::{
    concat, env,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
};

use hashbrown::HashSet;
use indexer_rabbitmq::geyser::{
    BlockMetadataNotify, InstructionNotify, Message, SlotStatusNotify, TransactionNotify,
};
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfoV2, ReplicaAccountInfoV3, ReplicaBlockInfoVersions, SlotStatus,
};
use solana_program::{instruction::CompiledInstruction, message::AccountKeys};

// pub(crate) static TOKEN_KEY: Pubkey =
//     solana_program::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

use solana_sdk::transaction::SanitizedTransaction;

use solana_transaction_status::{
    ConfirmedTransactionWithStatusMeta, TransactionStatusMeta, TransactionWithStatusMeta,
    UiTransactionEncoding, VersionedTransactionWithStatusMeta,
};

use crate::{
    config::{ChainProgress, Config},
    convert,
    interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfo, ReplicaAccountInfoVersions,
        ReplicaTransactionInfoVersions, Result,
    },
    metrics::{Counter, Metrics},
    prelude::*,
    selectors::{AccountSelector, InstructionSelector, TransactionSelector},
    sender::Sender,
    stats::{Stats, StatsRequest},
};

const UNINIT: &str = "RabbitMQ plugin not initialized yet!";

#[inline]
#[allow(clippy::needless_lifetimes)]
fn custom_err<'a, E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>>(
    counter: &'a Counter,
) -> impl FnOnce(E) -> GeyserPluginError + 'a {
    |e| {
        counter.log(1);
        GeyserPluginError::Custom(e.into())
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    rt: Arc<tokio::runtime::Runtime>,
    producer: Arc<Sender>,
    acct_sel: AccountSelector,
    ins_sel: InstructionSelector,
    tx_sel: TransactionSelector,
    metrics: Arc<Metrics>,
    chain_progress: ChainProgress,
    stats_sender: mpsc::SyncSender<StatsRequest>,
}

impl Inner {
    pub fn spawn<F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static>(
        self: &Arc<Self>,
        f: impl FnOnce(Arc<Self>) -> F,
    ) {
        self.rt.spawn(f(Arc::clone(self)));
    }
}

/// An instance of the plugin
#[derive(Debug, Default)]
#[repr(transparent)]
pub struct GeyserPluginRabbitMq(Option<Arc<Inner>>);

impl GeyserPluginRabbitMq {
    fn load_token_reg() -> HashSet<Pubkey> {
        HashSet::new()
    }

    fn expect_inner(&self) -> &Arc<Inner> {
        self.0.as_ref().expect(UNINIT)
    }

    #[inline]
    fn with_inner<T>(
        &self,
        uninit: impl FnOnce() -> GeyserPluginError,
        f: impl FnOnce(&Arc<Inner>) -> anyhow::Result<T>,
    ) -> Result<T> {
        match self.0 {
            Some(ref inner) => f(inner).map_err(custom_err(&inner.metrics.errs)),
            None => Err(uninit()),
        }
    }
}

impl GeyserPlugin for GeyserPluginRabbitMq {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, cfg: &str) -> Result<()> {
        solana_logger::setup_with_default("info");

        info!("Plugin loading");

        let metrics = Metrics::new_rc();

        let version;
        let host;

        {
            let ver = env!("CARGO_PKG_VERSION");
            let git = option_env!("META_GIT_HEAD");
            // TODO
            // let rem = option_env!("META_GIT_REMOTE");

            {
                use std::fmt::Write;

                let mut s = format!("v{ver}");

                if let Some(git) = git {
                    write!(s, "+git.{git}").unwrap();
                }

                version = s;
            }

            // TODO
            // let rustc_ver = env!("META_RUSTC_VERSION");
            // let build_host = env!("META_BUILD_HOST");
            // let target = env!("META_BUILD_TARGET");
            // let profile = env!("META_BUILD_PROFILE");
            // let platform = env!("META_BUILD_PLATFORM");

            host = hostname::get()
                .map_err(custom_err(&metrics.errs))?
                .into_string()
                .map_err(|_| anyhow!("Failed to parse system hostname"))
                .map_err(custom_err(&metrics.errs))?;
        }

        let (amqp, jobs, metrics_conf, chain_progress, mut acct_sel, ins_sel, tx_sel) =
            Config::read(cfg)
                .and_then(Config::into_parts)
                .map_err(custom_err(&metrics.errs))?;

        let startup_type = acct_sel.startup();

        if let Some(config) = metrics_conf.config {
            const VAR: &str = "SOLANA_METRICS_CONFIG";

            if env::var_os(VAR).is_some() {
                warn!("Overriding existing value for {}", VAR);
            }

            env::set_var(VAR, config);
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
                format!("solGeyserRmq{id:02}")
            })
            .worker_threads(jobs.limit)
            .max_blocking_threads(jobs.blocking.unwrap_or(jobs.limit))
            .build()
            .map_err(custom_err(&metrics.errs))?;
        let rt = Arc::new(rt);

        let s_producer = rt.block_on(async {
            let producer = Sender::new(
                amqp,
                format!("geyser-rabbitmq-{version}@{host}"),
                startup_type,
                Arc::clone(&metrics),
            )
            .await
            .map_err(custom_err(&metrics.errs))?;

            if acct_sel.screen_tokens() {
                acct_sel.init_tokens(Self::load_token_reg());
            }

            Result::<_>::Ok(producer)
        })?;
        let producer = Arc::new(s_producer);

        //create the stats processor
        let stats_sender = Stats::create_publisher(producer.clone(), rt.clone());

        self.0 = Some(Arc::new(Inner {
            rt,
            producer,
            acct_sel,
            ins_sel,
            tx_sel,
            metrics,
            chain_progress,
            stats_sender,
        }));

        info!("Plugin loaded");

        Ok(())
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::AccountsUpdateError { msg: UNINIT.into() },
            |this| {
                let (pubkey, owner) = match account {
                    ReplicaAccountInfoVersions::V0_0_1(acct) => {
                        let ReplicaAccountInfo { pubkey, owner, .. } = *acct;
                        (pubkey, owner)
                    },
                    ReplicaAccountInfoVersions::V0_0_2(acct) => {
                        let ReplicaAccountInfoV2 { pubkey, owner, .. } = *acct;
                        (pubkey, owner)
                    },
                    ReplicaAccountInfoVersions::V0_0_3(acct) => {
                        let ReplicaAccountInfoV3 { pubkey, owner, .. } = *acct;
                        (pubkey, owner)
                    },
                };

                if let Some(route) = this.acct_sel.get_route(owner, pubkey, is_startup) {
                    let acct = convert::create_account_update(&account, slot, is_startup);
                    let route = route.clone();
                    this.spawn(|this| async move {
                        this.producer
                            .send(Message::AccountUpdate(acct), route.as_str())
                            .await;
                        this.metrics.sends.log(1);
                        Ok(())
                    });
                }

                Ok(())
            },
        )
    }

    #[allow(clippy::too_many_lines)]
    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        #[inline]
        fn process_instruction<'a>(
            sel: &'a InstructionSelector,
            ins: &CompiledInstruction,
            keys: &AccountKeys,
            slot: u64,
        ) -> anyhow::Result<Option<(Message, &'a Arc<String>)>> {
            let program = *keys
                .get(ins.program_id_index as usize)
                .ok_or_else(|| anyhow!("Couldn't get program ID for instruction"))?;

            match sel.get_route(&program, ins) {
                None => Ok(None),
                Some(route) => {
                    let accounts = ins
                        .accounts
                        .iter()
                        .map(|i| {
                            keys.get(*i as usize).map_or_else(
                                || Err(anyhow!("Couldn't get input account for instruction")),
                                |k| Ok(*k),
                            )
                        })
                        .collect::<StdResult<Vec<_>, _>>()?;

                    let data = ins.data.clone();

                    Ok(Some((
                        Message::InstructionNotify(InstructionNotify {
                            program,
                            data,
                            accounts,
                            slot,
                            block_time: None,
                        }),
                        route,
                    )))
                },
            }
        }

        #[inline]
        fn process_transaction<'a>(
            sel: &'a TransactionSelector,
            stx: &SanitizedTransaction,
            meta: &TransactionStatusMeta,
            slot: u64,
        ) -> anyhow::Result<Option<(Message, &'a Arc<String>)>> {
            match sel.get_route(stx, meta) {
                None => Ok(None),
                Some(route) => {
                    //compress the meta
                    let mut compressor = zstd::bulk::Compressor::new(2).unwrap();
                    let pre_datum_compressed = meta.pre_datum.as_ref().map(|all_datums| {
                        all_datums
                            .iter()
                            .map(|data| {
                                data.as_ref().map(|some_data| {
                                    if some_data.is_empty() {
                                        some_data.clone()
                                    } else {
                                        compressor.compress(some_data).unwrap()
                                    }
                                })
                            })
                            .collect()
                    });
                    let post_datum_compressed = meta.post_datum.as_ref().map(|all_datums| {
                        all_datums
                            .iter()
                            .map(|data| {
                                data.as_ref().map(|some_data| {
                                    if some_data.is_empty() {
                                        some_data.clone()
                                    } else {
                                        compressor.compress(some_data).unwrap()
                                    }
                                })
                            })
                            .collect()
                    });
                    let meta = TransactionStatusMeta {
                        status: meta.status.clone(),
                        fee: meta.fee,
                        pre_balances: meta.pre_balances.clone(),
                        post_balances: meta.post_balances.clone(),
                        pre_datum: pre_datum_compressed,
                        post_datum: post_datum_compressed,
                        inner_instructions: meta.inner_instructions.clone(),
                        log_messages: meta.log_messages.clone(),
                        pre_token_balances: meta.pre_token_balances.clone(),
                        post_token_balances: meta.post_token_balances.clone(),
                        rewards: meta.rewards.clone(),
                        loaded_addresses: meta.loaded_addresses.clone(),
                        return_data: meta.return_data.clone(),
                        compute_units_consumed: meta.compute_units_consumed,
                    };

                    //make it pretty
                    let full_tx = ConfirmedTransactionWithStatusMeta {
                        tx_with_meta: TransactionWithStatusMeta::Complete(
                            VersionedTransactionWithStatusMeta {
                                meta,
                                transaction: stx.to_versioned_transaction(),
                            },
                        ),
                        slot,
                        block_time: None,
                    };
                    let encoded_tx = full_tx.encode(UiTransactionEncoding::JsonParsed, Some(0))?;

                    Ok(Some((
                        Message::TransactionNotify(Box::new(TransactionNotify {
                            transaction: encoded_tx,
                        })),
                        route,
                    )))
                },
            }
        }

        self.with_inner(
            || GeyserPluginError::Custom(anyhow!(UNINIT).into()),
            |this| {
                if this.ins_sel.is_empty() && this.tx_sel.is_empty() {
                    return Ok(());
                }

                this.metrics.recvs.log(1);

                let stx: &SanitizedTransaction;
                let meta: &TransactionStatusMeta;
                let is_vote: bool;

                match transaction {
                    ReplicaTransactionInfoVersions::V0_0_1(tx) => {
                        stx = tx.transaction;
                        meta = tx.transaction_status_meta;
                        is_vote = tx.is_vote;
                    },
                    ReplicaTransactionInfoVersions::V0_0_2(tx) => {
                        stx = tx.transaction;
                        meta = tx.transaction_status_meta;
                        is_vote = tx.is_vote;
                    },
                }

                let is_err = matches!(meta.status, Err(..));

                //send this tx to the stats thread
                this.stats_sender.send(StatsRequest {
                    slot,
                    stx: stx.clone(),
                    meta: meta.clone(),
                    is_vote,
                    is_err,
                })?;

                //no downstream processing of errors or votes
                if is_err || is_vote {
                    return Ok(());
                }

                //handle tx match
                if !this.tx_sel.is_empty() {
                    match process_transaction(&this.tx_sel, stx, meta, slot) {
                        Ok(Some(m)) => {
                            let message = m.0;
                            let route = m.1.clone();
                            this.spawn(|this| async move {
                                this.producer.send(message, route.as_str()).await;
                                this.metrics.sends.log(1);

                                Ok(())
                            });
                        },
                        Ok(None) => (),
                        Err(e) => {
                            warn!("Error processing transaction: {:?}", e);
                            this.metrics.errs.log(1);
                        },
                    }
                }

                //handle ix matches
                if !this.ins_sel.is_empty() {
                    let msg = stx.message();
                    let keys = msg.account_keys();

                    //first check if any of the keys are in the instruction selector
                    //this prevents blowing out the instruction list when not needed
                    if keys.iter().any(|a| this.ins_sel.programs.contains_key(a)) {
                        for ins in msg.instructions().iter().chain(
                            meta.inner_instructions
                                .iter()
                                .flatten()
                                .flat_map(|i| i.instructions.iter().map(|i| &i.instruction)),
                        ) {
                            match process_instruction(&this.ins_sel, ins, &keys, slot) {
                                Ok(Some(m)) => {
                                    let message = m.0;
                                    let route = m.1.clone();
                                    this.spawn(|this| async move {
                                        this.producer.send(message, route.as_str()).await;
                                        this.metrics.sends.log(1);

                                        Ok(())
                                    });
                                },
                                Ok(None) => (),
                                Err(e) => {
                                    warn!("Error processing instruction: {:?}", e);
                                    this.metrics.errs.log(1);
                                },
                            }
                        }
                    }
                }

                Ok(())
            },
        )
    }

    /// Called when a slot status is updated
    #[allow(unused_variables)]
    fn update_slot_status(&self, slot: u64, parent: Option<u64>, status: SlotStatus) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::Custom(anyhow!(UNINIT).into()),
            |this| {
                if !this.chain_progress.slot_status.unwrap_or(false) {
                    return Ok(());
                };
                let msg = Message::SlotStatusNotify(SlotStatusNotify {
                    slot,
                    parent,
                    status: match status {
                        SlotStatus::Processed => indexer_rabbitmq::geyser::SlotStatus::Processed,
                        SlotStatus::Confirmed => indexer_rabbitmq::geyser::SlotStatus::Confirmed,
                        SlotStatus::Rooted => indexer_rabbitmq::geyser::SlotStatus::Rooted,
                    },
                });
                this.spawn(|this| async move {
                    this.producer.send(msg, "multi.chain.slot_status").await;
                    this.metrics.sends.log(1);

                    Ok(())
                });
                Ok(())
            },
        )
    }

    /// Called when block's metadata is updated.
    #[allow(unused_variables)]
    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::Custom(anyhow!(UNINIT).into()),
            |this| {
                if !this.chain_progress.block_meta.unwrap_or(false) {
                    return Ok(());
                };
                match blockinfo {
                    ReplicaBlockInfoVersions::V0_0_1(bi) => {
                        let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                            blockhash: String::from(bi.blockhash),
                            slot: bi.slot,
                            block_time: bi.block_time.unwrap_or_default(),
                            block_height: bi.block_height.unwrap_or_default(),
                        });
                        this.spawn(|this| async move {
                            this.producer.send(msg, "multi.chain.block_meta").await;
                            this.metrics.sends.log(1);

                            Ok(())
                        });
                    },
                    ReplicaBlockInfoVersions::V0_0_2(bi) => {
                        let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                            blockhash: String::from(bi.blockhash),
                            slot: bi.slot,
                            block_time: bi.block_time.unwrap_or_default(),
                            block_height: bi.block_height.unwrap_or_default(),
                        });
                        this.spawn(|this| async move {
                            this.producer.send(msg, "multi.chain.block_meta").await;
                            this.metrics.sends.log(1);

                            Ok(())
                        });
                    },
                    ReplicaBlockInfoVersions::V0_0_3(bi) => {
                        let msg = Message::BlockMetadataNotify(BlockMetadataNotify {
                            blockhash: String::from(bi.blockhash),
                            slot: bi.slot,
                            block_time: bi.block_time.unwrap_or_default(),
                            block_height: bi.block_height.unwrap_or_default(),
                        });
                        this.spawn(|this| async move {
                            this.producer.send(msg, "multi.chain.block_meta").await;
                            this.metrics.sends.log(1);

                            Ok(())
                        });
                    },
                }
                Ok(())
            },
        )
    }

    fn account_data_notifications_enabled(&self) -> bool {
        let this = self.expect_inner();
        !this.acct_sel.is_empty()
    }

    fn transaction_notifications_enabled(&self) -> bool {
        let this = self.expect_inner();
        !(this.ins_sel.is_empty() && this.tx_sel.is_empty())
    }
}
