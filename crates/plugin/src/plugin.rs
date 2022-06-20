use std::{env, sync::Arc};

use anyhow::Context;
use hashbrown::HashSet;
use indexer_rabbitmq::geyser::{AccountUpdate, InstructionNotify, Message};
use solana_program::{instruction::CompiledInstruction, message::AccountKeys, program_pack::Pack};
use spl_token::state::Account as TokenAccount;

static TOKEN_KEY: Pubkey = solana_program::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

use serde::Deserialize;

use crate::{
    config::Config,
    interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfo, ReplicaAccountInfoVersions,
        ReplicaTransactionInfoVersions, Result,
    },
    metrics::{Counter, Metrics},
    prelude::*,
    selectors::{AccountSelector, InstructionSelector},
    sender::Sender,
};

const UNINIT: &str = "RabbitMQ plugin not initialized yet!";

#[inline]
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
    rt: tokio::runtime::Runtime,
    producer: Sender,
    acct_sel: AccountSelector,
    ins_sel: InstructionSelector,
    metrics: Arc<Metrics>,
    token_addresses: HashSet<Pubkey>,
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

#[derive(Deserialize)]
struct TokenItem {
    address: String,
}

#[derive(Deserialize)]
struct TokenList {
    tokens: Vec<TokenItem>,
}

impl GeyserPluginRabbitMq {
    const TOKEN_REG_URL: &'static str = "https://raw.githubusercontent.com/solana-labs/token-list/main/src/tokens/solana.tokenlist.json";

    async fn load_token_reg() -> anyhow::Result<HashSet<Pubkey>> {
        let res: TokenList = reqwest::get(Self::TOKEN_REG_URL)
            .await
            .context("HTTP request failed")?
            .json()
            .await
            .context("Failed to parse response JSON")?;

        res.tokens
            .into_iter()
            .map(|TokenItem { address }| address.parse())
            .collect::<StdResult<_, _>>()
            .context("Failed to convert token list")
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
        "GeyserPluginRabbitMq"
    }

    fn on_load(&mut self, cfg: &str) -> Result<()> {
        solana_logger::setup_with_default("info");

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

                let mut s = format!("v{}", ver);

                if let Some(git) = git {
                    write!(s, "+git.{}", git).unwrap();
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

        let (amqp, jobs, metrics_conf, acct_sel, ins_sel) = Config::read(cfg)
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
            .thread_name("geyser-rabbitmq")
            .worker_threads(jobs.limit)
            .max_blocking_threads(jobs.blocking.unwrap_or(jobs.limit))
            .build()
            .map_err(custom_err(&metrics.errs))?;

        let (producer, token_addresses) = rt.block_on(async {
            let producer = Sender::new(
                amqp,
                format!("geyser-rabbitmq-{}@{}", version, host),
                startup_type,
                Arc::clone(&metrics),
            )
            .await
            .map_err(custom_err(&metrics.errs))?;

            let tokens = Self::load_token_reg()
                .await
                .map_err(custom_err(&metrics.errs))?;

            Result::<_>::Ok((producer, tokens))
        })?;

        self.0 = Some(Arc::new(Inner {
            rt,
            producer,
            acct_sel,
            ins_sel,
            metrics,
            token_addresses,
        }));

        Ok(())
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        self.with_inner(
            || GeyserPluginError::AccountsUpdateError { msg: UNINIT.into() },
            |this| {
                this.metrics.recvs.log(1);

                match account {
                    ReplicaAccountInfoVersions::V0_0_1(acct) => {
                        if !this.acct_sel.is_selected(acct, is_startup) {
                            return Ok(());
                        }

                        let ReplicaAccountInfo {
                            pubkey,
                            lamports,
                            owner,
                            executable,
                            rent_epoch,
                            data,
                            write_version,
                        } = *acct;

                        if owner == TOKEN_KEY.as_ref()
                            && data.len() == TokenAccount::get_packed_len()
                        {
                            let token_account = TokenAccount::unpack_from_slice(data);

                            if let Ok(token_account) = token_account {
                                if token_account.amount > 1
                                    || this.token_addresses.contains(&token_account.mint)
                                {
                                    return Ok(());
                                }
                            }
                        }

                        let key = Pubkey::new_from_array(pubkey.try_into()?);
                        let owner = Pubkey::new_from_array(owner.try_into()?);
                        let data = data.to_owned();

                        this.spawn(|this| async move {
                            this.producer
                                .send(Message::AccountUpdate(AccountUpdate {
                                    key,
                                    lamports,
                                    owner,
                                    executable,
                                    rent_epoch,
                                    data,
                                    write_version,
                                    slot,
                                    is_startup,
                                }), &key.to_string())
                                .await;
                            this.metrics.sends.log(1);

                            Ok(())
                        });
                    },
                };

                Ok(())
            },
        )
    }

    fn notify_transaction(
        &mut self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        #[inline]
        fn process_instruction(
            sel: &InstructionSelector,
            ins: &CompiledInstruction,
            keys: &AccountKeys,
            slot: u64,
        ) -> anyhow::Result<Option<Message>> {
            let program = *keys
                .get(ins.program_id_index as usize)
                .ok_or_else(|| anyhow!("Couldn't get program ID for instruction"))?;

            if !sel.is_selected(&program) {
                return Ok(None);
            }

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

            Ok(Some(Message::InstructionNotify(InstructionNotify {
                program,
                data,
                accounts,
                slot,
            })))
        }

        self.with_inner(
            || GeyserPluginError::Custom(anyhow!(UNINIT).into()),
            |this| {
                if this.ins_sel.is_empty() {
                    return Ok(());
                }

                this.metrics.recvs.log(1);

                match transaction {
                    ReplicaTransactionInfoVersions::V0_0_1(tx) => {
                        if matches!(tx.transaction_status_meta.status, Err(..)) {
                            return Ok(());
                        }

                        let msg = tx.transaction.message();
                        let keys = msg.account_keys();

                        for ins in msg.instructions().iter().chain(
                            tx.transaction_status_meta
                                .inner_instructions
                                .iter()
                                .flatten()
                                .flat_map(|i| i.instructions.iter()),
                        ) {
                            match process_instruction(&this.ins_sel, ins, &keys, slot) {
                                Ok(Some(m)) => {
                                    this.spawn(|this| async move {
                                        // @TODO figure out a better default here
                                        this.producer.send(m, "").await;
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
                    },
                }

                Ok(())
            },
        )
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        let this = self.expect_inner();
        !this.ins_sel.is_empty()
    }
}
