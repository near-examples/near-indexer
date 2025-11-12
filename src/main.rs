use anyhow::Result;
use clap::Parser;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use tokio::sync::mpsc;

use configs::{Opts, SubCommand};
use near_indexer;
use near_indexer_primitives::types::AccountId;

mod configs;

fn main() -> Result<()> {
    openssl_probe::init_ssl_cert_env_vars();
    let env_filter = near_o11y::tracing_subscriber::EnvFilter::new(
        "nearcore=info,indexer_example=info,tokio_reactor=info,near=info,\
         stats=info,telemetry=info,indexer=info,near-performance-metrics=info",
    );
    let _subscriber = near_o11y::default_subscriber(env_filter, &Default::default()).global();
    // Parse the command line arguments
    let opts: Opts = Opts::parse();

    let home_dir = opts.home_dir.unwrap_or_else(near_indexer::get_default_home);

    match opts.subcmd {
        SubCommand::Run(args) => {
            // Get the list of accounts to watch from the command line arguments
            let watching_list: Vec<AccountId> = args
                .accounts
                .split(',')
                .map(|elem| AccountId::from_str(elem).expect("AccountId is invalid"))
                .collect();

            let sync_mode = if let Some(block_height) = args.block_height {
                near_indexer::SyncModeEnum::BlockHeight(block_height)
            } else {
                near_indexer::SyncModeEnum::LatestSynced
            };

            let indexer_config = near_indexer::IndexerConfig {
                home_dir,
                sync_mode: sync_mode,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
                finality: near_indexer::near_primitives::types::Finality::Final,
                validate_genesis: true,
            };
            let system = actix::System::new();
            system.block_on(async move {
                println!("Creating indexer...");
                let indexer = near_indexer::Indexer::new(indexer_config).expect("Indexer::new()");
                println!("Creating streamer...");
                let stream = indexer.streamer();
                println!("Listening to blocks...");
                actix::spawn(listen_blocks(stream, watching_list));
            });
            system.run()?;
        }
        SubCommand::Init(config) => near_indexer::indexer_init_configs(&home_dir, config.into())?,
    }
    Ok(())
}

async fn listen_blocks(
    mut stream: mpsc::Receiver<near_indexer::StreamerMessage>,
    watching_list: Vec<AccountId>,
) {
    // This will be a map of correspondence between transactions and receipts
    let mut tx_receipt_ids = HashMap::<String, String>::new();
    // This will be a list of receipt ids we're following
    let mut wanted_receipt_ids = HashSet::<String>::new();

    // Handle the messages from the stream
    while let Some(streamer_message) = stream.recv().await {
        parse_message(
            &streamer_message,
            &watching_list,
            &mut tx_receipt_ids,
            &mut wanted_receipt_ids,
        );
    }
}

fn parse_message(
    streamer_message: &near_indexer::StreamerMessage,
    watching_list: &[near_indexer_primitives::types::AccountId],
    tx_receipt_ids: &mut HashMap<String, String>,
    wanted_receipt_ids: &mut HashSet<String>,
) {
    eprintln!("Block height: {}", streamer_message.block.header.height);
    // Iterate over the shards in the block
    for shard in streamer_message.shards.clone() {
        let chunk = if let Some(chunk) = shard.chunk {
            chunk
        } else {
            continue;
        };

        // Iterate over the transactions in the chunk
        for transaction in chunk.transactions {
            // Check if transaction receiver id is one of the list we are interested in
            if is_tx_receiver_watched(&transaction, &watching_list) {
                // Extract receipt_id transaction was converted into
                let converted_into_receipt_id = transaction
                    .outcome
                    .execution_outcome
                    .outcome
                    .receipt_ids
                    .first()
                    .expect("`receipt_ids` must contain one Receipt Id")
                    .to_string();
                // Add `converted_into_receipt_id` to the list of receipt ids we are interested in
                wanted_receipt_ids.insert(converted_into_receipt_id.clone());
                // Add key value pair of transaction hash and in which receipt id it was converted for further lookup
                tx_receipt_ids.insert(
                    converted_into_receipt_id,
                    transaction.transaction.hash.to_string(),
                );
            }
        }

        // Iterate over the execution outcomes in the shard
        for execution_outcome in shard.receipt_execution_outcomes {
            // Check if the receipt id is in the list of receipt ids to track
            if let Some(receipt_id) =
                wanted_receipt_ids.take(&execution_outcome.receipt.receipt_id.to_string())
            {
                // Log the transaction hash, the receipt id and the status
                println!(
                    "\nTransaction hash {:?} related to {} executed with status {:?}",
                    tx_receipt_ids.get(receipt_id.as_str()),
                    &execution_outcome.receipt.receiver_id,
                    execution_outcome.execution_outcome.outcome.status
                );
                if let near_indexer_primitives::views::ReceiptEnumView::Action {
                    signer_id, ..
                } = &execution_outcome.receipt.receipt
                {
                    // Log the signer id
                    eprintln!("{}", signer_id);
                }

                if let near_indexer_primitives::views::ReceiptEnumView::Action { actions, .. } =
                    execution_outcome.receipt.receipt
                {
                    // Iterate over the actions in the receipt
                    for action in actions.iter() {
                        // If the action is a function call, log the decoded arguments
                        if let near_indexer_primitives::views::ActionView::FunctionCall {
                            args,
                            ..
                        } = action
                        {
                            if let Ok(args_json) =
                                serde_json::from_slice::<serde_json::Value>(&args)
                            {
                                eprintln!("{:#?}", args_json);
                            }
                        }
                    }
                }
                // Remove the receipt id from the map of receipt ids to transaction hashes because we have processed it
                tx_receipt_ids.remove(receipt_id.as_str());
            }
        }
    }
}

fn is_tx_receiver_watched(
    tx: &near_indexer_primitives::IndexerTransactionWithOutcome,
    watching_list: &[near_indexer_primitives::types::AccountId],
) -> bool {
    watching_list.contains(&tx.transaction.receiver_id)
}
