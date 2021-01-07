use clap::{crate_name, crate_version, App, AppSettings, SubCommand};
use env_logger::Env;
use futures::future::try_join_all;
use log::error;
use hotstuff::config::Config as _;
use hotstuff::config::{Committee, Secret};
use hotstuff::node::Node;
use std::fs;
use tokio::task::JoinHandle;

type MainResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> MainResult<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of the HostStuff protocol.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Runs a single node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'"),
        )
        .subcommand(
            SubCommand::with_name("deploy")
                .about("Deploys a network of nodes locally")
                .args_from_usage("--nodes=<INT> 'The number of nodes to deploy'"),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    env_logger::Builder::from_env(Env::default().default_filter_or(log_level)).init();

    match matches.subcommand() {
        ("keys", Some(subm)) => {
            let filename = subm.value_of("filename").unwrap();
            Node::print_key_file(&filename)?;
        }
        ("run", Some(subm)) => {
            let key_file = subm.value_of("keys").unwrap();
            let committee_file = subm.value_of("committee").unwrap();
            let parameters_file = subm.value_of("parameters");
            let store_path = subm.value_of("store").unwrap();
            match Node::make(committee_file, key_file, store_path, parameters_file).await {
                Ok(mut rx) => {
                    // Sink the commit channel.
                    while rx.recv().await.is_some() {}
                }
                Err(e) => error!("{}", e),
            }
        }
        ("deploy", Some(subm)) => {
            let nodes = subm.value_of("nodes").unwrap();
            match nodes.parse::<usize>() {
                Ok(nodes) if nodes > 0 => match deploy_testbed(nodes) {
                    Ok(handles) => {
                        let _ = try_join_all(handles).await;
                    }
                    Err(e) => error!("{}", e),
                },
                _ => error!("The number of nodes must be a positive integer"),
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn deploy_testbed(nodes: usize) -> MainResult<Vec<JoinHandle<()>>> {
    let keys: Vec<_> = (0..nodes).map(|_| Secret::new()).collect();

    let committee_file = "committee.json";
    let _ = fs::remove_file(committee_file);
    let authorities: Vec<_> = keys.iter().map(|keys| (keys.name, /* stake */ 1)).collect();
    let mut committee = Committee::new(&authorities, /* epoch */ 1);
    for x in committee.authorities.values_mut() {
        x.address.set_port(x.address.port() + 7000);
    }
    committee.write(committee_file)?;

    keys.iter()
        .enumerate()
        .map(|(i, keypair)| {
            let key_file = format!("node_{}.json", i);
            let _ = fs::remove_file(&key_file);
            keypair.write(&key_file)?;

            let store_path = format!("store_{}", i);
            let _ = fs::remove_dir_all(&store_path);

            Ok(tokio::spawn(async move {
                match Node::make(committee_file, &key_file, &store_path, None).await {
                    Ok(mut rx) => {
                        // Sink the commit channel.
                        while rx.recv().await.is_some() {}
                    }
                    Err(e) => error!("{}", e),
                }
            }))
        })
        .collect::<Result<_, Box<dyn std::error::Error>>>()
}
