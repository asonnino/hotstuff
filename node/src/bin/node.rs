use clap::{crate_name, crate_version, App, AppSettings, SubCommand};
use env_logger::Env;
use log::error;
use node::node::Node;
use std::fs;
use tokio::runtime::Runtime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        .setting(AppSettings::ArgRequiredElseHelp)
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
            Runtime::new()?.block_on(async {
                match Node::make(committee_file, key_file, store_path, parameters_file).await {
                    Ok(mut rx_channel) => {
                        // Sink the commit channel.
                        let _ = rx_channel.recv().await;
                    }
                    Err(e) => error!("{}", e),
                }
            });
        }
        ("deploy", Some(subm)) => {
            let nodes = subm
                .value_of("nodes")
                .unwrap()
                .parse::<usize>()
                .expect("The number of nodes must be an integer");
            deploy_testbed(nodes)?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn deploy_testbed(nodes: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Make the key files.
    let key_files: Vec<_> = (0..nodes).map(|x| format!(".node_{}.json", x)).collect();
    for filename in &key_files {
        let _ = fs::remove_file(&filename);
        Node::print_key_file(&filename)?;
    }

    /*
    // Make the committee to file.
    let committee_file = ".committee.json";
    let _ = fs::remove_dir_all(committee_file);
    let authorities = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let authority = Authority {
                name: *name,
                stake: 1,
                host: "127.0.0.1".to_string(),
                port: i as u16,
            };
            (*name, authority)
        })
        .collect();
    Committee {
        authorities,
        epoch: 1,
    }
    */

    Ok(())
}
