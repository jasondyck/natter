//! natter's server: LiveKit's signalling, APIs and SFU (`livekit`), TURN,
//! and the accounts behind natter's own web client. The binary only parses
//! the command line and calls [`serve`], [`admin`] or [`list_nodes`].

mod auth;
pub mod config;
mod http;
mod livekit;
mod metrics;
mod netio;
mod serve;
mod store;
mod turn;
mod util;

use std::sync::Arc;

use anyhow::{Context, Result, bail};

use auth::Auth;
use config::Settings;
use store::Store;

pub use serve::serve;

const USAGE: &str = "commands:
  generate-keys       print a new API key and secret (as LiveKit's)
  ports               print the ports the configuration uses (as LiveKit's)
  list-nodes          print the cluster's nodes (needs redis)
run with the server stopped:
  invite              print a new invite link
  users               list accounts
  delete-user NAME    delete an account, its sessions and its invites
  revoke-invites      delete every unused invite";

/// What the command line asks for; no words means serve.
#[derive(Debug, PartialEq)]
pub enum Command {
    Serve,
    GenerateKeys,
    Ports,
    ListNodes,
    Admin(Admin),
}

/// Account administration, which opens the database directly so it works
/// when nobody can sign in.
#[derive(Debug, PartialEq)]
pub enum Admin {
    Invite,
    Users,
    DeleteUser(String),
    RevokeInvites,
}

impl Command {
    pub fn parse(words: &[String]) -> Result<Self> {
        let words: Vec<&str> = words.iter().map(String::as_str).collect();
        Ok(match words.as_slice() {
            [] => Command::Serve,
            ["generate-keys"] => Command::GenerateKeys,
            ["ports"] => Command::Ports,
            ["list-nodes"] => Command::ListNodes,
            ["invite"] => Command::Admin(Admin::Invite),
            ["users"] => Command::Admin(Admin::Users),
            ["delete-user", name] => Command::Admin(Admin::DeleteUser(name.to_string())),
            ["revoke-invites"] => Command::Admin(Admin::RevokeInvites),
            _ => bail!("{USAGE}"),
        })
    }
}

/// Runs an account command and returns what to print.
pub fn admin(config: &Settings, op: Admin) -> Result<String> {
    let auth = open_auth(config)?;
    Ok(match op {
        Admin::Invite => auth.invite_url(&first_invite(&auth)?),
        Admin::Users => auth
            .store
            .users()?
            .iter()
            .map(|user| format!("{}\t{} passkey(s)", user.name, user.passkeys.len()))
            .collect::<Vec<_>>()
            .join("\n"),
        Admin::DeleteUser(name) => {
            if !auth.store.delete_user(&name)? {
                bail!("no account named {name:?}");
            }
            format!("deleted {name}")
        }
        Admin::RevokeInvites => format!("revoked {} invite(s)", auth.store.revoke_invites()?),
    })
}

/// The cluster's nodes as a table, read from Redis without joining.
pub async fn list_nodes(config: &Settings) -> Result<String> {
    let Some(c) = config.cluster.clone() else {
        bail!("list-nodes needs redis configured");
    };
    let cluster = livekit::cluster::Cluster::connect_read_only(c).await?;
    Ok(livekit::cluster::node_table(&cluster.nodes().await?))
}

/// The account database, which only one process can open.
fn open_auth(config: &Settings) -> Result<Arc<Auth>> {
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating {}", config.data_dir.display()))?;
    let db_path = config.data_dir.join("natter.redb");
    let store = Store::open(&db_path).with_context(|| {
        format!(
            "opening {} (only one process can, so stop the server to run `invite`)",
            db_path.display()
        )
    })?;
    Ok(Arc::new(Auth::new(config.origin.clone(), store)?))
}

/// An invite made by the server rather than a user, so it has no cap.
fn first_invite(auth: &Auth) -> Result<String> {
    Ok(auth
        .store
        .create_invite(None, usize::MAX)?
        .expect("uncapped"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(words: &[&str]) -> Result<Command> {
        Command::parse(&words.iter().map(|w| w.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn commands() {
        assert_eq!(parse(&[]).unwrap(), Command::Serve);
        assert_eq!(parse(&["ports"]).unwrap(), Command::Ports);
        assert_eq!(
            parse(&["delete-user", "ann"]).unwrap(),
            Command::Admin(Admin::DeleteUser("ann".into()))
        );
        assert!(parse(&["delete-user"]).is_err());
        assert!(parse(&["users", "extra"]).is_err());
        assert!(parse(&["nonsense"]).is_err());
    }
}
