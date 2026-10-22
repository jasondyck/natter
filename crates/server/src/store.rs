//! Accounts, sessions and invites in a single redb file.
//!
//! Values are JSON. Session and invite tokens are stored only as SHA-256
//! hashes, so a copy of the database can't be used to sign in.

use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;
use unicode_normalization::UnicodeNormalization;
use unicode_security::confusable_detection::skeleton;
use uuid::Uuid;
use webauthn_rs::prelude::Passkey;

use crate::util::random_bytes;

/// user id → [`User`]
const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("users");
/// [`name_keys`] → user id, each user under every key of their name.
/// Derived from `USERS`, and rebuilt on open.
const NAMES: TableDefinition<&str, &str> = TableDefinition::new("names");
/// token hash → [`Session`]
const SESSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("sessions");
/// token hash → [`Invite`]
const INVITES: TableDefinition<&str, &[u8]> = TableDefinition::new("invites");
/// Server secrets.
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// Key in `META` for the secret that derives decoy credential ids.
const DECOY_KEY: &str = "decoy_key";

pub const SESSION_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);
pub const INVITE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub name: String,
    pub passkeys: Vec<Passkey>,
    pub created_at: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Session {
    pub user_id: Uuid,
    pub expires_at: u64,
}

#[derive(Serialize, Deserialize)]
struct Invite {
    created_by: Option<Uuid>,
    expires_at: u64,
}

#[derive(Debug)]
pub enum CreateUserError {
    InviteInvalid,
    NameTaken,
}

pub struct Store {
    db: Database,
    decoy_key: Vec<u8>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path)?;
        let tx = db.begin_write()?;
        tx.open_table(USERS)?;
        tx.open_table(SESSIONS)?;
        tx.open_table(INVITES)?;
        rebuild_names(&tx)?;
        let decoy_key = {
            let mut meta = tx.open_table(META)?;
            let existing = meta.get(DECOY_KEY)?.map(|v| v.value().to_vec());
            match existing {
                Some(key) => key,
                None => {
                    let key = random_bytes::<32>().to_vec();
                    meta.insert(DECOY_KEY, key.as_slice())?;
                    key
                }
            }
        };
        tx.commit()?;
        Ok(Store { db, decoy_key })
    }

    pub fn user_count(&self) -> Result<u64> {
        Ok(self.db.begin_read()?.open_table(USERS)?.len()?)
    }

    /// Every account, oldest first.
    pub fn users(&self) -> Result<Vec<User>> {
        let tx = self.db.begin_read()?;
        sorted_users(&tx.open_table(USERS)?)
    }

    pub fn user(&self, id: Uuid) -> Result<Option<User>> {
        let tx = self.db.begin_read()?;
        let users = tx.open_table(USERS)?;
        users
            .get(id.to_string().as_str())?
            .map(|v| decode(v.value()))
            .transpose()
    }

    pub fn user_by_name(&self, name: &str) -> Result<Option<User>> {
        let id = {
            let tx = self.db.begin_read()?;
            let names = tx.open_table(NAMES)?;
            // Signup keeps keys of different users apart, so any match is
            // the only one.
            let mut found = None;
            for key in name_keys(name) {
                if let Some(id) = names.get(key.as_str())? {
                    found = Some(Uuid::parse_str(id.value())?);
                    break;
                }
            }
            let Some(id) = found else {
                return Ok(None);
            };
            id
        };
        self.user(id)
    }

    pub fn name_taken(&self, name: &str) -> Result<bool> {
        let tx = self.db.begin_read()?;
        let names = tx.open_table(NAMES)?;
        for key in name_keys(name) {
            if names.get(key.as_str())?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Creates `user` and consumes the invite, atomically. The outer error is
    /// a database failure; the inner one is a reason to refuse.
    pub fn create_user_with_invite(
        &self,
        user: &User,
        invite: &str,
    ) -> Result<Result<(), CreateUserError>> {
        let tx = self.db.begin_write()?;
        {
            let mut invites = tx.open_table(INVITES)?;
            let valid = invites
                .remove(token_hash(invite).as_str())?
                .map(|v| decode::<Invite>(v.value()))
                .transpose()?
                .is_some_and(|i| i.expires_at > now());
            // Returning before commit aborts the transaction, so a refused
            // signup doesn't use up the invite.
            if !valid {
                return Ok(Err(CreateUserError::InviteInvalid));
            }

            let mut names = tx.open_table(NAMES)?;
            let id = user.id.to_string();
            for key in name_keys(&user.name) {
                if names.insert(key.as_str(), id.as_str())?.is_some() {
                    return Ok(Err(CreateUserError::NameTaken));
                }
            }
            tx.open_table(USERS)?
                .insert(id.as_str(), encode(user)?.as_slice())?;
        }
        tx.commit()?;
        Ok(Ok(()))
    }

    /// Saves changes to an existing user's passkeys.
    pub fn save_user(&self, user: &User) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(USERS)?
            .insert(user.id.to_string().as_str(), encode(user)?.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    /// Removes an account with its sessions and the invites it made.
    /// Returns false if there is no such account.
    pub fn delete_user(&self, name: &str) -> Result<bool> {
        let Some(user) = self.user_by_name(name)? else {
            return Ok(false);
        };
        let tx = self.db.begin_write()?;
        tx.open_table(USERS)?.remove(user.id.to_string().as_str())?;
        {
            let mut names = tx.open_table(NAMES)?;
            for key in name_keys(&user.name) {
                names.remove(key.as_str())?;
            }
        }
        tx.open_table(SESSIONS)?
            .retain(|_, v| decode::<Session>(v).map_or(true, |s| s.user_id != user.id))?;
        tx.open_table(INVITES)?
            .retain(|_, v| decode::<Invite>(v).map_or(true, |i| i.created_by != Some(user.id)))?;
        tx.commit()?;
        Ok(true)
    }

    /// Makes an invite. With `created_by`, returns `None` instead when that
    /// user already has `limit` unused invites.
    pub fn create_invite(&self, created_by: Option<Uuid>, limit: usize) -> Result<Option<String>> {
        let token = random_token();
        let now = now();
        let invite = Invite {
            created_by,
            expires_at: now + INVITE_TTL.as_secs(),
        };
        let tx = self.db.begin_write()?;
        {
            let mut invites = tx.open_table(INVITES)?;
            if created_by.is_some() {
                let mut open = 0;
                for row in invites.iter()? {
                    let i = decode::<Invite>(row?.1.value())?;
                    if i.created_by == created_by && i.expires_at > now {
                        open += 1;
                    }
                }
                if open >= limit {
                    return Ok(None);
                }
            }
            invites.insert(token_hash(&token).as_str(), encode(&invite)?.as_slice())?;
        }
        tx.commit()?;
        Ok(Some(token))
    }

    pub fn invite_valid(&self, token: &str) -> Result<bool> {
        let tx = self.db.begin_read()?;
        let invites = tx.open_table(INVITES)?;
        Ok(invites
            .get(token_hash(token).as_str())?
            .map(|v| decode::<Invite>(v.value()))
            .transpose()?
            .is_some_and(|i| i.expires_at > now()))
    }

    /// Deletes every unused invite. Returns how many there were.
    pub fn revoke_invites(&self) -> Result<u64> {
        let tx = self.db.begin_write()?;
        let count = {
            let mut invites = tx.open_table(INVITES)?;
            let count = invites.len()?;
            invites.retain(|_, _| false)?;
            count
        };
        tx.commit()?;
        Ok(count)
    }

    pub fn create_session(&self, user_id: Uuid) -> Result<String> {
        let token = random_token();
        let session = Session {
            user_id,
            expires_at: now() + SESSION_TTL.as_secs(),
        };
        let tx = self.db.begin_write()?;
        tx.open_table(SESSIONS)?
            .insert(token_hash(&token).as_str(), encode(&session)?.as_slice())?;
        tx.commit()?;
        Ok(token)
    }

    /// The session with this [`token_hash`], if it exists and hasn't expired.
    pub fn session(&self, hash: &str) -> Result<Option<Session>> {
        let tx = self.db.begin_read()?;
        let sessions = tx.open_table(SESSIONS)?;
        Ok(sessions
            .get(hash)?
            .map(|v| decode::<Session>(v.value()))
            .transpose()?
            .filter(|s| s.expires_at > now()))
    }

    pub fn delete_session(&self, hash: &str) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(SESSIONS)?.remove(hash)?;
        tx.commit()?;
        Ok(())
    }

    /// Drops expired sessions and invites. Returns how many of each.
    pub fn purge_expired(&self) -> Result<(u64, u64)> {
        let now = now();
        let tx = self.db.begin_write()?;
        let sessions = {
            let mut table = tx.open_table(SESSIONS)?;
            let before = table.len()?;
            table.retain(|_, v| decode::<Session>(v).map_or(true, |s| s.expires_at > now))?;
            before - table.len()?
        };
        let invites = {
            let mut table = tx.open_table(INVITES)?;
            let before = table.len()?;
            table.retain(|_, v| decode::<Invite>(v).map_or(true, |i| i.expires_at > now))?;
            before - table.len()?
        };
        tx.commit()?;
        Ok((sessions, invites))
    }

    /// A credential id that is stable for `name` but unguessable without the
    /// server's secret. Sign-in offers it for names with no account, so the
    /// reply looks like one for a real account.
    pub fn decoy_credential_id(&self, name: &str) -> [u8; 16] {
        let digest = Sha256::new()
            .chain_update(&self.decoy_key)
            .chain_update(name_keys(name).join("\0"))
            .finalize();
        digest[..16].try_into().expect("16 of 32 bytes")
    }
}

/// Every account in `USERS`, oldest first.
fn sorted_users(table: &impl ReadableTable<&'static str, &'static [u8]>) -> Result<Vec<User>> {
    let mut users = table
        .iter()?
        .map(|row| decode::<User>(row?.1.value()))
        .collect::<Result<Vec<_>>>()?;
    users.sort_by_key(|u| u.created_at);
    Ok(users)
}

/// Recomputes `NAMES` from `USERS`, so changes to [`name_keys`] apply to
/// existing accounts. Where two accounts now share a key, the older keeps
/// the name and the newer can no longer sign in by it.
fn rebuild_names(tx: &redb::WriteTransaction) -> Result<()> {
    let users = sorted_users(&tx.open_table(USERS)?)?;

    tx.delete_table(NAMES)?;
    let mut names = tx.open_table(NAMES)?;
    for user in &users {
        let id = user.id.to_string();
        let keys = name_keys(&user.name);
        let mut clash = false;
        for key in &keys {
            clash |= names.get(key.as_str())?.is_some();
        }
        if clash {
            warn!(
                user = %user.name,
                id = %user.id,
                "name looks like an older account's; delete one with `delete-user`"
            );
            continue;
        }
        for key in &keys {
            names.insert(key.as_str(), id.as_str())?;
        }
    }
    Ok(())
}

/// 32 random bytes as hex.
pub fn random_token() -> String {
    hex::encode(random_bytes::<32>())
}

/// How a token is stored and looked up.
pub fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// A name as it is stored and shown: NFKC-normalised, with runs of
/// whitespace collapsed to one space and none at the ends.
pub fn display_name(name: &str) -> String {
    let name: String = name.nfkc().collect();
    name.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Names are unique ignoring case and look-alike characters: `Bob`, `bob`
/// and `Воb` (Cyrillic) are one name, and two names clash if any of their
/// keys match. The confusables skeleton is from Unicode TR39, so it also
/// folds sequences like `rn` into `m`.
///
/// There are two keys because no one order of case folding and skeleton
/// catches everything. Folding case first misses Cyrillic `В`, whose
/// lowercase `в` looks like small-capital `ʙ` rather than `b`. Taking the
/// skeleton first maps `I` to `l` but leaves `i` alone, losing `Ivan` =
/// `ivan`.
fn name_keys(name: &str) -> Vec<String> {
    let name = display_name(name);
    let skeleton_first = skeleton(&name).collect::<String>().to_lowercase();
    let case_first = skeleton(&name.to_lowercase()).collect::<String>();
    let mut keys = vec![skeleton_first, case_first];
    keys.dedup();
    keys
}

pub use crate::util::unix_secs as now;

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clash(a: &str, b: &str) -> bool {
        let b = name_keys(b);
        name_keys(a).iter().any(|k| b.contains(k))
    }

    #[test]
    fn name_keys_fold_case_and_look_alikes() {
        assert!(clash("Bob", "bob"));
        assert!(clash("Ivan", "ivan"));
        // Cyrillic В and о, upper and lower case.
        assert!(clash("Bob", "\u{412}\u{43e}b"));
        assert!(clash("bob", "\u{412}\u{43e}b"));
        // Fullwidth letters are NFKC-equivalent to ASCII.
        assert!(clash("bob", "ｂｏｂ"));
        // Composed and decomposed é.
        assert!(clash("Andr\u{e9}", "Andre\u{301}"));
        assert!(clash("  Bob   Smith ", "bob smith"));
        assert!(!clash("bob", "rob"));
    }

    #[test]
    fn display_names_are_normalised() {
        assert_eq!(display_name("  ｂｏｂ \t smith "), "bob smith");
    }
}
