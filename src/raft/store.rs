use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use axum::async_trait;
use kv::Integer;
use openraft::{
    AnyError, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader,
    RaftSnapshotBuilder, RaftStorage, Snapshot, SnapshotMeta, StorageIOError, StoredMembership,
    Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::raft::common::{Node, NodeId, SnapshotList, Response, TypeConfig};

// ------ Error Helpers ------ //
pub type StorageResult<T> = Result<T, StorageError>;
pub type StorageError = openraft::StorageError<NodeId>;

// Map a kv error to a storage error.
pub fn state_read_error<E: std::error::Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        AnyError::from(&e),
    )
        .into()
}

// Map a kv error to a storage error.
pub fn state_write_error<E: std::error::Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        AnyError::from(&e),
    )
        .into()
}

// Map a kv error to a storage error.
pub fn storage_read_error<E: std::error::Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::from(&e)).into()
}

// Map a kv error to a storage error.
pub fn storage_write_error<E: std::error::Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::from(&e)).into()
}

// Map a kv error to a storage error.
pub fn logs_read_error<E: std::error::Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::from(&e)).into()
}

// Map a kv error to a storage error.
pub fn logs_write_error<E: std::error::Error + 'static>(e: E) -> StorageError {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::from(&e)).into()
}

// ------ Snapshot ------ //

// Snapshots are replicated between nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,

    pub data: Vec<u8>,
}

// ------ State Machine ------ //

// The serializable state machine is what actually gets replicated between nodes. It's a wrapper
// around the internal state machine that is used to directly address kv.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializableStateMachine {
    pub last_applied_log: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, Node>,

    pub data: BTreeMap<String, String>,
}

// Serialize a state machine from internal state machine to be sent over the network
impl TryFrom<&InternalStateMachine> for SerializableStateMachine {
    type Error = StorageError;

    fn try_from(sm: &InternalStateMachine) -> Result<Self, Self::Error> {
        Ok(Self {
            last_applied_log: sm.last_applied_log()?,
            last_membership: sm.last_membership()?,
            data: {
                sm.db
                    .bucket::<String, String>(Some("data"))
                    .map_err(storage_read_error)?
                    .iter()
                    .map(|item| {
                        let item = item.map_err(storage_read_error)?;
                        Ok((
                            item.key().map_err(storage_read_error)?,
                            item.value().map_err(storage_read_error)?,
                        ))
                    })
                    .collect::<Result<BTreeMap<String, String>, StorageError>>()?
            },
        })
    }
}

// The internal state machine is what actually stores the data and interacts with the key-value
// store.
#[derive(Clone, Debug)]
pub struct InternalStateMachine {
    pub db: kv::Store,
}

impl InternalStateMachine {
    // Create a new internal state machine.
    pub fn new(db: &kv::Store) -> Self {
        Self { db: db.clone() }
    }

    // Get the data associated with a key.
    pub fn get(&self, key: &String) -> StorageResult<Option<String>> {
        // Get the data from the key-value store.
        self.db
            .bucket::<String, String>(Some("data"))
            .map_err(storage_read_error)?
            .get(key)
            .map_err(storage_read_error)
    }

    // Set the data associated with a key.
    pub fn set(&mut self, key: String, value: String) -> StorageResult<()> {
        // Set the data in the key-value store.
        self.db
            .bucket::<String, String>(Some("data"))
            .map_err(storage_write_error)?
            .set(&key, &value)
            .map_err(storage_write_error)
            .map(|_| ())
    }

    // Get the last membership.
    pub fn last_membership(&self) -> StorageResult<StoredMembership<NodeId, Node>> {
        // Get the last membership from the key-value store. Try to deserialize it. Otherwise,
        // return the default.
        self.db
            .bucket::<String, String>(Some("state_machine"))
            .map_err(state_read_error)?
            .get(&String::from("last_membership"))
            .map_err(state_read_error)?
            .map_or_else(
                || Ok(StoredMembership::default()),
                |last_membership| serde_json::from_str(&last_membership).map_err(state_read_error),
            )
    }

    // Set the last membership.
    pub fn set_last_membership(
        &mut self,
        membership: StoredMembership<NodeId, Node>,
    ) -> StorageResult<()> {
        // Set the last membership in the key-value store. Serialize it first. Throw out the
        // return if it
        self.db
            .bucket::<String, String>(Some("state_machine"))
            .map_err(state_write_error)?
            .set(
                &String::from("last_membership"),
                &serde_json::to_string(&membership).map_err(state_write_error)?,
            )
            .map_err(state_write_error)
            .map(|_| ())
    }

    // Get the last applied log.
    pub fn last_applied_log(&self) -> StorageResult<Option<LogId<NodeId>>> {
        // Get the last applied log from the key-value store. Try to deserialize it. Otherwise,
        // return the default.
        self.db
            .bucket::<String, String>(Some("state_machine"))
            .map_err(state_read_error)?
            .get(&String::from("last_applied_log"))
            .map_err(state_read_error)?
            .map_or_else(
                || Ok(None),
                |last_applied_log| {
                    serde_json::from_str(&last_applied_log).map_err(state_read_error)
                },
            )
    }

    // Set the last applied log.
    pub fn set_last_applied_log(&mut self, last_applied_log: LogId<NodeId>) -> StorageResult<()> {
        // Set the last applied log in the key-value store. Serialize it first. Throw out the
        // return if it
        self.db
            .bucket::<String, String>(Some("state_machine"))
            .map_err(state_write_error)?
            .set(
                &String::from("last_applied_log"),
                &serde_json::to_string(&last_applied_log).map_err(state_write_error)?,
            )
            .map_err(state_write_error)
            .map(|_| ())
    }

    // Turn a serializable state machine into an internal state machine.
    pub fn from_serializable(sm: SerializableStateMachine, db: &kv::Store) -> StorageResult<Self> {
        // Open the data bucket.
        let bucket = db
            .bucket::<String, String>(Some("data"))
            .map_err(storage_write_error)?;

        sm.data.iter().try_for_each(|(key, value)| {
            bucket.set(key, value).map_err(storage_write_error).map(|_| ())
        })?;

        let mut internal_sm = Self { db: db.clone() };

        // Set the last applied log.
        if let Some(last_applied_log) = sm.last_applied_log {
            internal_sm.set_last_applied_log(last_applied_log)?;
        }

        // Set the last membership.
        internal_sm.set_last_membership(sm.last_membership)?;

        Ok(internal_sm)
    }
}

// ------ Store ------ //

// The actual store where the data is stored.
#[derive(Clone, Debug)]
pub struct Store {
    pub db: kv::Store,
    pub sm: Arc<RwLock<InternalStateMachine>>,
}

impl Store {
    // Create a new store.
    pub async fn new(kv: kv::Store) -> Self {
        // Create a new internal state machine.
        let sm = InternalStateMachine::new(&kv);

        // Create a new store.
        Self {
            db: kv,
            sm: Arc::new(RwLock::new(sm)),
        }
    }

    // Get the meta data from the store.
    pub fn get_meta<M: meta::StoreMetadata>(&self) -> StorageResult<Option<M::Value>> {
        // Get the meta data from the key-value store. Try to deserialize it. Otherwise, return
        // the default.
        self.db
            .bucket::<String, String>(Some("meta"))
            .map_err(storage_read_error)?
            .get(&String::from(M::KEY))
            .map_err(storage_read_error)?
            .map_or_else(
                || Ok(None),
                |meta| serde_json::from_str(&meta).map_err(storage_read_error),
            )
    }

    // Set the meta data in the store.
    pub fn set_meta<M: meta::StoreMetadata>(&mut self, meta: &M::Value) -> StorageResult<()> {
        // Set the meta data in the key-value store. Serialize it first. Throw out the return if
        // it fails.
        self.db
            .bucket::<String, String>(Some("meta"))
            .map_err(storage_write_error)?
            .set(
                &String::from(M::KEY),
                &serde_json::to_string(&meta).map_err(storage_write_error)?,
            )
            .map_err(storage_write_error)
            .map(|_| ())
    }

    // Get the last log id.
    pub fn last_log_id(&self) -> StorageResult<Option<LogId<NodeId>>> {
        // Get the last log id from the key-value store. Try to deserialize it. Otherwise, return
        // the default.
        self
            .db
            .bucket::<Integer, String>(Some("log"))
            .map_err(logs_read_error)?
            .last()
            .map_err(logs_read_error)?
            .map_or_else(
                || Ok::<_, StorageError>(None),
                |x| {
                    Ok(Some(
                        serde_json::from_str::<Entry<TypeConfig>>(
                            &x.value::<String>().map_err(logs_read_error)?,
                        )
                            .map_err(logs_read_error)?
                            .log_id,
                    ))
                },
            )
    }
}

#[async_trait]
impl RaftLogReader<TypeConfig> for Store {
    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        // Open the log bucket and get the last log index, deserializing it if it exists.
        let last_log_index = self.last_log_id()?;

        // Get the last purged log id.
        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;

        Ok(LogState {
            last_purged_log_id,
            last_log_id: last_log_index.map_or_else(|| last_purged_log_id, Some),
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let start = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => x + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(x) => x + 1,
            Bound::Excluded(x) => *x,
            Bound::Unbounded => {
                (self
                    .db
                    .bucket::<Integer, String>(Some("log"))
                    .map_err(logs_read_error)?
                    .len()
                    + 1) as u64
            }
        };

        self.db
            .bucket::<Integer, String>(Some("log"))
            .map_err(logs_read_error)?
            .iter_range(&start.into(), &end.into())
            .map_err(logs_read_error)?
            .map(|x| {
                serde_json::from_str::<Entry<TypeConfig>>(
                    &x.map_err(logs_read_error)?
                        .value::<String>()
                        .map_err(logs_read_error)?,
                )
                    .map_err(logs_read_error)
            })
            .collect()
    }
}

#[async_trait]
impl RaftSnapshotBuilder<TypeConfig, SnapshotList> for Store {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<NodeId, Node, SnapshotList>> {
        let last_applied_log;
        let last_membership;
        let data;

        // Only hold the read lock for as long as we need it.
        {
            let state_machine = SerializableStateMachine::try_from(&*self.sm.read().await)?;
            data = serde_json::to_vec(&state_machine).map_err(state_read_error)?;
            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership;
        }

        // Increment the snapshot index.
        let snapshot_id = self.get_meta::<meta::SnapshotIndex>()?.unwrap_or_default() + 1;
        self.set_meta::<meta::SnapshotIndex>(&snapshot_id)?;

        // Format the actual snapshot id.
        let snapshot_id = if let Some(last_applied_log) = last_applied_log {
            format!(
                "{}-{}-{}",
                last_applied_log.leader_id, last_applied_log.index, snapshot_id
            )
        } else {
            format!("--{}", snapshot_id)
        };

        // Create the snapshot meta.
        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data,
        };

        self.set_meta::<meta::Snapshot>(&snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot.data)),
        })
    }
}

#[async_trait]
impl RaftStorage<TypeConfig> for Store {
    type SnapshotData = SnapshotList;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.set_meta::<meta::Vote>(vote)
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        self.get_meta::<meta::Vote>()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(&mut self, entries: &[&Entry<TypeConfig>]) -> StorageResult<()> {
        // Create a batch.
        let mut batch = kv::Batch::<Integer, String>::new();

        // Serialize the entries and add them to the batch.
        entries.iter().try_for_each(|entry| {
            let serialized_entry = serde_json::to_string(entry)?;
            batch.set(&Integer::from(entry.log_id.index), &serialized_entry)
        }).map_err(storage_write_error)?;

        // Insert the batch into the log bucket.
        self.db
            .bucket::<Integer, String>(Some("log"))
            .map_err(logs_write_error)?
            .batch(batch)
            .map_err(logs_write_error)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        tracing::debug!(?log_id, "Deleting conflict logs since log id");

        // Get the id of the last log entry.
        let last_log_id = self.last_log_id()?;

        // If the last log id doesn't exist, or is less than the log id to delete, return. Otherwise,
        // unwrap the last log id.
        let last_log_id = match last_log_id {
            None => return Ok(()),
            Some(x) if x < log_id => return Ok(()),
            Some(x) => x,
        };

        // Create a new batch.
        let mut batch = kv::Batch::<Integer, String>::new();

        // Iterate over the log entries to delete.
        (log_id.index..=last_log_id.index).for_each(|x| {
            let _ = batch.remove(&Integer::from(x));
        });

        // Write the batch to the database.
        self.db
            .bucket::<Integer, String>(Some("log"))
            .map_err(logs_write_error)?
            .batch(batch)
            .map_err(logs_write_error)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        // Create a new batch.
        let mut batch = kv::Batch::<Integer, String>::new();

        (0..=log_id.index).for_each(|x| {
            let _ = batch.remove(&Integer::from(x));
        });

        // Write the batch to the database.
        self.db
            .bucket::<Integer, String>(Some("log"))
            .map_err(logs_write_error)?
            .batch(batch)
            .map_err(logs_write_error)?;

        // Update the last purged log id.
        self.set_meta::<meta::LastPurged>(&log_id)
    }

    async fn last_applied_state(
        &mut self,
    ) -> StorageResult<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>)> {
        let state_machine = self.sm.read().await;
        Ok((
            state_machine.last_applied_log()?,
            state_machine.last_membership()?,
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<TypeConfig>],
    ) -> StorageResult<Vec<Response>> {
        // Get a write lock on the state machine.
        let mut state_machine = self.sm.write().await;

        // Iterate over the entries and apply them to the state machine.
        entries.iter().map(|entry| {
            tracing::debug!(%entry.log_id, "Replicating entry to state machine");

            state_machine.set_last_applied_log(entry.log_id)?;

            match entry.payload {
                EntryPayload::Blank => Ok(Response { value: None }),
                EntryPayload::Normal(ref request) => {
                    state_machine.set(request.key.clone(), request.value.clone())?;
                    Ok(Response {
                        value: Some(request.value.clone()),
                    })
                }
                EntryPayload::Membership(ref membership) => {
                    state_machine.set_last_membership(StoredMembership::new(
                        Some(entry.log_id),
                        membership.clone(),
                    ))?;
                    Ok(Response { value: None })
                }
            }
        }).collect()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<Self::SnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<Self::SnapshotData>,
    ) -> StorageResult<()> {
        tracing::debug!(?meta, "Installing snapshot");

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine, but only hold the lock as long as necessary
        {
            let new_state_machine: SerializableStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(state_read_error)?;
            let mut state_machine = self.sm.write().await;
            *state_machine = InternalStateMachine::from_serializable(new_state_machine, &self.db)?;
        }

        // Update the meta data
        self.set_meta::<meta::Snapshot>(&new_snapshot)?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> StorageResult<Option<Snapshot<NodeId, Node, Self::SnapshotData>>> {
        self.get_meta::<meta::Snapshot>()?.map_or_else(
            || Ok(None),
            |snapshot| {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            },
        )
    }
}

// ------ Meta Data for the Store ------ //
mod meta {
    use super::*;

    pub trait StoreMetadata {
        // Key used in the key-value store.
        const KEY: &'static str;

        // The value to store.
        type Value: serde::Serialize + serde::de::DeserializeOwned;

        // The subject of the storage error
        fn subject(v: Option<&Self::Value>) -> ErrorSubject<NodeId>;
    }

    pub struct LastPurged;

    pub struct SnapshotIndex;

    pub struct Vote;

    pub struct Snapshot;

    impl StoreMetadata for LastPurged {
        const KEY: &'static str = "last_purged_log_id";
        type Value = LogId<NodeId>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<NodeId> {
            ErrorSubject::Store
        }
    }

    impl StoreMetadata for SnapshotIndex {
        const KEY: &'static str = "snapshot_index";
        type Value = u64;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<NodeId> {
            ErrorSubject::Store
        }
    }

    impl StoreMetadata for Vote {
        const KEY: &'static str = "vote";
        type Value = openraft::Vote<NodeId>;

        fn subject(_v: Option<&Self::Value>) -> ErrorSubject<NodeId> {
            ErrorSubject::Vote
        }
    }

    impl StoreMetadata for Snapshot {
        const KEY: &'static str = "snapshot";
        type Value = StoredSnapshot;

        fn subject(v: Option<&Self::Value>) -> ErrorSubject<NodeId> {
            ErrorSubject::Snapshot(v.map_or_else(
                || SnapshotMeta::<NodeId, Node>::default().signature(),
                |v| v.meta.signature(),
            ))
        }
    }
}
