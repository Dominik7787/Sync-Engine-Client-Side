use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Logical operation type captured in the oplog.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OpType {
    Insert,
    Update,
    Delete,
}

impl OpType {
    pub fn as_str(self) -> &'static str {
        match self {
            OpType::Insert => "INSERT",
            OpType::Update => "UPDATE",
            OpType::Delete => "DELETE",
        }
    }
}

/// Local change recorded by the client oplog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub change_id: i64,                     // AUTOINCREMENT identity
    pub table_name: String,                 // e.g., "trips"
    pub row_id: String,                     // primary key value (stringified)
    pub op_type: OpType,                    // Insert/Update/Delete
    pub columns: Option<serde_json::Value>, // JSON array of changed columns (optional)
    pub new_row: Option<serde_json::Value>, // JSON snapshot after op (null for Delete)
    pub old_row: Option<serde_json::Value>, // JSON snapshot before op (optional)
    pub hlc: String,                        // hybrid/logical clock token
    pub origin: String,                     // stable client id
    pub sync_status: String,                // 'pending' | 'pushed' | 'acked'
}

/// Remote op pulled from the server feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteOp {
    pub remote_id: String, // server-assigned unique id (cursorable)
    pub table_name: String,
    pub row_id: String,
    pub op_type: OpType,
    pub columns: Option<serde_json::Value>,
    pub new_row: Option<serde_json::Value>,
    pub old_row: Option<serde_json::Value>,
    pub hlc: String,
    pub origin: String,
}

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid state: {0}")]
    State(&'static str),
}

/// Trait implemented by the host to apply a remote op into domain tables.
/// This keeps the engine schema-agnostic.
pub trait ApplyDomainOp {
    fn apply(&self, tx: &Transaction<'_>, op: &RemoteOp) -> Result<(), SyncError>;
}

/// SyncEngine encapsulates connection and common operations.
pub struct SyncEngine<'c> {
    conn: &'c Connection,
}

impl<'c> SyncEngine<'c> {
    /// Bind the engine to an existing SQLite connection.
    pub fn new(conn: &'c Connection) -> Result<Self, SyncError> {
        Ok(Self { conn })
    }

    /// Create required metadata tables and indexes.
    /// Safe to call multiple times.
    pub fn init_schema(&self) -> Result<(), SyncError> {
        self.conn.execute_batch(
            r#"
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS local_changes (
change_id INTEGER PRIMARY KEY AUTOINCREMENT,
table_name TEXT NOT NULL,
row_id TEXT NOT NULL,
op_type TEXT NOT NULL CHECK(op_type IN ('INSERT','UPDATE','DELETE')),
columns TEXT, -- JSON array (nullable)
new_row TEXT, -- JSON (nullable for DELETE)
old_row TEXT, -- JSON (optional)
hlc TEXT NOT NULL,
origin TEXT NOT NULL,
sync_status TEXT NOT NULL DEFAULT 'pending' CHECK(sync_status IN ('pending','pushed','acked')),
UNIQUE(hlc, origin) -- idempotency for local generation
);

CREATE INDEX IF NOT EXISTS idx_local_changes_status
ON local_changes(sync_status, change_id);

CREATE TABLE IF NOT EXISTS applied_remote_ops (
remote_id TEXT PRIMARY KEY,
applied_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_kv (
k TEXT PRIMARY KEY,
v TEXT NOT NULL
);
"#,
        )?;
        // Ensure a schema version exists; default to 1
        self.conn.execute(
            "INSERT INTO sync_kv(k,v) VALUES('schema_version','1')
ON CONFLICT(k) DO NOTHING",
            [],
        )?;
        Ok(())
    }

    /// Generate a monotonic HLC token "millis-counter-origin".
    /// Stored in sync_kv: hlc_last_ms, hlc_last_ctr.
    pub fn next_hlc(&self, origin: &str) -> Result<String, SyncError> {
        let now_ms: i64 = Utc::now().timestamp_millis();
        let tx = self.conn.unchecked_transaction()?;
        let last_ms: i64 = tx
            .query_row("SELECT v FROM sync_kv WHERE k='hlc_last_ms'", [], |r| {
                r.get::<_, String>(0).map(|s| s.parse::<i64>().unwrap_or(0))
            })
            .optional()?
            .unwrap_or(0);

        let ctr: i64 = tx
            .query_row("SELECT v FROM sync_kv WHERE k='hlc_last_ctr'", [], |r| {
                r.get::<_, String>(0).map(|s| s.parse::<i64>().unwrap_or(0))
            })
            .optional()?
            .unwrap_or(0);

        let (next_ms, next_ctr) = if now_ms > last_ms {
            (now_ms, 0)
        } else {
            (last_ms, ctr + 1)
        };

        // persist
        tx.execute(
            "INSERT INTO sync_kv(k,v) VALUES('hlc_last_ms',?1)
ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            params![next_ms.to_string()],
        )?;
        tx.execute(
            "INSERT INTO sync_kv(k,v) VALUES('hlc_last_ctr',?1)
ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            params![next_ctr.to_string()],
        )?;
        tx.commit()?;

        Ok(format!("{}-{}-{}", next_ms, next_ctr, origin))
    }

    /// Insert a local change. Use the convenience wrappers below for common ops.
    pub fn log_local_change(
        &self,
        table_name: &str,
        row_id: &str,
        op_type: OpType,
        columns: Option<&serde_json::Value>,
        new_row: Option<&serde_json::Value>,
        old_row: Option<&serde_json::Value>,
        hlc: &str,
        origin: &str,
    ) -> Result<i64, SyncError> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO local_changes
(table_name,row_id,op_type,columns,new_row,old_row,hlc,origin,sync_status)
VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'pending')",
            params![
                table_name,
                row_id,
                op_type.as_str(),
                columns.map(|v| v.to_string()),
                new_row.map(|v| v.to_string()),
                old_row.map(|v| v.to_string()),
                hlc,
                origin,
            ],
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(id)
    }

    /// Convenience: record a local INSERT with a full-row snapshot.
    pub fn log_insert_fullrow(
        &self,
        table_name: &str,
        row_id: &str,
        new_row: &serde_json::Value,
        origin: &str,
    ) -> Result<i64, SyncError> {
        let hlc = self.next_hlc(origin)?;
        self.log_local_change(
            table_name,
            row_id,
            OpType::Insert,
            None,
            Some(new_row),
            None,
            &hlc,
            origin,
        )
    }

    /// Convenience: record a local UPDATE (field-level list in `columns`, and new/old snapshots if available).
    pub fn log_update(
        &self,
        table_name: &str,
        row_id: &str,
        columns: Option<&serde_json::Value>, // e.g., ["category","name"]
        new_row: Option<&serde_json::Value>,
        old_row: Option<&serde_json::Value>,
        origin: &str,
    ) -> Result<i64, SyncError> {
        let hlc = self.next_hlc(origin)?;
        self.log_local_change(
            table_name,
            row_id,
            OpType::Update,
            columns,
            new_row,
            old_row,
            &hlc,
            origin,
        )
    }

    /// Convenience: record a local DELETE.
    pub fn log_delete(
        &self,
        table_name: &str,
        row_id: &str,
        origin: &str,
    ) -> Result<i64, SyncError> {
        let hlc = self.next_hlc(origin)?;
        self.log_local_change(
            table_name,
            row_id,
            OpType::Delete,
            None,
            None,
            None,
            &hlc,
            origin,
        )
    }

    /// Fetch pending local changes that must be pushed.
    pub fn get_pending_ops(&self, limit: i64) -> Result<Vec<Change>, SyncError> {
        let mut stmt = self.conn.prepare(
"SELECT change_id, table_name, row_id, op_type, columns, new_row, old_row, hlc, origin, sync_status
FROM local_changes
WHERE sync_status='pending'
ORDER BY change_id ASC
LIMIT ?1",
)?;

        let rows = stmt.query_map(params![limit], |r| {
            let op_str: String = r.get(3)?;
            let to_json = |idx| -> rusqlite::Result<Option<serde_json::Value>> {
                let s: Option<String> = r.get(idx)?;
                Ok(match s {
                    Some(raw) => Some(
                        serde_json::from_str::<serde_json::Value>(&raw)
                            .unwrap_or(serde_json::Value::Null),
                    ),
                    None => None,
                })
            };

            Ok(Change {
                change_id: r.get(0)?,
                table_name: r.get(1)?,
                row_id: r.get(2)?,
                op_type: match op_str.as_str() {
                    "INSERT" => OpType::Insert,
                    "UPDATE" => OpType::Update,
                    "DELETE" => OpType::Delete,
                    _ => OpType::Update,
                },
                columns: to_json(4)?,
                new_row: to_json(5)?,
                old_row: to_json(6)?,
                hlc: r.get(7)?,
                origin: r.get(8)?,
                sync_status: r.get(9)?,
            })
        })?;

        let mut out = Vec::new();
        for ch in rows {
            out.push(ch?);
        }
        Ok(out)
    }

    /// Mark a set of local changes as 'pushed' (server accepted receipt).
    pub fn mark_ops_pushed(&self, ids: &[i64]) -> Result<(), SyncError> {
        let tx = self.conn.unchecked_transaction()?;
        for id in ids {
            tx.execute(
                "UPDATE local_changes SET sync_status='pushed' WHERE change_id=?1",
                params![id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Mark a set of local changes as 'acked' (server has canonically applied them).
    pub fn mark_ops_acked(&self, ids: &[i64]) -> Result<(), SyncError> {
        let tx = self.conn.unchecked_transaction()?;
        for id in ids {
            tx.execute(
                "UPDATE local_changes SET sync_status='acked' WHERE change_id=?1",
                params![id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Apply a batch of remote operations transactionally and idempotently.
    /// - Uses `applied_remote_ops` to skip duplicates.
    /// - Delegates actual domain table writes to `applier`.
    pub fn apply_remote_ops<A: ApplyDomainOp>(
        &self,
        ops: &[RemoteOp],
        applier: &A,
    ) -> Result<(), SyncError> {
        let tx = self.conn.unchecked_transaction()?;
        for op in ops {
            let seen = tx
                .query_row(
                    "SELECT 1 FROM applied_remote_ops WHERE remote_id=?1",
                    params![&op.remote_id],
                    |_r| Ok(()),
                )
                .optional()?;
            if seen.is_some() {
                continue; // idempotent skip
            }

            applier.apply(&tx, op)?;

            let now_ms = Utc::now().timestamp_millis();
            tx.execute(
                "INSERT INTO applied_remote_ops(remote_id, applied_ms) VALUES(?1, ?2)",
                params![&op.remote_id, now_ms],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Get or set the last remote cursor (server-side checkpoint).
    pub fn get_remote_cursor(&self) -> Result<Option<String>, SyncError> {
        let cur: Option<String> = self
            .conn
            .query_row("SELECT v FROM sync_kv WHERE k='remote_cursor'", [], |r| {
                r.get(0)
            })
            .optional()?;
        Ok(cur)
    }
    pub fn set_remote_cursor(&self, cursor: &str) -> Result<(), SyncError> {
        self.conn.execute(
            "INSERT INTO sync_kv(k,v) VALUES('remote_cursor',?1)
            ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            params![cursor],
        )?;
        Ok(())
    }

    /// Return the current integer schema version stored in `sync_kv`.
    pub fn get_schema_version(&self) -> Result<i32, SyncError> {
        let ver: Option<String> = self
            .conn
            .query_row("SELECT v FROM sync_kv WHERE k='schema_version'", [], |r| r.get(0))
            .optional()?;
        Ok(ver.and_then(|s| s.parse::<i32>().ok()).unwrap_or(1))
    }

    /// Run migrations up to `target_version` transactionally.
    /// This placeholder uses no-op steps and only bumps the stored version.
    /// Domain-specific migrations can be wired here in the future.
    pub fn run_migrations(&self, target_version: i32) -> Result<(), SyncError> {
        if target_version < 1 {
            return Err(SyncError::State("invalid target_version"));
        }
        let current = self.get_schema_version()?;
        if current >= target_version { return Ok(()); }

        let tx = self.conn.unchecked_transaction()?;
        // Apply stepwise migrations here as needed.
        // For now, we just advance the version without schema changes.
        tx.execute(
            "INSERT INTO sync_kv(k,v) VALUES('schema_version',?1)
ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            params![target_version.to_string()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Execute closure `f` inside a transaction and commit if `f` returns Ok.
    pub fn with_tx<R, F>(&self, f: F) -> Result<R, SyncError>
    where
        F: FnOnce(&rusqlite::Transaction<'_>) -> Result<R, SyncError>,
    {
        let tx = self.conn.unchecked_transaction()?;
        let result = f(&tx)?;
        tx.commit()?;
        Ok(result)
    }
}
