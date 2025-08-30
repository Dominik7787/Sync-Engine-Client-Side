use crate::oplog::{ApplyDomainOp, Change, RemoteOp, SyncEngine, SyncError};


pub struct SyncClient<'c, A> {
    engine: SyncEngine<'c>,
    applier: A,
    // origin: String,
}

impl<'c, A: ApplyDomainOp> SyncClient<'c, A> {
    pub fn new(conn: &'c rusqlite::Connection, applier: A) -> Result<Self, SyncError> {
        let engine = SyncEngine::new(conn)?;
        engine.init_schema()?;
        Ok(Self { engine, applier})
    }
}

impl<'c, A: ApplyDomainOp> SyncClient<'c, A> {
    /// Run one full sync cycle (push all local changes to the server, pull all remote changes).
    pub fn sync_cycle<P, G>(&self, push: P, pull: G, limit: i64) -> Result<(), SyncError>
    where
        P: Fn(&[Change]) -> Result<Vec<i64>, SyncError>, // Push local ops -> return acked ids
        G: Fn(Option<String>) -> Result<(Vec<RemoteOp>, Option<String>), SyncError>, // pull: cursor -> (ops, new_cursor)
    {
        // 1. Push local changes to the server
        let locals = self.engine.get_pending_ops(limit)?;
        if !locals.is_empty() {
            let acked_ids = push(&locals)?;
            self.engine.mark_ops_acked(&acked_ids)?;
        }

        // 2. Pull remote changes from the server
        let cursor = self.engine.get_remote_cursor()?;
        let (remote_ops, new_cursor) = pull(cursor)?;
        if !remote_ops.is_empty() {
            self.engine.apply_remote_ops(&remote_ops, &self.applier)?;
        }
        if let Some(c) = new_cursor {
            self.engine.set_remote_cursor(&c)?;
        }

        Ok(())
    }
}