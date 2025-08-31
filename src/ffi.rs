use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

use std::mem::transmute;

use crate::oplog::{OpType, RemoteOp, SyncEngine, SyncError};
use rusqlite::OptionalExtension;

/// Opaque handle that owns a SQLite connection.
/// Swift/Objective-C hold this as an unsafe pointer and pass it back to Rust APIs.
pub struct SyncConnHandle {
    conn: rusqlite::Connection,
}

thread_local! {
    static LAST_ERROR: RefCell<(i32, String)> = RefCell::new((0, String::new()));
}

fn set_last_error(code: i32, msg: &str) { LAST_ERROR.with(|le| *le.borrow_mut() = (code, msg.to_string())); }
fn clear_last_error() { LAST_ERROR.with(|le| *le.borrow_mut() = (0, String::new())); }

#[repr(C)]
pub struct SE_Op {
    pub remote_id: *const c_char,
    pub table_name: *const c_char,
    pub row_id: *const c_char,
    pub op_type: i32, // 0 Insert,1 Update,2 Delete
    pub columns_json: *const c_char, // nullable
    pub new_row_json: *const c_char, // nullable
    pub old_row_json: *const c_char, // nullable
    pub hlc: *const c_char,
    pub origin: *const c_char,
}

pub type SE_ApplyCallback = Option<extern "C" fn(user_data: *mut c_void, op: *const SE_Op) -> c_int>;

thread_local! {
    static TLS_TX_PTR: RefCell<*mut rusqlite::Transaction<'static>> = RefCell::new(std::ptr::null_mut());
}

fn ptr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, ()> {
    if ptr.is_null() {
        return Err(());
    }
    unsafe { CStr::from_ptr(ptr).to_str().map_err(|_| ()) }
}

fn opt_ptr_to_str<'a>(ptr: *const c_char) -> Result<Option<&'a str>, ()> {
    if ptr.is_null() {
        return Ok(None);
    }
    Ok(Some(ptr_to_str(ptr)?))
}

fn to_cstring_ptr(s: &str) -> *mut c_char {
    CString::new(s).map(|cs| cs.into_raw()).unwrap_or(std::ptr::null_mut())
}

/// Close and free a C string returned by this library.
#[unsafe(no_mangle)]
pub extern "C" fn sync_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    unsafe { let _ = CString::from_raw(s); }
}

/// Open a SQLite connection. Path can be file path or ":memory:".
/// Returns null on failure.
#[unsafe(no_mangle)]
pub extern "C" fn sync_open(path: *const c_char) -> *mut SyncConnHandle {
    let path = match ptr_to_str(path) {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    match rusqlite::Connection::open(path) {
        Ok(conn) => {
            clear_last_error();
            Box::into_raw(Box::new(SyncConnHandle { conn }))
        },
        Err(e) => { set_last_error(1, &format!("sqlite: {}", e)); std::ptr::null_mut() },
    }
}

/// Close a previously opened connection.
#[unsafe(no_mangle)]
pub extern "C" fn sync_close(handle: *mut SyncConnHandle) {
    if handle.is_null() {
        return;
    }
    unsafe { let _ = Box::from_raw(handle); }
}

/// Initialize required metadata tables. Returns 0 on success, non-zero on error.
#[unsafe(no_mangle)]
pub extern "C" fn sync_init_schema(handle: *mut SyncConnHandle) -> c_int {
    let h = unsafe { handle.as_mut() };
    if let Some(h) = h {
        let engine = SyncEngine::new(&h.conn);
        match engine.and_then(|e| e.init_schema()) {
            Ok(_) => { clear_last_error(); 0 },
            Err(e) => { set_last_error(1, &format!("{}", e)); 1 },
        }
    } else {
        set_last_error(4, "null handle");
        2
    }
}

/// Generate next HLC token for an origin. Returns newly allocated C string or null on error.
#[unsafe(no_mangle)]
pub extern "C" fn sync_next_hlc(handle: *mut SyncConnHandle, origin: *const c_char) -> *mut c_char {
    let h = unsafe { handle.as_mut() };
    let origin = match ptr_to_str(origin) { Ok(s) => s, Err(_) => { set_last_error(4, "invalid origin"); return std::ptr::null_mut() } };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(e) => { set_last_error(1, &format!("{}", e)); return std::ptr::null_mut() } };
        match engine.next_hlc(origin) {
            Ok(s) => { clear_last_error(); to_cstring_ptr(&s) },
            Err(e) => { set_last_error(1, &format!("{}", e)); std::ptr::null_mut() },
        }
    } else {
        set_last_error(4, "null handle");
        std::ptr::null_mut()
    }
}

/// Log an INSERT with a full-row JSON snapshot. Returns change_id (>=1) or -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn sync_log_insert_fullrow(
    handle: *mut SyncConnHandle,
    table_name: *const c_char,
    row_id: *const c_char,
    new_row_json: *const c_char,
    origin: *const c_char,
) -> i64 {
    let h = unsafe { handle.as_mut() };
    let (table_name, row_id, origin) = match (
        ptr_to_str(table_name),
        ptr_to_str(row_id),
        ptr_to_str(origin),
    ) {
        (Ok(a), Ok(b), Ok(c)) => (a, b, c),
        _ => return -1,
    };
    let new_row_s = match ptr_to_str(new_row_json) { Ok(s) => s, Err(_) => return -1 };
    let new_row_v: serde_json::Value = match serde_json::from_str(new_row_s) { Ok(v) => v, Err(_) => return -1 };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(_) => return -1 };
        match engine.log_insert_fullrow(table_name, row_id, &new_row_v, origin) { Ok(id) => id, Err(_) => -1 }
    } else { -1 }
}

/// Log an UPDATE with optional fields and snapshots. Returns change_id or -1.
#[unsafe(no_mangle)]
pub extern "C" fn sync_log_update(
    handle: *mut SyncConnHandle,
    table_name: *const c_char,
    row_id: *const c_char,
    columns_json: *const c_char,   // nullable
    new_row_json: *const c_char,   // nullable
    old_row_json: *const c_char,   // nullable
    origin: *const c_char,
) -> i64 {
    let h = unsafe { handle.as_mut() };
    let (table_name, row_id, origin) = match (
        ptr_to_str(table_name),
        ptr_to_str(row_id),
        ptr_to_str(origin),
    ) {
        (Ok(a), Ok(b), Ok(c)) => (a, b, c),
        _ => return -1,
    };
    let columns_v: Option<serde_json::Value> = match opt_ptr_to_str(columns_json) {
        Ok(Some(s)) => match serde_json::from_str(s) { Ok(v) => Some(v), Err(_) => return -1 },
        Ok(None) => None,
        Err(_) => return -1,
    };
    let new_row_v: Option<serde_json::Value> = match opt_ptr_to_str(new_row_json) {
        Ok(Some(s)) => match serde_json::from_str(s) { Ok(v) => Some(v), Err(_) => return -1 },
        Ok(None) => None,
        Err(_) => return -1,
    };
    let old_row_v: Option<serde_json::Value> = match opt_ptr_to_str(old_row_json) {
        Ok(Some(s)) => match serde_json::from_str(s) { Ok(v) => Some(v), Err(_) => return -1 },
        Ok(None) => None,
        Err(_) => return -1,
    };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(_) => return -1 };
        match engine.log_update(
            table_name,
            row_id,
            columns_v.as_ref(),
            new_row_v.as_ref(),
            old_row_v.as_ref(),
            origin,
        ) { Ok(id) => id, Err(_) => -1 }
    } else { -1 }
}

/// Log a DELETE. Returns change_id or -1.
#[unsafe(no_mangle)]
pub extern "C" fn sync_log_delete(
    handle: *mut SyncConnHandle,
    table_name: *const c_char,
    row_id: *const c_char,
    origin: *const c_char,
) -> i64 {
    let h = unsafe { handle.as_mut() };
    let (table_name, row_id, origin) = match (
        ptr_to_str(table_name),
        ptr_to_str(row_id),
        ptr_to_str(origin),
    ) {
        (Ok(a), Ok(b), Ok(c)) => (a, b, c),
        _ => return -1,
    };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(_) => return -1 };
        match engine.log_delete(table_name, row_id, origin) { Ok(id) => id, Err(_) => -1 }
    } else { -1 }
}

/// Get pending ops as JSON array string. Returns newly allocated C string or null on error.
#[unsafe(no_mangle)]
pub extern "C" fn sync_get_pending_ops_json(handle: *mut SyncConnHandle, limit: i64) -> *mut c_char {
    let h = unsafe { handle.as_mut() };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(e) => { set_last_error(1, &format!("{}", e)); return std::ptr::null_mut() } };
        match engine.get_pending_ops(limit) {
            Ok(changes) => match serde_json::to_string(&changes) {
                Ok(s) => { clear_last_error(); to_cstring_ptr(&s) },
                Err(e) => { set_last_error(2, &format!("{}", e)); std::ptr::null_mut() },
            },
            Err(e) => { set_last_error(1, &format!("{}", e)); std::ptr::null_mut() },
        }
    } else { std::ptr::null_mut() }
}

/// Mark provided change ids as acked. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn sync_mark_ops_acked(handle: *mut SyncConnHandle, ids: *const i64, len: usize) -> c_int {
    let h = unsafe { handle.as_mut() };
    if h.is_none() { set_last_error(4, "null handle"); return 2; }
    if ids.is_null() && len > 0 { set_last_error(4, "ids null but len > 0"); return 3; }
    let slice = unsafe { std::slice::from_raw_parts(ids, len) };
    let h = h.unwrap();
    let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(e) => { set_last_error(1, &format!("{}", e)); return 1 } };
    match engine.mark_ops_acked(slice) { Ok(_) => { clear_last_error(); 0 }, Err(e) => { set_last_error(1, &format!("{}", e)); 1 } }
}

/// Get the remote cursor if set. Returns empty string if not set, null on error.
#[unsafe(no_mangle)]
pub extern "C" fn sync_get_remote_cursor(handle: *mut SyncConnHandle) -> *mut c_char {
    let h = unsafe { handle.as_mut() };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(e) => { set_last_error(1, &format!("{}", e)); return std::ptr::null_mut() } };
        match engine.get_remote_cursor() {
            Ok(Some(s)) => { clear_last_error(); to_cstring_ptr(&s) },
            Ok(None) => { clear_last_error(); to_cstring_ptr("") },
            Err(e) => { set_last_error(1, &format!("{}", e)); std::ptr::null_mut() },
        }
    } else { std::ptr::null_mut() }
}

/// Set the remote cursor. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn sync_set_remote_cursor(handle: *mut SyncConnHandle, cursor: *const c_char) -> c_int {
    let h = unsafe { handle.as_mut() };
    let cursor = match ptr_to_str(cursor) { Ok(s) => s, Err(_) => { set_last_error(4, "invalid cursor"); return 3 } };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(e) => { set_last_error(1, &format!("{}", e)); return 1 } };
        match engine.set_remote_cursor(cursor) { Ok(_) => { clear_last_error(); 0 }, Err(e) => { set_last_error(1, &format!("{}", e)); 1 } }
    } else { set_last_error(4, "null handle"); 2 }
}


/// Return the last error code for the current thread.
#[unsafe(no_mangle)]
pub extern "C" fn sync_last_error_code() -> c_int { LAST_ERROR.with(|le| le.borrow().0) }

/// Return the last error message for the current thread as a newly allocated C string. Caller must free with sync_string_free.
#[unsafe(no_mangle)]
pub extern "C" fn sync_last_error_message() -> *mut c_char { to_cstring_ptr(&LAST_ERROR.with(|le| le.borrow().1.clone())) }

/// Mark provided change ids as pushed. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn sync_mark_ops_pushed(handle: *mut SyncConnHandle, ids: *const i64, len: usize) -> c_int {
    let h = unsafe { handle.as_mut() };
    if h.is_none() { set_last_error(4, "null handle"); return 2; }
    if ids.is_null() && len > 0 { set_last_error(4, "ids null but len > 0"); return 3; }
    let slice = unsafe { std::slice::from_raw_parts(ids, len) };
    let h = h.unwrap();
    let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(e) => { set_last_error(1, &format!("{}", e)); return 1 } };
    match engine.mark_ops_pushed(slice) { Ok(_) => { clear_last_error(); 0 }, Err(e) => { set_last_error(1, &format!("{}", e)); 1 } }
}

/// Get the current schema version. Returns 0 on success and writes to out_version.
#[unsafe(no_mangle)]
pub extern "C" fn sync_get_schema_version(handle: *mut SyncConnHandle, out_version: *mut i32) -> c_int {
    if out_version.is_null() { set_last_error(4, "out_version is null"); return 3; }
    let h = unsafe { handle.as_mut() };
    if h.is_none() { set_last_error(4, "null handle"); return 2; }
    let h = h.unwrap();
    let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(e) => { set_last_error(1, &format!("{}", e)); return 1 } };
    match engine.get_schema_version() {
        Ok(v) => { unsafe { *out_version = v; } clear_last_error(); 0 },
        Err(e) => { set_last_error(1, &format!("{}", e)); 1 }
    }
}

/// Run migrations up to target_version. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn sync_run_migrations(handle: *mut SyncConnHandle, target_version: i32) -> c_int {
    let h = unsafe { handle.as_mut() };
    if h.is_none() { set_last_error(4, "null handle"); return 2; }
    let h = h.unwrap();
    let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(e) => { set_last_error(1, &format!("{}", e)); return 1 } };
    match engine.run_migrations(target_version) {
        Ok(_) => { clear_last_error(); 0 },
        Err(e) => { set_last_error(1, &format!("{}", e)); 1 }
    }
}

/// Execute a SQL statement inside the current transaction context, if any (used by apply callback). Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn sync_tx_exec_current(sql: *const c_char) -> c_int {
    let sql = match ptr_to_str(sql) { Ok(s) => s, Err(_) => { set_last_error(4, "invalid sql"); return 3 } };
    let mut ran = false;
    let mut err: Option<String> = None;
    TLS_TX_PTR.with(|cell| {
        let ptr = *cell.borrow();
        if ptr.is_null() { err = Some("no active transaction".to_string()); return; }
        ran = true;
        unsafe {
            match (&mut *ptr).execute_batch(sql) {
                Ok(_) => { clear_last_error(); },
                Err(e) => { set_last_error(1, &format!("{}", e)); err = Some(e.to_string()); }
            }
        }
    });
    if !ran { return 2; }
    if err.is_some() { 1 } else { 0 }
}

fn cstr_or_none<'a>(p: *const c_char) -> Result<Option<&'a str>, ()> { opt_ptr_to_str(p) }
fn str_or_fail<'a>(p: *const c_char, name: &str) -> Result<&'a str, ()> { ptr_to_str(p).map_err(|_| ()) }

fn op_from_se(op: &SE_Op) -> Result<RemoteOp, SyncError> {
    let remote_id = str_or_fail(op.remote_id, "remote_id").map_err(|_| SyncError::State("remote_id"))?.to_string();
    let table_name = str_or_fail(op.table_name, "table_name").map_err(|_| SyncError::State("table_name"))?.to_string();
    let row_id = str_or_fail(op.row_id, "row_id").map_err(|_| SyncError::State("row_id"))?.to_string();
    let op_type = match op.op_type { 0 => OpType::Insert, 1 => OpType::Update, 2 => OpType::Delete, _ => return Err(SyncError::State("invalid op_type")) };
    let columns = match cstr_or_none(op.columns_json) { Ok(Some(s)) => Some(serde_json::from_str(s)?), Ok(None) => None, Err(_) => return Err(SyncError::State("columns_json")) };
    let new_row = match cstr_or_none(op.new_row_json) { Ok(Some(s)) => Some(serde_json::from_str(s)?), Ok(None) => None, Err(_) => return Err(SyncError::State("new_row_json")) };
    let old_row = match cstr_or_none(op.old_row_json) { Ok(Some(s)) => Some(serde_json::from_str(s)?), Ok(None) => None, Err(_) => return Err(SyncError::State("old_row_json")) };
    let hlc = str_or_fail(op.hlc, "hlc").map_err(|_| SyncError::State("hlc"))?.to_string();
    let origin = str_or_fail(op.origin, "origin").map_err(|_| SyncError::State("origin"))?.to_string();
    Ok(RemoteOp { remote_id, table_name, row_id, op_type, columns, new_row, old_row, hlc, origin })
}

/// Apply a batch of remote ops transactionally. For each op, the callback is invoked; Swift may call `sync_tx_exec_current` within the callback to perform domain writes inside the same transaction. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn sync_apply_remote_ops(
    handle: *mut SyncConnHandle,
    ops: *const SE_Op,
    len: usize,
    cb: SE_ApplyCallback,
    user_data: *mut c_void,
) -> c_int {
    let h = unsafe { handle.as_mut() };
    if h.is_none() { set_last_error(4, "null handle"); return 2; }
    if ops.is_null() && len > 0 { set_last_error(4, "ops null but len > 0"); return 3; }
    let h = h.unwrap();
    let _engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(e) => { set_last_error(1, &format!("{}", e)); return 1 } };

    // Build Rust RemoteOp list first to validate inputs.
    let slice = unsafe { std::slice::from_raw_parts(ops, len) };
    let mut parsed_ops: Vec<RemoteOp> = Vec::with_capacity(len);
    for o in slice.iter() {
        match op_from_se(o) { Ok(ro) => parsed_ops.push(ro), Err(e) => { set_last_error(4, &format!("{}", e)); return 3 } }
    }

    let mut tx = match h.conn.unchecked_transaction() {
        Ok(t) => t,
        Err(e) => { set_last_error(1, &format!("{}", e)); return 1 }
    };
    // Place tx into TLS for callback to use.
    let mut tx_box = Box::new(tx);
    let tx_ptr: *mut rusqlite::Transaction<'static> = unsafe { transmute::<*mut rusqlite::Transaction<'_>, *mut rusqlite::Transaction<'static>>(&mut *tx_box) };
    TLS_TX_PTR.with(|cell| *cell.borrow_mut() = tx_ptr);

    for (idx, op) in parsed_ops.iter().enumerate() {
        // Idempotency check
        let seen = tx_box.query_row(
            "SELECT 1 FROM applied_remote_ops WHERE remote_id=?1",
            rusqlite::params![&op.remote_id],
            |_r| Ok(()),
        ).optional();
        match seen {
            Ok(Some(_)) => { continue; },
            Ok(None) => {},
            Err(e) => { TLS_TX_PTR.with(|cell| *cell.borrow_mut() = std::ptr::null_mut()); set_last_error(1, &format!("{}", e)); return 1; }
        }

        // Callback
        if let Some(func) = cb {
            // Borrow the input op directly from the C slice; do not move.
            let c_op_ptr: *const SE_Op = unsafe { slice.as_ptr().add(idx) };
            let rc = func(user_data, c_op_ptr);
            if rc != 0 { TLS_TX_PTR.with(|cell| *cell.borrow_mut() = std::ptr::null_mut()); set_last_error(3, "apply callback failed"); return rc; }
        }

        // Record as applied
        let now_ms = chrono::Utc::now().timestamp_millis();
        if let Err(e) = tx_box.execute(
            "INSERT INTO applied_remote_ops(remote_id, applied_ms) VALUES(?1, ?2)",
            rusqlite::params![&op.remote_id, now_ms],
        ) { TLS_TX_PTR.with(|cell| *cell.borrow_mut() = std::ptr::null_mut()); set_last_error(1, &format!("{}", e)); return 1; }
    }

    // Clear TLS and commit
    TLS_TX_PTR.with(|cell| *cell.borrow_mut() = std::ptr::null_mut());
    match tx_box.commit() {
        Ok(_) => { clear_last_error(); 0 },
        Err(e) => { set_last_error(1, &format!("{}", e)); 1 }
    }
}


