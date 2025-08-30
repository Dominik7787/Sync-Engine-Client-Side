use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

use crate::oplog::SyncEngine;

/// Opaque handle that owns a SQLite connection.
/// Swift/Objective-C hold this as an unsafe pointer and pass it back to Rust APIs.
pub struct SyncConnHandle {
    conn: rusqlite::Connection,
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
        Ok(conn) => Box::into_raw(Box::new(SyncConnHandle { conn })),
        Err(_) => std::ptr::null_mut(),
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
            Ok(_) => 0,
            Err(_) => 1,
        }
    } else {
        2
    }
}

/// Generate next HLC token for an origin. Returns newly allocated C string or null on error.
#[unsafe(no_mangle)]
pub extern "C" fn sync_next_hlc(handle: *mut SyncConnHandle, origin: *const c_char) -> *mut c_char {
    let h = unsafe { handle.as_mut() };
    let origin = match ptr_to_str(origin) { Ok(s) => s, Err(_) => return std::ptr::null_mut() };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(_) => return std::ptr::null_mut() };
        match engine.next_hlc(origin) {
            Ok(s) => to_cstring_ptr(&s),
            Err(_) => std::ptr::null_mut(),
        }
    } else {
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
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(_) => return std::ptr::null_mut() };
        match engine.get_pending_ops(limit) {
            Ok(changes) => match serde_json::to_string(&changes) {
                Ok(s) => to_cstring_ptr(&s),
                Err(_) => std::ptr::null_mut(),
            },
            Err(_) => std::ptr::null_mut(),
        }
    } else { std::ptr::null_mut() }
}

/// Mark provided change ids as acked. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn sync_mark_ops_acked(handle: *mut SyncConnHandle, ids: *const i64, len: usize) -> c_int {
    let h = unsafe { handle.as_mut() };
    if h.is_none() { return 2; }
    if ids.is_null() && len > 0 { return 3; }
    let slice = unsafe { std::slice::from_raw_parts(ids, len) };
    let h = h.unwrap();
    let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(_) => return 1 };
    match engine.mark_ops_acked(slice) { Ok(_) => 0, Err(_) => 1 }
}

/// Get the remote cursor if set. Returns empty string if not set, null on error.
#[unsafe(no_mangle)]
pub extern "C" fn sync_get_remote_cursor(handle: *mut SyncConnHandle) -> *mut c_char {
    let h = unsafe { handle.as_mut() };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(_) => return std::ptr::null_mut() };
        match engine.get_remote_cursor() {
            Ok(Some(s)) => to_cstring_ptr(&s),
            Ok(None) => to_cstring_ptr(""),
            Err(_) => std::ptr::null_mut(),
        }
    } else { std::ptr::null_mut() }
}

/// Set the remote cursor. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn sync_set_remote_cursor(handle: *mut SyncConnHandle, cursor: *const c_char) -> c_int {
    let h = unsafe { handle.as_mut() };
    let cursor = match ptr_to_str(cursor) { Ok(s) => s, Err(_) => return 3 };
    if let Some(h) = h {
        let engine = match SyncEngine::new(&h.conn) { Ok(e) => e, Err(_) => return 1 };
        match engine.set_remote_cursor(cursor) { Ok(_) => 0, Err(_) => 1 }
    } else { 2 }
}


