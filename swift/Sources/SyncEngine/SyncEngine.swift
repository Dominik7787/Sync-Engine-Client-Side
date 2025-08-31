import Foundation
import SyncEngineCore

public final class SyncConnection {
    private var handle: UnsafeMutablePointer<SyncConnHandle>?

    public init?(path: String) {
        path.withCString { cstr in
            self.handle = sync_open(cstr)
        }
        if handle == nil { return nil }
    }

    deinit {
        if let h = handle { sync_close(h) }
    }

    public func initSchema() throws {
        if sync_init_schema(handle) != 0 { throw NSError(domain: "SyncEngine", code: 1) }
    }

    public func nextHlc(origin: String) -> String? {
        var out: UnsafeMutablePointer<CChar>? = nil
        origin.withCString { cstr in
            out = sync_next_hlc(handle, cstr)
        }
        guard let ptr = out else { return nil }
        let s = String(cString: ptr)
        sync_string_free(ptr)
        return s
    }

    public func logInsertFullrow(table: String, rowId: String, newRowJSON: String, origin: String) -> Int64? {
        var id: Int64 = -1
        table.withCString { t in
            rowId.withCString { r in
                newRowJSON.withCString { j in
                    origin.withCString { o in
                        id = sync_log_insert_fullrow(handle, t, r, j, o)
                    }
                }
            }
        }
        return id >= 0 ? id : nil
    }

    public func logUpdate(table: String, rowId: String, columnsJSON: String?, newRowJSON: String?, oldRowJSON: String?, origin: String) -> Int64? {
        var id: Int64 = -1
        table.withCString { t in
            rowId.withCString { r in
                origin.withCString { o in
                    id = withOptionalCString(columnsJSON) { c in
                        withOptionalCString(newRowJSON) { n in
                            withOptionalCString(oldRowJSON) { ojson in
                                sync_log_update(handle, t, r, c, n, ojson, o)
                            }
                        }
                    }
                }
            }
        }
        return id >= 0 ? id : nil
    }

    public func logDelete(table: String, rowId: String, origin: String) -> Int64? {
        var id: Int64 = -1
        table.withCString { t in
            rowId.withCString { r in
                origin.withCString { o in
                    id = sync_log_delete(handle, t, r, o)
                }
            }
        }
        return id >= 0 ? id : nil
    }

    public func getPendingOpsJSON(limit: Int64) -> String? {
        let ptr = sync_get_pending_ops_json(handle, limit)
        guard let p = ptr else { return nil }
        let s = String(cString: p)
        sync_string_free(p)
        return s
    }

    public func markOpsAcked(_ ids: [Int64]) throws {
        let res = ids.withUnsafeBufferPointer { buf in
            sync_mark_ops_acked(handle, buf.baseAddress, UInt(buf.count))
        }
        if res != 0 { throw NSError(domain: "SyncEngine", code: Int(res)) }
    }

    public func getRemoteCursor() -> String? {
        let ptr = sync_get_remote_cursor(handle)
        guard let p = ptr else { return nil }
        let s = String(cString: p)
        sync_string_free(p)
        return s.isEmpty ? nil : s
    }

    public func setRemoteCursor(_ cursor: String) throws {
        let res = cursor.withCString { c in
            sync_set_remote_cursor(handle, c)
        }
        if res != 0 { throw NSError(domain: "SyncEngine", code: Int(res)) }
    }
}

@inline(__always)
private func withOptionalCString<T>(_ s: String?, _ body: (UnsafePointer<CChar>?) -> T) -> T {
    if let s = s {
        return s.withCString { body($0) }
    } else {
        return body(nil)
    }
}


