import Foundation
import SyncEngineCore

public final class SyncConnection {
    private var handle: OpaquePointer?

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

    public func markOpsPushed(_ ids: [Int64]) throws {
        let res = ids.withUnsafeBufferPointer { buf in
            sync_mark_ops_pushed(handle, buf.baseAddress, UInt(buf.count))
        }
        if res != 0 { throw NSError(domain: "SyncEngine", code: Int(res)) }
    }

    public func getSchemaVersion() throws -> Int32 {
        var v: Int32 = 0
        let rc = withUnsafeMutablePointer(to: &v) { ptr in
            sync_get_schema_version(handle, ptr)
        }
        if rc != 0 { throw NSError(domain: "SyncEngine", code: Int(rc)) }
        return v
    }

    public func runMigrations(targetVersion: Int32) throws {
        let rc = sync_run_migrations(handle, targetVersion)
        if rc != 0 { throw NSError(domain: "SyncEngine", code: Int(rc)) }
    }

    public struct RemoteOp {
        public enum OpType: Int32 { case insert = 0, update = 1, delete = 2 }
        public var remoteId: String
        public var table: String
        public var rowId: String
        public var opType: OpType
        public var columnsJSON: String?
        public var newRowJSON: String?
        public var oldRowJSON: String?
        public var hlc: String
        public var origin: String
    }

    public typealias ApplyCallback = (_ op: RemoteOp) -> Int32

    public func applyRemoteOps(_ ops: [RemoteOp], callback: ApplyCallback) throws {
        // Trampoline capturing Swift closure and bridging to C callback signature
        class Box { let cb: ApplyCallback; init(_ cb: @escaping ApplyCallback) { self.cb = cb } }
        let box = Box(callback)
        let unmanaged = Unmanaged.passRetained(box)
        defer { unmanaged.release() }

        let cCallback: (@convention(c) (UnsafeMutableRawPointer?, UnsafePointer<SE_Op>?) -> Int32) = { userData, cOpPtr in
            guard let userData = userData, let cOp = cOpPtr?.pointee else { return 3 }
            let box = Unmanaged<Box>.fromOpaque(userData).takeUnretainedValue()
            func str(_ p: UnsafePointer<CChar>?) -> String { p != nil ? String(cString: p!) : "" }
            let op = RemoteOp(
                remoteId: str(cOp.remote_id),
                table: str(cOp.table_name),
                rowId: str(cOp.row_id),
                opType: RemoteOp.OpType(rawValue: cOp.op_type) ?? .update,
                columnsJSON: cOp.columns_json != nil ? str(cOp.columns_json) : nil,
                newRowJSON: cOp.new_row_json != nil ? str(cOp.new_row_json) : nil,
                oldRowJSON: cOp.old_row_json != nil ? str(cOp.old_row_json) : nil,
                hlc: str(cOp.hlc),
                origin: str(cOp.origin)
            )
            return box.cb(op)
        }

        var cOps: [SE_Op] = []
        cOps.reserveCapacity(ops.count)
        // Build C views using withCString lifetimes inside a nested scope to ensure pointers stay valid during the call.
        try ops.withUnsafeBufferPointer { _ in
            var storage: [(UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafePointer<CChar>?)] = []
            storage.reserveCapacity(ops.count)
            for op in ops {
                var rid: UnsafePointer<CChar>? = nil
                var tbl: UnsafePointer<CChar>? = nil
                var row: UnsafePointer<CChar>? = nil
                var cols: UnsafePointer<CChar>? = nil
                var nrow: UnsafePointer<CChar>? = nil
                var orow: UnsafePointer<CChar>? = nil
                var hlc: UnsafePointer<CChar>? = nil
                var orig: UnsafePointer<CChar>? = nil
                op.remoteId.withCString { rid = $0 }
                op.table.withCString { tbl = $0 }
                op.rowId.withCString { row = $0 }
                if let cj = op.columnsJSON { cj.withCString { cols = $0 } }
                if let nj = op.newRowJSON { nj.withCString { nrow = $0 } }
                if let oj = op.oldRowJSON { oj.withCString { orow = $0 } }
                op.hlc.withCString { hlc = $0 }
                op.origin.withCString { orig = $0 }
                storage.append((rid, tbl, row, cols, nrow, orow, hlc))
                cOps.append(SE_Op(remote_id: rid, table_name: tbl, row_id: row, op_type: op.opType.rawValue, columns_json: cols, new_row_json: nrow, old_row_json: orow, hlc: hlc, origin: orig))
            }
            let rc = cOps.withUnsafeBufferPointer { buf in
                sync_apply_remote_ops(handle, buf.baseAddress, UInt(buf.count), cCallback, unmanaged.toOpaque())
            }
            if rc != 0 { throw NSError(domain: "SyncEngine", code: Int(rc)) }
        }
    }

    public func txExecCurrent(sql: String) throws {
        let rc = sql.withCString { c in
            sync_tx_exec_current(c)
        }
        if rc != 0 { throw NSError(domain: "SyncEngine", code: Int(rc)) }
    }
    public func lastError() -> (code: Int32, message: String) {
        let code = sync_last_error_code()
        let ptr = sync_last_error_message()
        if let p = ptr {
            let s = String(cString: p)
            sync_string_free(p)
            return (code, s)
        }
        return (code, "")
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


