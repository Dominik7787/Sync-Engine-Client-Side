use serde_json::Value;

pub fn should_overwrite(local_hlc: &str, remote_hlc: &str) -> bool {
    parse_hlc(local_hlc) > parse_hlc(remote_hlc)
}

pub fn parse_hlc(s: &str) -> (i128, i64, String) {
    let mut parts = s.splitn(3, '-');
    let ms = parts.next().unwrap_or("0").parse::<i128>().unwrap_or(0);
    let ctr = parts.next().unwrap_or("0").parse::<i64>().unwrap_or(0);
    let origin = parts.next().unwrap_or("");
    (ms, ctr, origin)
}

pub fn lww_merge_row(local: &Value, remote: &Value, changed_fields: Option<&[str]>) -> Value {
    match changed_fields {
        None => remote.clone(),
        Some(fields) => {
            let mut out = local.clone();
            for k in fields {
                if let Some(v) = remote.get(*k) {
                    if let Some(obj) = out.as_object_mut() {
                        obj.insert((*k).to_string(), v.clone());
                    }
                }
            }
            out
        }
    }
}