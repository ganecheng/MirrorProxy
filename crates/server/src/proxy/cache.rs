use super::*;
use std::sync::{Mutex, RwLock};

static CACHE_WORK: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
static CACHE_FILES: RwLock<()> = RwLock::new(());
static CAPACITY: Mutex<Option<Capacity>> = Mutex::new(None);

struct Capacity {
    directory: String,
    max_bytes: u64,
    bytes: u64,
    checked_at: Instant,
}

// Serialize writers so body/metadata pairs and incremental accounting agree.
pub(super) async fn cache_work<T: Send + 'static>(
    write: bool,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, tokio::task::JoinError> {
    let permit = CACHE_WORK
        .acquire()
        .await
        .expect("cache semaphore is never closed");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if write {
            let _guard = CACHE_FILES.write().expect("cache lock poisoned");
            work()
        } else {
            let _guard = CACHE_FILES.read().expect("cache lock poisoned");
            work()
        }
    })
    .await
}

fn maintain_capacity(cache: &CacheConfig, previous_bytes: u64, written_bytes: u64) {
    let mut capacity = CAPACITY.lock().expect("cache capacity lock poisoned");
    if let Some(current) = capacity.as_mut().filter(|current| {
        current.directory == cache.directory && current.max_bytes == max_cache_total_bytes(cache)
    }) {
        current.bytes = current
            .bytes
            .saturating_sub(previous_bytes)
            .saturating_add(written_bytes);
        if current.bytes <= current.max_bytes
            && current.checked_at.elapsed() < std::time::Duration::from_secs(30)
        {
            return;
        }
    }
    *capacity = Some(Capacity {
        directory: cache.directory.clone(),
        max_bytes: max_cache_total_bytes(cache),
        bytes: evict_disk_cache(cache),
        checked_at: Instant::now(),
    });
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct DiskCacheMetadata {
    pub(super) status: u16,
    pub(super) headers: Vec<(String, String)>,
    #[serde(default)]
    pub(super) stored_at: u64,
    #[serde(default)]
    pub(super) expires_at: u64,
    #[serde(default)]
    pub(super) vary: Vec<(String, String)>,
    /// A HEAD answer carries no body, so it may only satisfy another HEAD request.
    #[serde(default)]
    pub(super) head_only: bool,
}

/// Explicit cache write policy for answers that are not a plain GET success.
#[derive(Clone, Copy, Default)]
pub(super) struct DiskCacheWriteOptions {
    /// Persist a body-less HEAD answer; it never replaces a cached response body.
    pub(super) head_only: bool,
    /// Replaces upstream freshness, for example a negative or HEAD lifetime.
    pub(super) ttl_secs: Option<u64>,
}

pub(super) struct DiskCacheEntry {
    pub(super) body: Vec<u8>,
    pub(super) metadata: DiskCacheMetadata,
}

pub(super) fn cache_paths(cache: &CacheConfig, url: &Url) -> Option<(PathBuf, PathBuf)> {
    if !cache.enabled || cache.directory.trim().is_empty() {
        return None;
    }
    let key = format!("{:x}", Sha256::digest(url.as_str().as_bytes()));
    let root = Path::new(&cache.directory);
    Some((
        root.join(format!("{key}.body")),
        root.join(format!("{key}.json")),
    ))
}

/// Metadata-only probe, so a cache hit can be located across every configured
/// upstream before any body is allocated.
pub(super) fn load_disk_cache_metadata(
    cache: &CacheConfig,
    url: &Url,
    incoming_headers: &HeaderMap,
) -> Option<DiskCacheMetadata> {
    let (_, metadata_path) = cache_paths(cache, url)?;
    let metadata: DiskCacheMetadata =
        serde_json::from_slice(&fs::read(metadata_path).ok()?).ok()?;
    vary_matches(&metadata, incoming_headers).then_some(metadata)
}

fn vary_matches(metadata: &DiskCacheMetadata, incoming_headers: &HeaderMap) -> bool {
    !metadata.vary.iter().any(|(name, expected)| {
        incoming_headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            != expected
    })
}

pub(super) fn load_disk_cache(
    cache: &CacheConfig,
    url: &Url,
    incoming_headers: &HeaderMap,
) -> Option<DiskCacheEntry> {
    let (body_path, metadata_path) = cache_paths(cache, url)?;
    let metadata: DiskCacheMetadata =
        serde_json::from_slice(&fs::read(metadata_path).ok()?).ok()?;
    if !vary_matches(&metadata, incoming_headers) {
        return None;
    }
    // Avoid allocating the body for mismatched variants, and reject oversized files.
    if fs::metadata(&body_path).ok()?.len() > max_cache_entry_bytes(cache) {
        return None;
    }
    let body = fs::read(&body_path).ok()?;
    let _ = fs::OpenOptions::new()
        .write(true)
        .open(&body_path)
        .and_then(|file| file.set_modified(SystemTime::now()));
    Some(DiskCacheEntry { body, metadata })
}

/// A HEAD-only entry holds no body, so it may only answer another HEAD request.
pub(super) fn cache_entry_serves(entry: &DiskCacheEntry, method: &Method) -> bool {
    !entry.metadata.head_only || *method == Method::HEAD
}

pub(super) fn cached_response(
    entry: DiskCacheEntry,
    cache_status: &'static str,
) -> Result<Response, ProxyError> {
    let DiskCacheEntry { body, metadata } = entry;
    let status = StatusCode::from_u16(metadata.status).map_err(|_| ProxyError::InvalidHeader)?;
    let mut builder = Response::builder()
        .status(status)
        .header("x-mirrorproxy-cache", cache_status)
        .header("age", unix_timestamp().saturating_sub(metadata.stored_at));
    for (name, value) in metadata.headers {
        if let (Ok(name), Ok(value)) = (HeaderName::try_from(name), HeaderValue::try_from(value)) {
            if should_forward_response_header(&name) && name != header::AGE {
                builder = builder.header(name, value);
            }
        }
    }
    builder
        .body(Body::from(body))
        .map_err(|_| ProxyError::InvalidHeader)
}

pub(super) fn write_disk_cache(
    cache: &CacheConfig,
    url: &Url,
    incoming_headers: &HeaderMap,
    status: reqwest::StatusCode,
    headers: &HeaderMap,
    body: &[u8],
    options: DiskCacheWriteOptions,
) {
    let Some((body_path, metadata_path)) = cache_paths(cache, url) else {
        return;
    };
    let Some(parent) = body_path.parent() else {
        return;
    };
    if fs::create_dir_all(parent).is_err() {
        return;
    }
    let lifetime = match options.ttl_secs {
        Some(ttl_secs) => explicit_cache_lifetime(cache, ttl_secs),
        None => cache_lifetime(cache, headers),
    };
    let Some((stored_at, expires_at)) = lifetime else {
        return;
    };
    if headers.contains_key(header::SET_COOKIE) {
        return;
    }
    // A body-less HEAD answer must never replace a cached response body.
    if options.head_only
        && fs::read(&metadata_path)
            .ok()
            .and_then(|value| serde_json::from_slice::<DiskCacheMetadata>(&value).ok())
            .is_some_and(|existing| !existing.head_only)
    {
        return;
    }
    let Some(vary) = cache_vary(headers, incoming_headers) else {
        return;
    };
    let metadata = DiskCacheMetadata {
        status: status.as_u16(),
        headers: headers
            .iter()
            .filter(|(name, _)| should_forward_response_header(name))
            // A negative answer is replayed without the upstream body, so
            // body-specific headers must not describe the cached empty body.
            .filter(|(name, _)| {
                status == reqwest::StatusCode::OK
                    || !matches!(name.as_str(), "content-length" | "content-encoding")
            })
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect(),
        stored_at,
        expires_at,
        vary,
        head_only: options.head_only,
    };
    let previous_bytes = fs::metadata(&body_path).map_or(0, |metadata| metadata.len());
    let body_tmp = body_path.with_extension("body.tmp");
    let metadata_tmp = metadata_path.with_extension("json.tmp");
    if fs::write(&body_tmp, body).is_ok()
        && serde_json::to_vec(&metadata)
            .ok()
            .is_some_and(|value| fs::write(&metadata_tmp, value).is_ok())
    {
        if fs::rename(&body_tmp, &body_path).is_ok()
            && fs::rename(&metadata_tmp, &metadata_path).is_ok()
        {
            maintain_capacity(cache, previous_bytes, body.len() as u64);
        }
        let _ = fs::remove_file(body_tmp);
        let _ = fs::remove_file(metadata_tmp);
    }
}

pub(super) fn refresh_cache_metadata(
    cache: &CacheConfig,
    url: &Url,
    metadata: &mut DiskCacheMetadata,
    response_headers: &HeaderMap,
) {
    let Some((stored_at, expires_at)) = cache_lifetime(cache, response_headers) else {
        return;
    };
    metadata.stored_at = stored_at;
    metadata.expires_at = expires_at;
    for name in ["etag", "last-modified", "cache-control", "expires"] {
        if let Some(value) = response_headers
            .get(name)
            .and_then(|value| value.to_str().ok())
        {
            metadata
                .headers
                .retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
            metadata.headers.push((name.to_string(), value.to_string()));
        }
    }
    if let Some((_, metadata_path)) = cache_paths(cache, url) {
        if let Ok(value) = serde_json::to_vec(metadata) {
            let temporary = metadata_path.with_extension("json.tmp");
            if fs::write(&temporary, value).is_ok() {
                let _ = fs::rename(temporary, metadata_path);
            }
        }
    }
}

pub(super) fn cache_lifetime(cache: &CacheConfig, headers: &HeaderMap) -> Option<(u64, u64)> {
    let directives = headers
        .get("cache-control")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
    if directives.iter().any(|directive| {
        matches!(
            directive.to_ascii_lowercase().as_str(),
            "no-store" | "private" | "no-cache"
        )
    }) {
        return None;
    }
    let upstream_ttl = directives.iter().find_map(|directive| {
        let (name, value) = directive.split_once('=')?;
        matches!(
            name.trim().to_ascii_lowercase().as_str(),
            "s-maxage" | "max-age"
        )
        .then(|| value.trim_matches('"').parse::<u64>().ok())
        .flatten()
    });
    let ttl = upstream_ttl
        .unwrap_or(cache.default_ttl_secs)
        .min(cache.max_ttl_secs);
    (ttl > 0).then(|| {
        let now = unix_timestamp();
        (now, now.saturating_add(ttl))
    })
}

/// Operator configured freshness for an answer the upstream did not publish a
/// usable lifetime for, such as a negative result, a HEAD probe, or a document
/// that is rewritten per request.
pub(super) fn explicit_cache_lifetime(cache: &CacheConfig, ttl_secs: u64) -> Option<(u64, u64)> {
    let ttl = ttl_secs.min(cache.max_ttl_secs);
    (ttl > 0).then(|| {
        let now = unix_timestamp();
        (now, now.saturating_add(ttl))
    })
}

pub(super) fn cache_vary(
    headers: &HeaderMap,
    incoming: &HeaderMap,
) -> Option<Vec<(String, String)>> {
    let mut vary = Vec::new();
    for name in headers
        .get_all("vary")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if name == "*" {
            return None;
        }
        vary.push((
            name.to_ascii_lowercase(),
            incoming
                .get(name)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string(),
        ));
    }
    Some(vary)
}

pub(super) fn cached_header<'a>(metadata: &'a DiskCacheMetadata, name: &str) -> Option<&'a str> {
    metadata
        .headers
        .iter()
        .find(|(existing, _)| existing.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

pub(super) fn evict_disk_cache(cache: &CacheConfig) -> u64 {
    let Ok(entries) = fs::read_dir(&cache.directory) else {
        return 0;
    };
    let mut bodies: Vec<_> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            (path.extension().and_then(|v| v.to_str()) == Some("body")).then_some(path)
        })
        .filter_map(|path| {
            let metadata = fs::metadata(&path).ok()?;
            Some((
                metadata
                    .modified()
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                metadata.len(),
                path,
            ))
        })
        .collect();
    let mut total: u64 = bodies.iter().map(|(_, len, _)| *len).sum();
    if total <= max_cache_total_bytes(cache) {
        return total;
    }
    bodies.sort_by_key(|(modified, _, _)| *modified);
    for (_, len, path) in bodies {
        if total <= max_cache_total_bytes(cache) {
            break;
        }
        if fs::remove_file(&path).is_ok() {
            let _ = fs::remove_file(path.with_extension("json"));
            total = total.saturating_sub(len);
        }
    }
    total
}

#[derive(Debug, Serialize)]
pub struct CacheStats {
    pub enabled: bool,
    pub directory: String,
    pub entries: u64,
    pub bytes: u64,
    pub max_bytes: u64,
}

pub(super) fn disk_cache_stats(cache: &CacheConfig) -> CacheStats {
    let (entries, bytes) = fs::read_dir(&cache.directory)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            (path.extension().and_then(|value| value.to_str()) == Some("body"))
                .then(|| fs::metadata(path).ok().map(|metadata| metadata.len()))
                .flatten()
        })
        .fold((0_u64, 0_u64), |(entries, bytes), length| {
            (entries + 1, bytes.saturating_add(length))
        });
    CacheStats {
        enabled: cache.enabled,
        directory: cache.directory.clone(),
        entries,
        bytes,
        max_bytes: max_cache_total_bytes(cache),
    }
}

pub(super) fn purge_disk_cache(cache: &CacheConfig) -> std::io::Result<u64> {
    // Invalidate the incremental count even if a partial purge fails.
    *CAPACITY.lock().expect("cache capacity lock poisoned") = None;
    let entries = match fs::read_dir(&cache.directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut removed = 0_u64;
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("body" | "json" | "tmp")
        ) && fs::remove_file(path).is_ok()
        {
            removed += 1;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concurrent_writes_keep_payload_metadata_and_capacity_consistent() {
        let directory = std::env::temp_dir().join(format!(
            "mirrorproxy-cache-workers-{}",
            uuid::Uuid::new_v4()
        ));
        let cache = CacheConfig {
            enabled: true,
            directory: directory.display().to_string(),
            max_entry_mb: 1,
            max_total_mb: 1,
            ..CacheConfig::default()
        };
        let mut writes = tokio::task::JoinSet::new();
        for index in 0..12 {
            let cache = cache.clone();
            writes.spawn(async move {
                cache_work(true, move || {
                    let url =
                        Url::parse(&format!("https://upstream.example/{}", index % 3)).unwrap();
                    let mut headers = HeaderMap::new();
                    headers.insert("etag", HeaderValue::from_str(&index.to_string()).unwrap());
                    write_disk_cache(
                        &cache,
                        &url,
                        &HeaderMap::new(),
                        StatusCode::OK,
                        &headers,
                        &vec![index; 400 * 1024],
                        DiskCacheWriteOptions::default(),
                    );
                })
                .await
                .unwrap();
            });
        }
        while let Some(result) = writes.join_next().await {
            result.unwrap();
        }
        let checked = cache.clone();
        cache_work(false, move || {
            assert!(disk_cache_stats(&checked).bytes <= max_cache_total_bytes(&checked));
            let mut hits = 0;
            for index in 0..3 {
                let url = Url::parse(&format!("https://upstream.example/{index}")).unwrap();
                if let Some(entry) = load_disk_cache(&checked, &url, &HeaderMap::new()) {
                    hits += 1;
                    let expected: u8 = cached_header(&entry.metadata, "etag")
                        .unwrap()
                        .parse()
                        .unwrap();
                    assert!(entry.body.iter().all(|byte| *byte == expected));
                }
            }
            assert!(hits > 0);
        })
        .await
        .unwrap();
        cache_work(true, move || {
            purge_disk_cache(&cache).unwrap();
            fs::remove_dir_all(directory).unwrap();
        })
        .await
        .unwrap();
    }
}
