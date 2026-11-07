mod cache;
use cache::*;
pub mod anaconda;
pub mod clojars;
pub mod cocoapods;
pub mod composer;
pub mod cpan;
pub mod cran;
pub mod cratesio;
pub mod elpa;
pub mod flatpak;
pub mod github;
pub mod go;
pub mod guix;
pub mod hackage;
pub mod homebrew;
pub mod huggingface;
pub mod julia;
pub mod luarocks;
pub mod maven;
pub mod nix;
pub mod npm;
pub mod nuget;
pub mod nvm;
pub mod oci;
pub mod opam;
pub mod os;
pub mod pub_repository;
pub mod pypi;
pub mod rubygems;
pub mod rustup;
pub mod texlive;
pub mod winget;

use std::{
    collections::HashMap,
    fs,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, Bytes},
    http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    response::Response,
};
use futures_util::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use opentelemetry::global;
use opentelemetry_http::HeaderInjector;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{config::CacheConfig, AppState};

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("proxy is disabled: {0}")]
    Disabled(&'static str),
    #[error("cache worker failed")]
    CacheWorker,
    #[error("invalid upstream url")]
    InvalidUrl,
    #[error("unsupported proxy target")]
    UnsupportedTarget,
    #[error("method is not allowed")]
    MethodNotAllowed,
    #[error("upstream request failed: {0}")]
    Upstream(#[from] reqwest::Error),
    #[error("upstream returned invalid header")]
    InvalidHeader,
    #[error("upstream redirect has an unsafe destination")]
    UnsafeRedirect,
}

/// Returns the first endpoint from an ordered comma-separated upstream group.
/// The forwarding layer expands the remaining endpoints and retries failures
/// which are safe for another mirror to satisfy.
pub fn select_upstream(configured: &str) -> Result<&str, ProxyError> {
    configured
        .split(',')
        .map(str::trim)
        .find(|endpoint| !endpoint.is_empty())
        .ok_or(ProxyError::InvalidUrl)
}

impl ProxyError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Disabled(_) => StatusCode::NOT_FOUND,
            Self::InvalidUrl | Self::UnsupportedTarget | Self::InvalidHeader => {
                StatusCode::BAD_REQUEST
            }
            Self::UnsafeRedirect => StatusCode::BAD_GATEWAY,
            Self::CacheWorker => StatusCode::INTERNAL_SERVER_ERROR,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
        }
    }
}

pub async fn forward(
    state: &AppState,
    method: Method,
    url: Url,
    incoming_headers: &HeaderMap,
) -> Result<Response, ProxyError> {
    if !matches!(method, Method::GET | Method::HEAD) {
        return Err(ProxyError::MethodNotAllowed);
    }

    forward_request(state, method, url, incoming_headers, None).await
}

pub async fn forward_with_body(
    state: &AppState,
    method: Method,
    url: Url,
    incoming_headers: &HeaderMap,
    body: Body,
) -> Result<Response, ProxyError> {
    if method != Method::POST {
        return Err(ProxyError::MethodNotAllowed);
    }

    let body = reqwest::Body::wrap_stream(body.into_data_stream());
    forward_request(state, method, url, incoming_headers, Some(body)).await
}

async fn forward_request(
    state: &AppState,
    method: Method,
    url: Url,
    incoming_headers: &HeaderMap,
    mut body: Option<reqwest::Body>,
) -> Result<Response, ProxyError> {
    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())
        .map_err(|_| ProxyError::MethodNotAllowed)?;
    let config = state.config();
    let client = state.client();
    // Keyed on the requested artifact rather than on the endpoint that serves
    // it, so a concurrent duplicate joins the leader's fetch.
    let in_flight_key = format!("{method} {url}");
    let candidates = if body.is_some() {
        vec![url]
    } else {
        state.upstream_selector.rank(
            config.upstream_candidates_for(&url),
            &config.upstream_selection,
        )
    };
    let cacheable = cacheable_request(method.clone(), incoming_headers) && config.cache.enabled;
    let cache_config = &config.cache;
    if cacheable {
        // A cached answer lives under the endpoint that served it, which is often
        // not the first configured one. Resolve it across the whole group first,
        // so a warm artifact never pays for the preceding misses again.
        let hit = cached_hit(cache_config, &candidates, incoming_headers, &method);
        if let Some(entry) = hit.await {
            return cached_response(entry, "HIT");
        }
    }
    // Coalesce concurrent fetches of the same artifact: the leader walks the
    // upstream group and fills the cache, while followers wait for it and then
    // read the answer from disk instead of repeating the same upstream work.
    let mut leader = None;
    if cacheable {
        match begin_in_flight(in_flight_key) {
            InFlight::Leader(guard) => leader = Some(guard),
            InFlight::Follower(done) => {
                let limit = Duration::from_secs(config.timeout.request_secs.max(1));
                wait_for_in_flight(done, limit).await;
                let hit = cached_hit(cache_config, &candidates, incoming_headers, &method);
                if let Some(entry) = hit.await {
                    return cached_response(entry, "HIT");
                }
            }
        }
    }
    let _leader = leader;
    let mut not_found_everywhere = true;
    for (index, candidate) in candidates.iter().enumerate() {
        let cached = if cacheable {
            let cache = config.cache.clone();
            let candidate = candidate.clone();
            let headers = incoming_headers.clone();
            cache_work(false, move || load_disk_cache(&cache, &candidate, &headers))
                .await
                .unwrap_or(None)
                .filter(|entry| cache_entry_serves(entry, &method))
        } else {
            None
        };
        if cached
            .as_ref()
            .is_some_and(|entry| entry.metadata.expires_at > unix_timestamp())
        {
            return cached_response(cached.expect("fresh cache entry exists"), "HIT");
        }
        let mut request = upstream_request(
            &client,
            reqwest_method.clone(),
            candidate.clone(),
            incoming_headers,
            &config,
        );
        if let Some(entry) = &cached {
            if let Some(value) = cached_header(&entry.metadata, "etag") {
                request = request.header("if-none-match", value);
            }
            if let Some(value) = cached_header(&entry.metadata, "last-modified") {
                request = request.header("if-modified-since", value);
            }
        }
        if let Some(body) = body.take() {
            request = request.body(body);
        }
        let started_at = Instant::now();
        let upstream = match send_upstream_request(&client, &config, request).await {
            Ok(response) => response,
            Err(error) if index + 1 < candidates.len() => {
                not_found_everywhere = false;
                state
                    .upstream_selector
                    .record_failure(candidate, &config.upstream_selection);
                tracing::warn!(upstream = %candidate, %error, "upstream request failed; racing the remaining configured endpoints");
                return forward_remaining_upstreams(
                    state,
                    &config,
                    &method,
                    &candidates,
                    &candidates[index + 1..],
                    incoming_headers,
                    cacheable,
                    not_found_everywhere,
                )
                .await;
            }
            Err(error) => {
                not_found_everywhere = false;
                state
                    .upstream_selector
                    .record_failure(candidate, &config.upstream_selection);
                return Err(error);
            }
        };
        let status = upstream.status();
        if status == StatusCode::NOT_MODIFIED {
            if let Some(mut entry) = cached {
                let cache = config.cache.clone();
                let cache_candidate = candidate.clone();
                let headers = upstream.headers().clone();
                entry = cache_work(true, move || {
                    refresh_cache_metadata(&cache, &cache_candidate, &mut entry.metadata, &headers);
                    entry
                })
                .await
                .map_err(|_| ProxyError::CacheWorker)?;
                state
                    .upstream_selector
                    .record_success(candidate, started_at.elapsed());
                return cached_response(entry, "REVALIDATED");
            }
        }
        if should_failover_status(status) && index + 1 < candidates.len() {
            state
                .upstream_selector
                .record_failure(candidate, &config.upstream_selection);
            if status != StatusCode::NOT_FOUND {
                not_found_everywhere = false;
            }
            tracing::info!(upstream = %candidate, status = %status, "upstream did not return 200; racing the remaining configured endpoints");
            return forward_remaining_upstreams(
                state,
                &config,
                &method,
                &candidates,
                &candidates[index + 1..],
                incoming_headers,
                cacheable,
                not_found_everywhere,
            )
            .await;
        }
        if should_failover_status(status) {
            state
                .upstream_selector
                .record_failure(candidate, &config.upstream_selection);
        } else {
            state
                .upstream_selector
                .record_success(candidate, started_at.elapsed());
        }
        return finish_upstream_response(
            &config,
            &method,
            incoming_headers,
            cacheable,
            &candidates,
            candidate,
            cached,
            upstream,
            not_found_everywhere,
        )
        .await;
    }
    unreachable!("every upstream request has at least one candidate")
}

/// Concurrent fetches of one artifact share a single upstream walk.
type InFlightMap = HashMap<String, tokio::sync::watch::Receiver<bool>>;

static IN_FLIGHT: OnceLock<Mutex<InFlightMap>> = OnceLock::new();

fn in_flight_requests() -> &'static Mutex<InFlightMap> {
    IN_FLIGHT.get_or_init(|| Mutex::new(HashMap::new()))
}

struct InFlightGuard {
    key: String,
    done: tokio::sync::watch::Sender<bool>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let _ = self.done.send(true);
        if let Some(in_flight) = IN_FLIGHT.get() {
            if let Ok(mut pending) = in_flight.lock() {
                pending.remove(&self.key);
            }
        }
    }
}

enum InFlight {
    Leader(InFlightGuard),
    Follower(tokio::sync::watch::Receiver<bool>),
}

fn begin_in_flight(key: String) -> InFlight {
    let mut in_flight = in_flight_requests()
        .lock()
        .expect("in-flight lock poisoned");
    if let Some(done) = in_flight.get(&key) {
        return InFlight::Follower(done.clone());
    }
    let (done, waiting) = tokio::sync::watch::channel(false);
    in_flight.insert(key.clone(), waiting);
    InFlight::Leader(InFlightGuard { key, done })
}

async fn wait_for_in_flight(mut done: tokio::sync::watch::Receiver<bool>, limit: Duration) {
    let waited = tokio::time::timeout(limit, async {
        loop {
            let finished = *done.borrow();
            if finished || done.changed().await.is_err() {
                break;
            }
        }
    })
    .await;
    if waited.is_err() {
        tracing::warn!("timed out waiting for a concurrent fetch of the same artifact");
    }
}

/// Resolves a fresh cache entry under any endpoint of the group, so an artifact
/// served by a later endpoint never pays for the preceding misses again.
async fn cached_hit(
    cache: &CacheConfig,
    candidates: &[Url],
    incoming_headers: &HeaderMap,
    method: &Method,
) -> Option<DiskCacheEntry> {
    let cache = cache.clone();
    let candidates = candidates.to_vec();
    let headers = incoming_headers.clone();
    let request_method = method.clone();
    cache_work(false, move || {
        candidates.iter().find_map(|candidate| {
            let metadata = load_disk_cache_metadata(&cache, candidate, &headers)?;
            if metadata.expires_at <= unix_timestamp()
                || (metadata.head_only && request_method != Method::HEAD)
            {
                return None;
            }
            load_disk_cache(&cache, candidate, &headers)
        })
    })
    .await
    .unwrap_or(None)
}

struct RaceOutcome {
    winner: Option<(Url, reqwest::Response)>,
    /// A non-404 failure such as an upstream timeout or rate limit.
    failure: Option<(Url, reqwest::Response)>,
    /// The last resort: every endpoint that answered said "not found".
    not_found_fallback: Option<(Url, reqwest::Response)>,
    last_error: Option<ProxyError>,
    not_found_everywhere: bool,
}

/// Probes every remaining endpoint at once. A slow or hanging endpoint can then
/// only delay its own answer instead of serializing the requests behind it.
async fn race_upstreams(
    state: &AppState,
    config: &crate::config::Config,
    method: &Method,
    candidates: &[Url],
    incoming_headers: &HeaderMap,
) -> RaceOutcome {
    let client = state.client();
    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())
        .expect("request methods are validated before forwarding");
    let mut pending = FuturesUnordered::new();
    for candidate in candidates {
        let client = client.clone();
        let method = reqwest_method.clone();
        let candidate = candidate.clone();
        let headers = incoming_headers.clone();
        pending.push(async move {
            let started_at = Instant::now();
            let request = upstream_request(&client, method, candidate.clone(), &headers, config);
            let result = send_upstream_request(&client, config, request).await;
            (candidate, result, started_at.elapsed())
        });
    }
    let mut outcome = RaceOutcome {
        winner: None,
        failure: None,
        not_found_fallback: None,
        last_error: None,
        not_found_everywhere: true,
    };
    while let Some((candidate, result, elapsed)) = pending.next().await {
        match result {
            Ok(response) if !should_failover_status(response.status()) => {
                state.upstream_selector.record_success(&candidate, elapsed);
                outcome.winner = Some((candidate, response));
                return outcome;
            }
            Ok(response) => {
                if response.status() == StatusCode::NOT_FOUND {
                    outcome
                        .not_found_fallback
                        .get_or_insert((candidate.clone(), response));
                } else {
                    outcome.not_found_everywhere = false;
                    outcome.failure.get_or_insert((candidate.clone(), response));
                }
                state
                    .upstream_selector
                    .record_failure(&candidate, &config.upstream_selection);
            }
            Err(error) => {
                outcome.not_found_everywhere = false;
                state
                    .upstream_selector
                    .record_failure(&candidate, &config.upstream_selection);
                tracing::warn!(upstream = %candidate, %error, "upstream request failed; continuing with the other configured endpoints");
                outcome.last_error = Some(error);
            }
        }
    }
    outcome
}

/// Finishes a request from the endpoint that answered: revalidates a cached
/// copy, refreshes the cache, or streams the upstream body back.
#[allow(clippy::too_many_arguments)]
async fn finish_upstream_response(
    config: &crate::config::Config,
    method: &Method,
    incoming_headers: &HeaderMap,
    cacheable: bool,
    candidates: &[Url],
    candidate: &Url,
    cached: Option<DiskCacheEntry>,
    upstream: reqwest::Response,
    not_found_everywhere: bool,
) -> Result<Response, ProxyError> {
    let status = upstream.status();
    if status == StatusCode::NOT_MODIFIED {
        if let Some(mut entry) = cached {
            let cache = config.cache.clone();
            let cache_candidate = candidate.clone();
            let headers = upstream.headers().clone();
            entry = cache_work(true, move || {
                refresh_cache_metadata(&cache, &cache_candidate, &mut entry.metadata, &headers);
                entry
            })
            .await
            .map_err(|_| ProxyError::CacheWorker)?;
            return cached_response(entry, "REVALIDATED");
        }
    }
    let headers = upstream.headers().clone();
    let head_only = *method == Method::HEAD;
    // A HEAD answer is stored without a body, so its declared length must not
    // decide whether the answer is worth remembering.
    let entry_size_fits = head_only
        || headers
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length <= max_cache_entry_bytes(&config.cache));
    if cacheable && status == StatusCode::OK && entry_size_fits {
        let entry_ttl = head_only.then_some(config.cache.head_ttl_secs);
        let response_body = upstream.bytes().await?;
        let cache = config.cache.clone();
        let candidate = candidate.clone();
        let incoming = incoming_headers.clone();
        let stored_headers = headers.clone();
        let stored_body = response_body.clone();
        let _ = cache_work(true, move || {
            write_disk_cache(
                &cache,
                &candidate,
                &incoming,
                status,
                &stored_headers,
                &stored_body,
                DiskCacheWriteOptions {
                    head_only,
                    ttl_secs: entry_ttl,
                },
            )
        })
        .await;
        return response_with_headers(status, &headers, Body::from(response_body));
    }
    if cacheable
        && status == StatusCode::NOT_FOUND
        && not_found_everywhere
        && config.cache.negative_ttl_secs > 0
    {
        // Every configured endpoint agreed the artifact does not exist. Remember
        // that for a bounded window so repeated misses, such as Gradle probing a
        // plugin marker that has no jar, stop walking the whole group.
        let cache = config.cache.clone();
        let negative_candidates = candidates.to_vec();
        let incoming = incoming_headers.clone();
        let stored_headers = headers.clone();
        let ttl_secs = config.cache.negative_ttl_secs;
        let _ = cache_work(true, move || {
            for candidate in &negative_candidates {
                write_disk_cache(
                    &cache,
                    candidate,
                    &incoming,
                    status,
                    &stored_headers,
                    &[],
                    DiskCacheWriteOptions {
                        head_only: false,
                        ttl_secs: Some(ttl_secs),
                    },
                );
            }
        })
        .await;
    }
    let stream = upstream.bytes_stream().map_err(std::io::Error::other);
    response_with_headers(status, &headers, Body::from_stream(stream))
}

/// Completes a request after the first endpoint failed over, choosing the racer
/// that answered first and remembering a confirmed "not found" for the group.
#[allow(clippy::too_many_arguments)]
async fn forward_remaining_upstreams(
    state: &AppState,
    config: &crate::config::Config,
    method: &Method,
    candidates: &[Url],
    remaining: &[Url],
    incoming_headers: &HeaderMap,
    cacheable: bool,
    not_found_everywhere: bool,
) -> Result<Response, ProxyError> {
    let outcome = race_upstreams(state, config, method, remaining, incoming_headers).await;
    if let Some((winner, response)) = outcome.winner {
        return finish_upstream_response(
            config,
            method,
            incoming_headers,
            cacheable,
            candidates,
            &winner,
            None,
            response,
            false,
        )
        .await;
    }
    if let Some((candidate, response)) = outcome.failure.or(outcome.not_found_fallback) {
        return finish_upstream_response(
            config,
            method,
            incoming_headers,
            cacheable,
            candidates,
            &candidate,
            None,
            response,
            not_found_everywhere && outcome.not_found_everywhere,
        )
        .await;
    }
    match outcome.last_error {
        Some(error) => Err(error),
        None => unreachable!("every raced upstream attempt returns a response or an error"),
    }
}

pub async fn get_with_fallback(
    state: &AppState,
    url: Url,
) -> Result<reqwest::Response, ProxyError> {
    let config = state.config();
    let client = state.client();
    let candidates = state.upstream_selector.rank(
        config.upstream_candidates_for(&url),
        &config.upstream_selection,
    );
    for (index, candidate) in candidates.iter().enumerate() {
        let started_at = Instant::now();
        let response = match send_upstream_request(
            &client,
            &config,
            upstream_request(
                &client,
                reqwest::Method::GET,
                candidate.clone(),
                &HeaderMap::new(),
                &config,
            ),
        )
        .await
        {
            Ok(response) => response,
            Err(error) if index + 1 < candidates.len() => {
                state
                    .upstream_selector
                    .record_failure(candidate, &config.upstream_selection);
                tracing::warn!(upstream = %candidate, %error, next_upstream = %candidates[index + 1], "upstream request failed; trying the next configured endpoint");
                continue;
            }
            Err(error) => {
                state
                    .upstream_selector
                    .record_failure(candidate, &config.upstream_selection);
                return Err(error);
            }
        };
        if should_failover_status(response.status()) && index + 1 < candidates.len() {
            state
                .upstream_selector
                .record_failure(candidate, &config.upstream_selection);
            tracing::info!(upstream = %candidate, status = %response.status(), next_upstream = %candidates[index + 1], "upstream did not return 200; trying the next configured endpoint");
            continue;
        }
        if should_failover_status(response.status()) {
            state
                .upstream_selector
                .record_failure(candidate, &config.upstream_selection);
        } else {
            state
                .upstream_selector
                .record_success(candidate, started_at.elapsed());
        }
        return Ok(response);
    }
    unreachable!("every upstream request has at least one candidate")
}

/// A document buffered for per-request rewriting, such as pub package metadata
/// that embeds this instance's public URL.
pub struct CachedDocument {
    pub body: Bytes,
    pub content_type: String,
    /// `HIT` when the disk cache answered without contacting an upstream.
    pub cache_status: &'static str,
}

/// Fetches an upstream JSON document through the disk cache and buffers its body.
/// Adapters that must rewrite a document per request use this instead of reading
/// a forwarded stream, so the upstream representation is still cached.
pub async fn cached_document(
    state: &AppState,
    url: Url,
    incoming_headers: &HeaderMap,
) -> Result<Option<CachedDocument>, ProxyError> {
    let config = state.config();
    let cacheable = cacheable_request(Method::GET, incoming_headers) && config.cache.enabled;
    if cacheable {
        let cache = config.cache.clone();
        let cached_url = url.clone();
        let headers = incoming_headers.clone();
        let hit = cache_work(false, move || load_disk_cache(&cache, &cached_url, &headers))
            .await
            .unwrap_or(None)
            .filter(|entry| {
                entry.metadata.status == StatusCode::OK.as_u16()
                    && entry.metadata.expires_at > unix_timestamp()
            });
        if let Some(entry) = hit {
            return Ok(Some(CachedDocument {
                content_type: cached_header(&entry.metadata, "content-type")
                    .unwrap_or_default()
                    .to_string(),
                body: Bytes::from(entry.body),
                cache_status: "HIT",
            }));
        }
    }
    let response = get_with_fallback(state, url.clone()).await?;
    if response.status() != StatusCode::OK {
        return Ok(None);
    }
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    // The buffered body is decoded, so body-specific headers must not describe it.
    let mut stored_headers = response.headers().clone();
    stored_headers.remove(header::CONTENT_ENCODING);
    stored_headers.remove(header::CONTENT_LENGTH);
    let body = response.bytes().await?;
    if cacheable && body.len() as u64 <= max_cache_entry_bytes(&config.cache) {
        let cache = config.cache.clone();
        let incoming = incoming_headers.clone();
        let stored_body = body.clone();
        let ttl_secs = config.cache.document_ttl_secs;
        let _ = cache_work(true, move || {
            write_disk_cache(
                &cache,
                &url,
                &incoming,
                StatusCode::OK,
                &stored_headers,
                &stored_body,
                DiskCacheWriteOptions {
                    head_only: false,
                    ttl_secs: Some(ttl_secs),
                },
            )
        })
        .await;
    }
    Ok(Some(CachedDocument {
        body,
        content_type,
        cache_status: "MISS",
    }))
}

fn upstream_request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: Url,
    incoming_headers: &HeaderMap,
    config: &crate::config::Config,
) -> reqwest::RequestBuilder {
    let mut request = client.request(method, url.clone());
    for (name, value) in incoming_headers {
        if should_forward_request_header(name) {
            request = request.header(name.as_str(), value.as_bytes());
        }
    }
    let mut trace_headers = HeaderMap::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &tracing::Span::current().context(),
            &mut HeaderInjector(&mut trace_headers),
        );
    });
    request = request.headers(trace_headers);
    if let Some(auth) = config.upstream_auth_for(&url) {
        request = match (&auth.username, &auth.password, &auth.bearer_token) {
            (Some(username), Some(password), None) => request.basic_auth(username, Some(password)),
            (None, None, Some(token)) => request.bearer_auth(token),
            _ => unreachable!("validated upstream authentication configuration"),
        };
    } else if config.forward_client_authorization {
        if let Some(value) = incoming_headers.get("authorization") {
            request = request.header("authorization", value);
        }
    }

    request
}

/// Follow data-plane redirects only after validating and pinning each new target.
/// The initially configured upstream remains operator-controlled, including
/// explicitly configured private addresses.
pub(crate) async fn send_upstream_request(
    initial_client: &reqwest::Client,
    config: &crate::config::Config,
    request: reqwest::RequestBuilder,
) -> Result<reqwest::Response, ProxyError> {
    let mut client = initial_client.clone();
    let mut request = request.build()?;
    let original = request.url().clone();
    let mut redirects = 0;
    loop {
        let replay = request.try_clone();
        let previous_url = request.url().clone();
        let method = request.method().clone();
        let mut headers = request.headers().clone();
        let response = client.execute(request).await?;
        let status = response.status();
        if !matches!(
            status,
            reqwest::StatusCode::MOVED_PERMANENTLY
                | reqwest::StatusCode::FOUND
                | reqwest::StatusCode::SEE_OTHER
                | reqwest::StatusCode::TEMPORARY_REDIRECT
                | reqwest::StatusCode::PERMANENT_REDIRECT
        ) {
            return Ok(response);
        }
        let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
            return Ok(response);
        };
        let location = location.to_str().map_err(|_| ProxyError::UnsafeRedirect)?;
        let target = response
            .url()
            .join(location)
            .map_err(|_| ProxyError::UnsafeRedirect)?;
        if redirects == 10
            || !matches!(target.scheme(), "http" | "https")
            || (previous_url.scheme() == "https" && target.scheme() != "https")
            || target.host_str().is_none()
            || !target.username().is_empty()
            || target.password().is_some()
            || target.fragment().is_some()
        {
            return Err(ProxyError::UnsafeRedirect);
        }
        let same_origin = previous_url.scheme() == target.scheme()
            && previous_url.host_str() == target.host_str()
            && previous_url.port_or_known_default() == target.port_or_known_default();
        let original_origin = original.scheme() == target.scheme()
            && original.host_str() == target.host_str()
            && original.port_or_known_default() == target.port_or_known_default();
        let explicit_ip_origin = same_origin
            && original_origin
            && target
                .host_str()
                .and_then(|host| host.parse::<IpAddr>().ok())
                .is_some();
        let addresses = if explicit_ip_origin {
            vec![std::net::SocketAddr::new(
                target
                    .host_str()
                    .and_then(|host| host.parse().ok())
                    .ok_or(ProxyError::UnsafeRedirect)?,
                target
                    .port_or_known_default()
                    .ok_or(ProxyError::UnsafeRedirect)?,
            )]
        } else {
            oci::public_destination_addresses(&target)
                .await
                .map_err(|_| ProxyError::UnsafeRedirect)?
        };
        // A remote HTTP/SOCKS proxy may resolve the target itself, bypassing
        // resolve_to_addrs and the address check above.
        if config.outbound_proxy.enabled && !explicit_ip_origin {
            return Err(ProxyError::UnsafeRedirect);
        }
        let host = target.host_str().ok_or(ProxyError::UnsafeRedirect)?;
        client = crate::build_streaming_upstream_client_builder(config)
            .map_err(|_| ProxyError::UnsafeRedirect)?
            .resolve_to_addrs(host, &addresses)
            .build()?;
        if !same_origin {
            for name in [
                reqwest::header::AUTHORIZATION,
                reqwest::header::COOKIE,
                reqwest::header::PROXY_AUTHORIZATION,
            ] {
                headers.remove(name);
            }
        }
        let change_to_get = method != reqwest::Method::GET
            && method != reqwest::Method::HEAD
            && matches!(
                status,
                reqwest::StatusCode::MOVED_PERMANENTLY
                    | reqwest::StatusCode::FOUND
                    | reqwest::StatusCode::SEE_OTHER
            );
        request = if change_to_get {
            headers.remove(reqwest::header::CONTENT_LENGTH);
            headers.remove(reqwest::header::CONTENT_TYPE);
            headers.remove(reqwest::header::TRANSFER_ENCODING);
            client
                .request(reqwest::Method::GET, target)
                .headers(headers)
                .build()?
        } else {
            let Some(mut replay) = replay else {
                return Ok(response);
            };
            *replay.url_mut() = target;
            *replay.headers_mut() = headers;
            replay
        };
        redirects += 1;
    }
}

pub(crate) async fn probe_endpoint(
    state: &AppState,
    method: reqwest::Method,
    url: Url,
) -> Result<reqwest::Response, ProxyError> {
    let config = state.config();
    send_upstream_request(
        &state.client(),
        &config,
        upstream_request(&state.client(), method, url, &HeaderMap::new(), &config),
    )
    .await
}

fn cacheable_request(method: Method, headers: &HeaderMap) -> bool {
    matches!(method, Method::GET | Method::HEAD)
        && !headers.contains_key("authorization")
        && !headers.contains_key("cookie")
        && !headers.contains_key("range")
}

fn response_with_headers(
    status: reqwest::StatusCode,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, ProxyError> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        if should_forward_response_header(name) {
            builder = builder.header(name, value);
        }
    }
    builder.body(body).map_err(|_| ProxyError::InvalidHeader)
}

fn max_cache_entry_bytes(cache: &CacheConfig) -> u64 {
    cache.max_entry_mb.saturating_mul(1024 * 1024)
}
fn max_cache_total_bytes(cache: &CacheConfig) -> u64 {
    cache.max_total_mb.saturating_mul(1024 * 1024)
}

#[cfg(test)]
mod upstream_selection_tests {
    use super::*;

    #[test]
    fn selects_the_first_comma_separated_upstream() {
        let configured = "https://rr-one.invalid/root, https://rr-two.invalid/root";
        assert_eq!(
            select_upstream(configured).unwrap(),
            "https://rr-one.invalid/root"
        );
    }

    #[test]
    fn rejects_an_empty_upstream_group() {
        assert!(matches!(
            select_upstream(" , "),
            Err(ProxyError::InvalidUrl)
        ));
    }
}

pub fn sanitize_repository_path(path: &str) -> Result<&str, ProxyError> {
    let path = path.trim_start_matches('/');
    if path.is_empty()
        || path.contains('\\')
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == ".." || part.contains('\0'))
    {
        return Err(ProxyError::InvalidUrl);
    }
    Ok(path)
}

pub fn repository_url(base: &str, path: &str, query: Option<&str>) -> Result<Url, ProxyError> {
    let mut url = Url::parse(select_upstream(base)?).map_err(|_| ProxyError::InvalidUrl)?;
    let base_path = url.path().trim_end_matches('/');
    url.set_path(&format!("{base_path}/{path}"));
    url.set_query(query);
    Ok(url)
}

pub fn build_url(base: &str, path: &str, query: Option<&str>) -> Result<Url, ProxyError> {
    let mut url = Url::parse(select_upstream(base)?).map_err(|_| ProxyError::InvalidUrl)?;
    url.set_path(path);
    url.set_query(query);
    Ok(url)
}

pub fn proxied_absolute_url(public_base_url: &str, absolute: &str) -> String {
    format!("{}/{}", public_base_url.trim_end_matches('/'), absolute)
}

/// Client-facing freshness for rewritten metadata. It stays shorter than
/// `cache.document_ttl_secs`, so a client revalidates long before the origin
/// cache entry expires.
pub fn metadata_cache_value() -> HeaderValue {
    HeaderValue::from_static("public, max-age=300, stale-while-revalidate=3600")
}

pub fn metadata_vary_value() -> HeaderValue {
    HeaderValue::from_static("X-Forwarded-Host, X-Forwarded-Proto")
}

pub(super) fn should_forward_request_header(name: &HeaderName) -> bool {
    !matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "host"
            | "authorization"
            | "cookie"
            | "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "traceparent"
            | "tracestate"
            | "baggage"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn should_forward_response_header(name: &HeaderName) -> bool {
    !matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            // Upstream session cookies, including the bot-management cookies that
            // Maven Central attaches to a 404, are never relayed to clients or
            // replayed from the cache.
            | "set-cookie"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::header;
    use axum::{routing::get, Router};
    use std::collections::BTreeMap;

    #[test]
    fn injects_configured_upstream_credentials_without_forwarding_client_credentials() {
        let mut config = crate::config::Config::default();
        config.upstream_auth = BTreeMap::from([(
            "npm".to_string(),
            crate::config::UpstreamAuth {
                username: Some("mirror".to_string()),
                password: Some("secret".to_string()),
                bearer_token: None,
            },
        )]);
        let mut incoming = HeaderMap::new();
        incoming.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer client-secret"),
        );
        let request = upstream_request(
            &reqwest::Client::new(),
            reqwest::Method::GET,
            Url::parse("https://registry.npmjs.org/package").unwrap(),
            &incoming,
            &config,
        )
        .build()
        .unwrap();
        assert_eq!(
            request.headers()[header::AUTHORIZATION],
            "Basic bWlycm9yOnNlY3JldA=="
        );
    }

    #[tokio::test]
    async fn redirects_stay_on_an_explicitly_configured_private_origin() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                "/start",
                get(|| async { (StatusCode::FOUND, [(header::LOCATION, "/end")]) }),
            )
            .route("/end", get(|| async { "redirected" }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = crate::config::Config::default();
        let client = crate::build_upstream_client(&config).unwrap();
        let response = send_upstream_request(
            &client,
            &config,
            client.get(format!("http://{address}/start")),
        )
        .await
        .unwrap();
        assert_eq!(response.text().await.unwrap(), "redirected");
        server.abort();
    }

    #[tokio::test]
    async fn redirects_to_other_loopback_origins_and_private_dns_are_rejected() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                "/ip",
                get(|| async {
                    (
                        StatusCode::FOUND,
                        [(header::LOCATION, "http://127.0.0.1:12345/secret")],
                    )
                }),
            )
            .route(
                "/dns",
                get(|| async {
                    (
                        StatusCode::FOUND,
                        [(header::LOCATION, "http://localhost:12345/secret")],
                    )
                }),
            );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = crate::config::Config::default();
        let client = crate::build_upstream_client(&config).unwrap();
        for path in ["ip", "dns"] {
            let result = send_upstream_request(
                &client,
                &config,
                client.get(format!("http://{address}/{path}")),
            )
            .await;
            assert!(matches!(result, Err(ProxyError::UnsafeRedirect)), "{path}");
        }
        server.abort();
    }

    #[tokio::test]
    async fn disk_cache_round_trip_preserves_response_headers() {
        let directory =
            std::env::temp_dir().join(format!("mirrorproxy-cache-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        let cache = CacheConfig {
            enabled: true,
            directory: directory.display().to_string(),
            max_entry_mb: 1,
            max_total_mb: 2,
            ..CacheConfig::default()
        };
        let url = Url::parse("https://upstream.example/package").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        write_disk_cache(
            &cache,
            &url,
            &HeaderMap::new(),
            reqwest::StatusCode::OK,
            &headers,
            b"{} ",
            DiskCacheWriteOptions::default(),
        );

        let response = cached_response(
            load_disk_cache(&cache, &url, &HeaderMap::new()).expect("cache hit"),
            "HIT",
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap()[..],
            b"{} "
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn cacheable_requests_exclude_private_and_partial_responses() {
        let mut headers = HeaderMap::new();
        assert!(cacheable_request(Method::GET, &headers));
        assert!(cacheable_request(Method::HEAD, &headers));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer token"),
        );
        assert!(!cacheable_request(Method::GET, &headers));
        assert!(!cacheable_request(Method::HEAD, &headers));
        headers.clear();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=0-99"));
        assert!(!cacheable_request(Method::GET, &headers));
        assert!(!cacheable_request(Method::HEAD, &headers));
        headers.clear();
        headers.insert(header::COOKIE, HeaderValue::from_static("session=1"));
        assert!(!cacheable_request(Method::GET, &headers));
        assert!(!cacheable_request(Method::HEAD, &headers));
        assert!(!cacheable_request(Method::POST, &HeaderMap::new()));
    }

    #[test]
    fn explicit_cache_lifetimes_respect_the_configured_ceiling() {
        let cache = CacheConfig {
            default_ttl_secs: 60,
            max_ttl_secs: 900,
            ..CacheConfig::default()
        };
        let (stored_at, expires_at) = explicit_cache_lifetime(&cache, 3600).unwrap();
        assert_eq!(expires_at - stored_at, 900);
        assert!(explicit_cache_lifetime(&cache, 0).is_none());
    }

    #[test]
    fn head_only_cache_writes_never_replace_a_cached_body() {
        let directory = std::env::temp_dir().join(format!(
            "mirrorproxy-cache-head-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        let cache = CacheConfig {
            enabled: true,
            directory: directory.display().to_string(),
            max_entry_mb: 1,
            max_total_mb: 2,
            ..CacheConfig::default()
        };
        let url = Url::parse("https://upstream.example/artifact.jar").unwrap();
        write_disk_cache(
            &cache,
            &url,
            &HeaderMap::new(),
            reqwest::StatusCode::OK,
            &HeaderMap::new(),
            b"artifact payload",
            DiskCacheWriteOptions::default(),
        );
        write_disk_cache(
            &cache,
            &url,
            &HeaderMap::new(),
            reqwest::StatusCode::OK,
            &HeaderMap::new(),
            b"",
            DiskCacheWriteOptions {
                head_only: true,
                ttl_secs: Some(3600),
            },
        );
        let entry = load_disk_cache(&cache, &url, &HeaderMap::new()).expect("cache hit");
        assert!(!entry.metadata.head_only);
        assert_eq!(entry.body, b"artifact payload");
        assert!(cache_entry_serves(&entry, &Method::GET));
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn cache_policy_rejects_private_responses_and_caps_upstream_ttl() {
        let cache = CacheConfig {
            default_ttl_secs: 60,
            max_ttl_secs: 300,
            ..CacheConfig::default()
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=3600"),
        );
        let (stored_at, expires_at) = cache_lifetime(&cache, &headers).unwrap();
        assert_eq!(expires_at - stored_at, 300);

        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("private, max-age=300"),
        );
        assert!(cache_lifetime(&cache, &headers).is_none());
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        assert!(cache_lifetime(&cache, &headers).is_none());
    }

    #[test]
    fn cache_vary_requires_the_original_request_header_value() {
        let mut response_headers = HeaderMap::new();
        response_headers.insert(header::VARY, HeaderValue::from_static("Accept, X-Flavor"));
        let mut incoming = HeaderMap::new();
        incoming.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        incoming.insert("x-flavor", HeaderValue::from_static("stable"));
        assert_eq!(
            cache_vary(&response_headers, &incoming).unwrap(),
            vec![
                ("accept".to_string(), "application/json".to_string()),
                ("x-flavor".to_string(), "stable".to_string())
            ]
        );

        response_headers.insert(header::VARY, HeaderValue::from_static("*"));
        assert!(cache_vary(&response_headers, &incoming).is_none());
    }

    #[test]
    fn failover_preserves_protocol_success_and_authentication_statuses() {
        assert!(!should_failover_status(StatusCode::PARTIAL_CONTENT));
        assert!(!should_failover_status(StatusCode::NOT_MODIFIED));
        assert!(!should_failover_status(StatusCode::UNAUTHORIZED));
        assert!(should_failover_status(StatusCode::NOT_FOUND));
        assert!(should_failover_status(StatusCode::BAD_GATEWAY));
        assert!(should_failover_status(StatusCode::TOO_MANY_REQUESTS));
    }

    #[test]
    fn never_forwards_client_credentials_or_cookies() {
        assert!(!should_forward_request_header(&header::AUTHORIZATION));
        assert!(!should_forward_request_header(&header::COOKIE));
        assert!(!should_forward_request_header(&header::PROXY_AUTHORIZATION));
        assert!(!should_forward_request_header(&HeaderName::from_static(
            "traceparent"
        )));
        assert!(!should_forward_request_header(&HeaderName::from_static(
            "tracestate"
        )));
        assert!(!should_forward_request_header(&HeaderName::from_static(
            "baggage"
        )));
        assert!(should_forward_request_header(&header::ACCEPT));
    }

    #[test]
    fn never_forwards_or_stores_upstream_session_cookies() {
        assert!(!should_forward_response_header(&header::SET_COOKIE));
        assert!(should_forward_response_header(&header::CONTENT_TYPE));
    }

    #[test]
    fn client_authorization_requires_explicit_opt_in() {
        let config = crate::config::Config::default();
        assert!(!config.forward_client_authorization);
    }

    #[test]
    fn capacity_eviction_removes_cached_body_and_metadata() {
        let directory =
            std::env::temp_dir().join(format!("mirrorproxy-evict-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        let cache = CacheConfig {
            enabled: true,
            directory: directory.display().to_string(),
            max_entry_mb: 1,
            max_total_mb: 0,
            ..CacheConfig::default()
        };
        let url = Url::parse("https://upstream.example/evict").unwrap();
        write_disk_cache(
            &cache,
            &url,
            &HeaderMap::new(),
            reqwest::StatusCode::OK,
            &HeaderMap::new(),
            b"entry",
            DiskCacheWriteOptions::default(),
        );
        let (body, metadata) = cache_paths(&cache, &url).unwrap();
        assert!(!body.exists());
        assert!(!metadata.exists());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn capacity_eviction_keeps_most_recently_read_entry() {
        let directory =
            std::env::temp_dir().join(format!("mirrorproxy-lru-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        let mut cache = CacheConfig {
            enabled: true,
            directory: directory.display().to_string(),
            max_entry_mb: 1,
            max_total_mb: 2,
            ..CacheConfig::default()
        };
        let headers = HeaderMap::new();
        let first = Url::parse("https://upstream.example/first").unwrap();
        let second = Url::parse("https://upstream.example/second").unwrap();
        let third = Url::parse("https://upstream.example/third").unwrap();
        let payload = vec![0; 600 * 1024];
        write_disk_cache(
            &cache,
            &first,
            &HeaderMap::new(),
            reqwest::StatusCode::OK,
            &headers,
            &payload,
            DiskCacheWriteOptions::default(),
        );
        let (first_body, _) = cache_paths(&cache, &first).unwrap();
        write_disk_cache(
            &cache,
            &second,
            &HeaderMap::new(),
            reqwest::StatusCode::OK,
            &headers,
            &payload,
            DiskCacheWriteOptions::default(),
        );
        fs::OpenOptions::new()
            .write(true)
            .open(&first_body)
            .unwrap()
            .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(1))
            .unwrap();
        cache.max_total_mb = 1;
        write_disk_cache(
            &cache,
            &third,
            &HeaderMap::new(),
            reqwest::StatusCode::OK,
            &headers,
            &payload,
            DiskCacheWriteOptions::default(),
        );
        let (second_body, _) = cache_paths(&cache, &second).unwrap();
        assert!(first_body.exists());
        assert!(!second_body.exists());
        let _ = fs::remove_dir_all(directory);
    }
}

fn should_failover_status(status: StatusCode) -> bool {
    status == StatusCode::NOT_FOUND
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub async fn cache_stats(cache: CacheConfig) -> Result<cache::CacheStats, tokio::task::JoinError> {
    cache_work(false, move || disk_cache_stats(&cache)).await
}

pub async fn purge_cache(cache: CacheConfig) -> anyhow::Result<u64> {
    Ok(cache_work(true, move || purge_disk_cache(&cache)).await??)
}
