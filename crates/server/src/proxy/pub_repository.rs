use super::ProxyError;
use crate::{proxy, AppState};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use serde_json::Value;

/// Origins a rewritten metadata document may point back at. Archives are
/// downloaded from their original host through `/pub/upstream/<host>/<path>`,
/// so this list also bounds what that route can reach. Hosts outside the list
/// keep their absolute URL instead of turning the mirror into an open proxy.
const ARCHIVE_HOSTS: &[&str] = &[
    "pub.dev",
    "pub.flutter-io.cn",
    "storage.flutter-io.cn",
    "storage.googleapis.com",
];

/// A layout some upstream metadata still advertises although the origin stopped
/// serving it, paired with the endpoint that still serves the same file.
/// `pub.flutter-io.cn` keeps pointing `versions[].archive_url` at the retired
/// `dartlang-pub-public-packages` bucket, which answers 404 for every package,
/// while the archive is available from the mirror's `api/archives/` entry point.
struct RetiredArchiveLayout {
    host: &'static str,
    prefix: &'static str,
    live_host: &'static str,
    live_prefix: &'static str,
}

const RETIRED_ARCHIVE_LAYOUTS: &[RetiredArchiveLayout] = &[RetiredArchiveLayout {
    host: "storage.flutter-io.cn",
    prefix: "dartlang-pub-public-packages/packages/",
    live_host: "pub.flutter-io.cn",
    live_prefix: "api/archives/",
}];

/// pub serves package metadata as JSON from `/api/...`; `/api/archives/` is the
/// only binary endpoint and must stay on the streaming, cacheable path instead
/// of being buffered for URL rewriting.
fn is_archive_path(path: &str) -> bool {
    path.starts_with("api/archives/")
}

pub async fn root(State(state): State<AppState>) -> Result<Response, ProxyError> {
    if !state.config().is_enabled("pub") {
        return Err(ProxyError::Disabled("pub"));
    }
    Response::builder()
        .status(200)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )
        .body(Body::from("MirrorProxy Pub repository proxy\n"))
        .map_err(|_| ProxyError::InvalidHeader)
}
pub async fn proxy(
    State(state): State<AppState>,
    Path(path): Path<String>,
    request: axum::extract::Request,
) -> Result<Response, ProxyError> {
    let config = state.config();
    if !config.is_enabled("pub") {
        return Err(ProxyError::Disabled("pub"));
    }
    let path = sanitize(&path)?;
    let query = request.uri().query();
    if let Some(url) = archive_upstream_url(&path, query)? {
        return proxy::forward(&state, request.method().clone(), url, request.headers()).await;
    }
    let url = repository_url(&config.upstreams.pub_repository, &path, query)?;
    if is_archive_path(&path) {
        return proxy::forward(&state, request.method().clone(), url, request.headers()).await;
    }
    // The upstream document is cached as published and rewritten per request,
    // because the embedded archive URLs depend on this instance's public URL.
    let Some(document) = proxy::cached_document(&state, url.clone(), request.headers()).await?
    else {
        return proxy::forward(&state, request.method().clone(), url, request.headers()).await;
    };
    if !document.content_type.contains("json") {
        return proxy::forward(&state, request.method().clone(), url, request.headers()).await;
    }
    let mut value: Value =
        serde_json::from_slice(&document.body).map_err(|_| ProxyError::InvalidUrl)?;
    rewrite_archive_urls(&mut value, &state.public_base_url(request.headers()));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CACHE_CONTROL, proxy::metadata_cache_value())
        .header(header::VARY, proxy::metadata_vary_value())
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        )
        .header("x-mirrorproxy-cache", document.cache_status)
        .body(Body::from(
            serde_json::to_vec(&value).map_err(|_| ProxyError::InvalidUrl)?,
        ))
        .map_err(|_| ProxyError::InvalidHeader)
}
fn sanitize(path: &str) -> Result<String, ProxyError> {
    let path = path.trim_start_matches('/');
    if path.is_empty()
        || path.contains('\\')
        || path
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == ".." || p.contains('\0'))
    {
        return Err(ProxyError::InvalidUrl);
    }
    Ok(path.to_string())
}
/// Maps a retired archive layout onto the endpoint that still serves the file.
/// Anything else keeps its own origin, so an unexpected upstream layout is
/// proxied as advertised instead of being guessed at.
fn live_archive_target(host: &str, path: &str) -> (String, String) {
    for layout in RETIRED_ARCHIVE_LAYOUTS {
        let Some(file) = path.strip_prefix(layout.prefix) else {
            continue;
        };
        if host != layout.host || file.is_empty() || file.contains('/') {
            continue;
        }
        return (layout.live_host.to_string(), format!("{}{file}", layout.live_prefix));
    }
    (host.to_string(), path.to_string())
}
/// Resolves `/pub/upstream/<host>/<path>` back to the archive origin. Every
/// other path returns `Ok(None)` so normal upstream routing takes over.
fn archive_upstream_url(
    path: &str,
    query: Option<&str>,
) -> Result<Option<reqwest::Url>, ProxyError> {
    let Some(rest) = path.strip_prefix("upstream/") else {
        return Ok(None);
    };
    let (host, upstream_path) = rest.split_once('/').ok_or(ProxyError::InvalidUrl)?;
    if !ARCHIVE_HOSTS.contains(&host) {
        return Err(ProxyError::InvalidUrl);
    }
    let (host, upstream_path) = live_archive_target(host, upstream_path);
    let mut url = reqwest::Url::parse(&format!("https://{host}/{upstream_path}"))
        .map_err(|_| ProxyError::InvalidUrl)?;
    url.set_query(query);
    Ok(Some(url))
}
fn repository_url(base: &str, path: &str, query: Option<&str>) -> Result<reqwest::Url, ProxyError> {
    let mut url =
        reqwest::Url::parse(proxy::select_upstream(base)?).map_err(|_| ProxyError::InvalidUrl)?;
    let base_path = url.path().trim_end_matches('/');
    url.set_path(&format!("{base_path}/{path}"));
    url.set_query(query);
    Ok(url)
}
fn rewrite_archive_urls(value: &mut Value, base: &str) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if key == "archive_url" {
                    if let Some(url) = value.as_str() {
                        if let Some(proxied) = proxied_archive_url(url, base) {
                            *value = Value::String(proxied);
                        }
                    }
                } else {
                    rewrite_archive_urls(value, base)
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                rewrite_archive_urls(value, base)
            }
        }
        _ => {}
    }
}
/// `archive_url` moved from Google Storage to per-origin `/api/archives/`
/// paths, so match any supported archive origin instead of one fixed prefix.
fn proxied_archive_url(url: &str, base: &str) -> Option<String> {
    let base = base.trim_end_matches('/');
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let (host, tail) = rest.split_once('/')?;
    if base.is_empty() || rest.contains('#') || !ARCHIVE_HOSTS.contains(&host) {
        return None;
    }
    let (upstream_path, query) = match tail.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (tail, None),
    };
    if sanitize(upstream_path).is_err() {
        return None;
    }
    let (host, upstream_path) = live_archive_target(host, upstream_path);
    let mut proxied = format!("{base}/pub/upstream/{host}/{upstream_path}");
    if let Some(query) = query {
        proxied.push('?');
        proxied.push_str(query);
    }
    Some(proxied)
}
#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use axum::{body::to_bytes, routing::any, Router};

    use crate::config::Config;

    use super::*;
    #[test]
    fn rejects_unsafe_path() {
        assert!(sanitize("../pubspec.yaml").is_err())
    }
    #[test]
    fn rewrites_current_and_legacy_archive_urls() {
        for (archive, expected) in [
            (
                "https://pub.dev/api/archives/http-1.6.0.tar.gz",
                "https://mirror.example/pub/upstream/pub.dev/api/archives/http-1.6.0.tar.gz",
            ),
            (
                "https://storage.flutter-io.cn/dartlang-pub-exported-api/latest/api/archives/http-1.6.0.tar.gz",
                "https://mirror.example/pub/upstream/storage.flutter-io.cn/dartlang-pub-exported-api/latest/api/archives/http-1.6.0.tar.gz",
            ),
            (
                "https://storage.googleapis.com/pub-packages/packages/foo-1.tar.gz",
                "https://mirror.example/pub/upstream/storage.googleapis.com/pub-packages/packages/foo-1.tar.gz",
            ),
        ] {
            let mut value = serde_json::json!({"archive_url": archive});
            rewrite_archive_urls(&mut value, "https://mirror.example/");
            assert_eq!(value["archive_url"], expected);
        }
    }
    #[test]
    fn keeps_unknown_archive_hosts() {
        let mut value = serde_json::json!({
            "versions": [
                {"archive_url": "https://packages.example/foo-1.tar.gz"},
                {"archive_url": "https://pub.dev.evil.example/api/archives/foo-1.tar.gz"},
            ]
        });
        rewrite_archive_urls(&mut value, "https://mirror.example");
        assert_eq!(
            value["versions"][0]["archive_url"],
            "https://packages.example/foo-1.tar.gz"
        );
        assert_eq!(
            value["versions"][1]["archive_url"],
            "https://pub.dev.evil.example/api/archives/foo-1.tar.gz"
        );
    }
    #[test]
    fn preserves_encoded_build_suffixes() {
        let mut value =
            serde_json::json!({"archive_url": "https://pub.dev/api/archives/http-0.2.7%2B0.tar.gz"});
        rewrite_archive_urls(&mut value, "https://mirror.example");
        assert_eq!(
            value["archive_url"],
            "https://mirror.example/pub/upstream/pub.dev/api/archives/http-0.2.7%2B0.tar.gz"
        );
    }
    #[test]
    fn round_trips_rewritten_archive_urls() {
        let mut value =
            serde_json::json!({"archive_url": "https://pub.dev/api/archives/http-1.6.0.tar.gz"});
        rewrite_archive_urls(&mut value, "https://mirror.example");
        let rewritten = value["archive_url"].as_str().expect("rewritten archive url");
        let path = sanitize(
            rewritten
                .strip_prefix("https://mirror.example/pub/")
                .expect("mirror prefix"),
        )
        .expect("valid path");
        let url = archive_upstream_url(&path, None)
            .expect("supported host")
            .expect("archive route");
        assert_eq!(url.as_str(), "https://pub.dev/api/archives/http-1.6.0.tar.gz");
    }
    #[test]
    fn rewrites_retired_mirror_bucket_to_the_live_endpoint() {
        let retired =
            "https://storage.flutter-io.cn/dartlang-pub-public-packages/packages/http-1.6.tar.gz";
        let mut value = serde_json::json!({"archive_url": retired});
        rewrite_archive_urls(&mut value, "https://mirror.example");
        assert_eq!(
            value["archive_url"],
            "https://mirror.example/pub/upstream/pub.flutter-io.cn/api/archives/http-1.6.tar.gz"
        );
    }
    #[test]
    fn serves_retired_bucket_paths_from_the_live_endpoint() {
        let path =
            "upstream/storage.flutter-io.cn/dartlang-pub-public-packages/packages/http-1.6.tar.gz";
        let url = archive_upstream_url(path, None)
            .expect("supported host")
            .expect("archive route");
        assert_eq!(url.host_str(), Some("pub.flutter-io.cn"));
        assert_eq!(url.path(), "/api/archives/http-1.6.tar.gz");
    }
    #[test]
    fn keeps_other_mirror_layouts_on_their_own_origin() {
        let url = archive_upstream_url("upstream/storage.flutter-io.cn/packages/http.tar.gz", None)
            .expect("supported host")
            .expect("archive route");
        assert_eq!(url.host_str(), Some("storage.flutter-io.cn"));
    }
    #[test]
    fn retired_archive_targets_stay_inside_the_allowlist() {
        for layout in RETIRED_ARCHIVE_LAYOUTS {
            assert!(ARCHIVE_HOSTS.contains(&layout.live_host));
        }
    }
    #[test]
    fn rejects_archive_hosts_outside_the_allowlist() {
        assert!(archive_upstream_url("upstream/packages.example/foo-1.tar.gz", None).is_err());
        assert!(archive_upstream_url("upstream/pub.dev@evil.example/foo.tar.gz", None).is_err());
        assert!(archive_upstream_url("api/packages/http", None)
            .expect("not an archive route")
            .is_none());
        let archive = "upstream/pub.dev/api/archives/http-1.6.0.tar.gz";
        let url = archive_upstream_url(archive, Some("v=1"))
            .expect("supported host")
            .expect("archive route");
        assert_eq!(url.as_str(), "https://pub.dev/api/archives/http-1.6.0.tar.gz?v=1");
    }
    #[test]
    fn archives_stream_through_the_cache_path() {
        assert!(is_archive_path("api/archives/http-1.6.0.tar.gz"));
        assert!(!is_archive_path("api/packages/http"));
    }

    type MockUpstream = (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>);

    async fn spawn_metadata_upstream() -> MockUpstream {
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&requests);
        let app = Router::new().fallback(any(move |_: axum::extract::Request| {
            let observed = Arc::clone(&observed);
            async move {
                observed.fetch_add(1, Ordering::SeqCst);
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    r#"{"name":"http","latest":{"version":"1.6.0","archive_url":"https://pub.dev/api/archives/http-1.6.0.tar.gz"}}"#,
                )
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{address}/repository"), requests, task)
    }

    #[tokio::test]
    async fn package_metadata_is_rewritten_from_a_cached_upstream_document() {
        let (upstream, requests, task) = spawn_metadata_upstream().await;
        let directory = std::env::temp_dir().join(format!(
            "mirrorproxy-pub-cache-{}",
            uuid::Uuid::new_v4()
        ));
        let mut config = Config {
            public_base_url: "https://mirror.example".to_string(),
            upstreams: crate::config::Upstreams {
                pub_repository: upstream,
                ..crate::config::Upstreams::default()
            },
            ..Config::default()
        };
        config.cache.enabled = true;
        config.cache.directory = directory.display().to_string();
        config.cache.max_entry_mb = 1;
        config.cache.max_total_mb = 8;
        let state = crate::test_app_state(config).await;

        for expected_status in ["MISS", "HIT"] {
            let response = proxy(
                State(state.clone()),
                Path("api/packages/http".to_string()),
                axum::extract::Request::builder()
                    .uri("/pub/api/packages/http")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["x-mirrorproxy-cache"], expected_status);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let value: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                value["latest"]["archive_url"],
                "https://mirror.example/pub/upstream/pub.dev/api/archives/http-1.6.0.tar.gz"
            );
        }
        // The document is fetched once and rewritten for every client.
        assert_eq!(requests.load(Ordering::SeqCst), 1);

        crate::proxy::purge_cache(state.config().cache.clone())
            .await
            .unwrap();
        let _ = std::fs::remove_dir_all(directory);
        task.abort();
    }
}
