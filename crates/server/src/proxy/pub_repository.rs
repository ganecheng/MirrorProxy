use super::ProxyError;
use crate::{proxy, AppState};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderValue},
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
    let response = proxy::get_with_fallback(&state, url).await?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    if !(status.is_success() && content_type.contains("json")) {
        let url = repository_url(&config.upstreams.pub_repository, &path, query)?;
        return proxy::forward(&state, request.method().clone(), url, request.headers()).await;
    }
    let bytes = response.bytes().await?;
    let mut value: Value = serde_json::from_slice(&bytes).map_err(|_| ProxyError::InvalidUrl)?;
    rewrite_archive_urls(&mut value, &state.public_base_url(request.headers()));
    Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, proxy::metadata_cache_value())
        .header(header::VARY, proxy::metadata_vary_value())
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        )
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
    let mut proxied = format!("{base}/pub/upstream/{host}/{upstream_path}");
    if let Some(query) = query {
        proxied.push('?');
        proxied.push_str(query);
    }
    Some(proxied)
}
#[cfg(test)]
mod tests {
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
}
