use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use serde_json::Value;

use crate::{proxy, AppState};

use super::ProxyError;

pub async fn root(State(state): State<AppState>) -> Result<Response, ProxyError> {
    if !state.config().is_enabled("nuget") {
        return Err(ProxyError::Disabled("nuget"));
    }

    Response::builder()
        .status(200)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )
        .body(Body::from("MirrorProxy NuGet v3 repository proxy\n"))
        .map_err(|_| ProxyError::InvalidHeader)
}

pub async fn service_index(
    State(state): State<AppState>,
    request: axum::extract::Request,
) -> Result<Response, ProxyError> {
    let config = state.config();
    if !config.is_enabled("nuget") {
        return Err(ProxyError::Disabled("nuget"));
    }

    let upstream_path = "/v3/index.json";
    let upstream = proxy::select_upstream(&config.upstreams.nuget)?;
    let url = proxy::build_url(upstream, upstream_path, request.uri().query())?;
    // The service index is cached exactly as published and rewritten per request,
    // because every resource URL must point at this instance.
    let document = proxy::cached_document(&state, url, request.headers()).await?;
    let document = document.filter(|entry| entry.content_type.contains("json"));
    if let Some(document) = document {
        let mut value: Value =
            serde_json::from_slice(&document.body).map_err(|_| ProxyError::InvalidUrl)?;
        rewrite_upstream_urls(
            &mut value,
            upstream,
            &state.public_base_url(request.headers()),
        );
        return Response::builder()
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
            .map_err(|_| ProxyError::InvalidHeader);
    }

    let fallback_url = proxy::build_url(upstream, upstream_path, request.uri().query())?;
    proxy::forward(
        &state,
        request.method().clone(),
        fallback_url,
        request.headers(),
    )
    .await
}

pub async fn proxy(
    State(state): State<AppState>,
    Path(path): Path<String>,
    request: axum::extract::Request,
) -> Result<Response, ProxyError> {
    let config = state.config();
    if !config.is_enabled("nuget") {
        return Err(ProxyError::Disabled("nuget"));
    }

    let clean_path = sanitize_repository_path(&path)?;
    let configured_upstream = proxy::select_upstream(&config.upstreams.nuget)?;
    let (upstream, upstream_path) = select_upstream(&clean_path, configured_upstream)?;
    let url = proxy::build_url(&upstream, &upstream_path, request.uri().query())?;
    proxy::forward(&state, request.method().clone(), url, request.headers()).await
}

fn sanitize_repository_path(path: &str) -> Result<String, ProxyError> {
    let path = path.trim_start_matches('/');
    if path.is_empty()
        || path.contains('\\')
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == ".." || part.contains('\0'))
    {
        return Err(ProxyError::InvalidUrl);
    }
    Ok(path.to_string())
}

fn select_upstream(path: &str, configured_upstream: &str) -> Result<(String, String), ProxyError> {
    let Some((host, remaining_path)) = path
        .strip_prefix("upstream/")
        .and_then(|value| value.split_once('/'))
    else {
        return Ok((configured_upstream.to_string(), format!("/{path}")));
    };
    let configured_url =
        reqwest::Url::parse(configured_upstream).map_err(|_| ProxyError::InvalidUrl)?;
    let configured_host = configured_url.host_str().ok_or(ProxyError::InvalidUrl)?;
    if host == configured_host {
        return Ok((
            configured_upstream.to_string(),
            format!("/{remaining_path}"),
        ));
    }
    if !is_official_resource_host(host) {
        return Err(ProxyError::UnsupportedTarget);
    }
    Ok((format!("https://{host}"), format!("/{remaining_path}")))
}

fn rewrite_upstream_urls(document: &mut Value, upstream: &str, public_base_url: &str) {
    match document {
        Value::String(_) => {}
        Value::Array(values) => {
            for value in values {
                rewrite_upstream_urls(value, upstream, public_base_url);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if key == "@id" {
                    rewrite_resource_url(value, upstream, public_base_url);
                } else {
                    rewrite_upstream_urls(value, upstream, public_base_url);
                }
            }
        }
        _ => {}
    }
}

fn rewrite_resource_url(value: &mut Value, configured_upstream: &str, public_base_url: &str) {
    let Some(original) = value.as_str() else {
        return;
    };
    let configured_host = reqwest::Url::parse(configured_upstream)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string));
    let hosts = [
        configured_host.as_deref(),
        Some("api.nuget.org"),
        Some("azuresearch-usnc.nuget.org"),
        Some("azuresearch-ussc.nuget.org"),
        Some("www.nuget.org"),
    ];
    for host in hosts.into_iter().flatten() {
        let prefix = format!("https://{host}");
        if let Some(path) = original.strip_prefix(&prefix) {
            *value = Value::String(format!(
                "{}/nuget/upstream/{host}{path}",
                public_base_url.trim_end_matches('/')
            ));
            return;
        }
    }
}

fn is_official_resource_host(host: &str) -> bool {
    matches!(
        host,
        "api.nuget.org"
            | "azuresearch-usnc.nuget.org"
            | "azuresearch-ussc.nuget.org"
            | "www.nuget.org"
    )
}

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, http::Request, routing::any, Router};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::*;

    #[test]
    fn accepts_v3_resource_and_package_paths() {
        assert!(sanitize_repository_path("v3-flatcontainer/newtonsoft.json/index.json").is_ok());
        assert!(
            sanitize_repository_path("registration5-gz-semver2/newtonsoft.json/index.json").is_ok()
        );
        assert!(sanitize_repository_path(
            "v3-flatcontainer/newtonsoft.json/13.0.3/newtonsoft.json.13.0.3.nupkg"
        )
        .is_ok());
    }

    #[test]
    fn rejects_traversal_and_empty_paths() {
        assert!(sanitize_repository_path("../NuGet.Config").is_err());
        assert!(sanitize_repository_path("v3-flatcontainer//pkg").is_err());
        assert!(sanitize_repository_path("v3-flatcontainer\\pkg").is_err());
    }

    #[test]
    fn rewrites_all_upstream_resource_urls() {
        let mut value = serde_json::json!({
            "resources": [
                { "@id": "https://api.nuget.org/v3-flatcontainer/", "@type": "PackageBaseAddress/3.0.0" },
                { "@id": "https://azuresearch-usnc.nuget.org/query", "nested": ["https://api.nuget.org/registration5-gz-semver2/"] },
                { "@id": "https://elsewhere.example/v3/index.json" }
            ]
        });

        rewrite_upstream_urls(
            &mut value,
            "https://api.nuget.org",
            "https://mirror.example",
        );
        assert_eq!(
            value["resources"][0]["@id"],
            "https://mirror.example/nuget/upstream/api.nuget.org/v3-flatcontainer/"
        );
        assert_eq!(
            value["resources"][1]["@id"],
            "https://mirror.example/nuget/upstream/azuresearch-usnc.nuget.org/query"
        );
        assert_eq!(
            value["resources"][1]["nested"][0],
            "https://api.nuget.org/registration5-gz-semver2/"
        );
        assert_eq!(
            value["resources"][2]["@id"],
            "https://elsewhere.example/v3/index.json"
        );
    }

    #[test]
    fn routes_only_configured_or_official_resource_hosts() {
        assert_eq!(
            select_upstream(
                "upstream/azuresearch-usnc.nuget.org/query",
                "https://api.nuget.org"
            )
            .unwrap(),
            (
                "https://azuresearch-usnc.nuget.org".to_string(),
                "/query".to_string()
            )
        );
        assert!(select_upstream("upstream/evil.example/query", "https://api.nuget.org").is_err());
    }

    async fn nuget_test_state(upstream: String) -> (AppState, std::path::PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "mirrorproxy-nuget-document-{}",
            uuid::Uuid::new_v4()
        ));
        let mut config = crate::config::Config::default();
        config.upstreams.nuget = upstream;
        config.cache.enabled = true;
        config.cache.directory = directory.display().to_string();
        config.cache.max_entry_mb = 1;
        config.cache.max_total_mb = 8;
        (crate::test_app_state(config).await, directory)
    }

    #[tokio::test]
    async fn cached_service_index_is_rewritten_for_every_request() {
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        let upstream = Router::new().fallback(any(move || {
            let observed = observed.clone();
            async move {
                observed.fetch_add(1, Ordering::SeqCst);
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    r#"{"resources":[{"@id":"https://api.nuget.org/v3-flatcontainer/"}]}"#,
                )
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, upstream).await.unwrap();
        });
        let (state, directory) = nuget_test_state(format!("http://{address}")).await;

        let mut cache_statuses = Vec::new();
        for _ in 0..2 {
            let response = service_index(
                State(state.clone()),
                Request::builder()
                    .uri("/nuget/v3/index.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            cache_statuses.push(
                response.headers()["x-mirrorproxy-cache"]
                    .to_str()
                    .unwrap()
                    .to_string(),
            );
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(body["resources"][0]["@id"]
                .as_str()
                .unwrap()
                .contains("/nuget/upstream/"));
        }
        assert_eq!(cache_statuses, ["MISS", "HIT"]);
        assert_eq!(requests.load(Ordering::SeqCst), 1);

        crate::proxy::purge_cache(state.config().cache.clone())
            .await
            .unwrap();
        let _ = std::fs::remove_dir_all(directory);
        server.abort();
    }
}
