//! Cluster trust and transport.
//!
//! The control plane generates one self-signed TLS identity on first start. The join token
//! carries a fingerprint of that certificate plus a shared secret, so a client can verify it
//! is talking to the right cluster before it sends the secret (the same idea as a K3s node
//! token). Every connection — CLI, agent to server, agent to agent — is TLS pinned to that
//! one certificate, so nothing depends on DNS names or a public CA.
//!
//! Trust model: the token is cluster-admin. Whoever holds it can run commands on every node.

use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::de::DeserializeOwned;

use crate::proto::ApiError;

/// Name in the cluster certificate. Clients connect by IP and present this as SNI.
pub const TLS_NAME: &str = "dist-alloc.internal";
pub const DEFAULT_PORT: u16 = 7443;

#[derive(Clone)]
pub struct Identity {
    pub cert_pem: String,
    pub key_pem: String,
}

impl Identity {
    pub fn load_or_create(dir: &Path) -> Result<Self> {
        let (cert_path, key_path) = (dir.join("cluster.crt"), dir.join("cluster.key"));
        match (cert_path.exists(), key_path.exists()) {
            (true, true) => {
                return Ok(Self {
                    cert_pem: std::fs::read_to_string(&cert_path)
                        .with_context(|| format!("reading {}", cert_path.display()))?,
                    key_pem: std::fs::read_to_string(&key_path)
                        .with_context(|| format!("reading {}", key_path.display()))?,
                });
            }
            (false, false) => {}
            // A new identity would silently invalidate every device's token.
            _ => bail!(
                "{} has only half of the cluster identity (cluster.crt and cluster.key); restore the \
                 missing file, or delete both to start a new cluster that every device must rejoin",
                dir.display()
            ),
        }
        let generated = rcgen::generate_simple_self_signed(vec![TLS_NAME.to_owned()])
            .context("generating cluster certificate")?;
        let identity = Self {
            cert_pem: generated.cert.pem(),
            key_pem: generated.signing_key.serialize_pem(),
        };
        std::fs::write(&cert_path, &identity.cert_pem)?;
        write_private(&key_path, &identity.key_pem)?;
        Ok(identity)
    }
}

/// Writes a secret readable only by the owner.
pub fn write_private(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents.as_bytes())
}

/// 128-bit fingerprint of a PEM certificate (over its base64 body, whitespace-insensitive).
pub fn fingerprint(cert_pem: &str) -> String {
    let body: String = cert_pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .flat_map(str::chars)
        .filter(|c| !c.is_whitespace())
        .collect();
    blake3::hash(body.as_bytes()).to_hex()[..32].to_owned()
}

#[derive(Clone)]
pub struct Token {
    pub fingerprint: String,
    pub secret: String,
}

impl Token {
    pub fn generate(cert_pem: &str) -> Self {
        Self {
            fingerprint: fingerprint(cert_pem),
            secret: format!("{:032x}", rand::random::<u128>()),
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        let parts: Vec<&str> = raw.trim().split('.').collect();
        match parts.as_slice() {
            ["da1", fp, secret] if fp.len() == 32 && !secret.is_empty() => Ok(Self {
                fingerprint: (*fp).to_owned(),
                secret: (*secret).to_owned(),
            }),
            _ => bail!("malformed token (expected da1.<fingerprint>.<secret>)"),
        }
    }

    /// Credential agents present to each other for blob transfer, so the admin secret is
    /// only ever sent to the control plane.
    pub fn peer_key(&self) -> String {
        blake3::Hasher::new_derive_key("dist-alloc v1 peer blob transfer")
            .update(self.secret.as_bytes())
            .finalize()
            .to_hex()
            .to_string()
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "da1.{}.{}", self.fingerprint, self.secret)
    }
}

pub fn constant_time_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// A non-2xx answer from a cluster endpoint.
#[derive(Debug)]
pub struct ApiFailure {
    pub status: u16,
    pub message: String,
}

impl fmt::Display for ApiFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (HTTP {})", self.message, self.status)
    }
}

impl std::error::Error for ApiFailure {}

/// `addr` as `host:port` for a lookup, adding [`DEFAULT_PORT`] when no port is given.
/// Accepts `host`, `host:port`, `1.2.3.4`, `::1`, `[::1]` and `[::1]:port`, optionally with a
/// leading `https://` or a trailing `/`.
pub fn with_default_port(addr: &str) -> String {
    let addr = addr
        .trim()
        .trim_start_matches("https://")
        .trim_end_matches('/');
    if addr.parse::<SocketAddr>().is_ok() {
        return addr.to_owned();
    }
    if let Ok(ip) = addr
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
    {
        return SocketAddr::new(ip, DEFAULT_PORT).to_string();
    }
    match addr.rsplit_once(':') {
        Some((_, port)) if port.parse::<u16>().is_ok() => addr.to_owned(),
        _ => format!("{addr}:{DEFAULT_PORT}"),
    }
}

pub async fn resolve(addr: &str) -> Result<SocketAddr> {
    let with_port = with_default_port(addr);
    let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host(&with_port)
        .await
        .with_context(|| format!("resolving {with_port}"))?
        .collect();
    addrs.sort_by_key(|a| !a.is_ipv4());
    addrs
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("{with_port} resolved to no addresses"))
}

/// A pinned-TLS, bearer-authenticated connection to one cluster member.
#[derive(Clone)]
pub struct Endpoint {
    client: reqwest::Client,
    base: String,
    bearer: String,
    pub addr: SocketAddr,
}

impl Endpoint {
    /// Connects trusting exactly `cert_pem`.
    pub fn pinned(addr: SocketAddr, cert_pem: &str, bearer: &str) -> Result<Self> {
        let cert = reqwest::Certificate::from_pem(cert_pem.as_bytes())?;
        let client = reqwest::Client::builder()
            .tls_certs_only([cert])
            .resolve(TLS_NAME, addr)
            .connect_timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            client,
            base: format!("https://{TLS_NAME}:{}", addr.port()),
            bearer: bearer.to_owned(),
            addr,
        })
    }

    /// Connects to the control plane knowing only the token: fetches the server's
    /// certificate over an unverified channel, accepts it only if it matches the token's
    /// fingerprint, and from then on trusts nothing else. Returns the certificate too.
    pub async fn bootstrap(server: &str, token: &Token) -> Result<(Self, String)> {
        let addr = resolve(server).await?;
        let insecure = reqwest::Client::builder()
            .tls_danger_accept_invalid_certs(true)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()?;
        let cert_pem = insecure
            .get(format!("https://{addr}/v1/ca"))
            .send()
            .await
            .with_context(|| format!("connecting to control plane at {addr}"))?
            .error_for_status()?
            .text()
            .await?;
        if !constant_time_eq(&fingerprint(&cert_pem), &token.fingerprint) {
            bail!(
                "the server at {addr} does not match this token (wrong cluster, or someone is intercepting the connection)"
            );
        }
        Ok((Self::pinned(addr, &cert_pem, &token.secret)?, cert_pem))
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(&self.bearer)
    }

    pub fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::GET, path)
    }

    pub fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::POST, path)
    }

    pub fn put(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::PUT, path)
    }

    pub fn delete(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::DELETE, path)
    }
}

/// Sends a request, turning non-2xx answers into [`ApiFailure`].
pub async fn send(request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
    let response = request.send().await?;
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    let message = serde_json::from_str::<ApiError>(&body)
        .map(|e| e.error)
        .unwrap_or(body);
    Err(ApiFailure {
        status: status.as_u16(),
        message,
    }
    .into())
}

pub async fn send_json<T: DeserializeOwned>(request: reqwest::RequestBuilder) -> Result<T> {
    Ok(send(request).await?.json().await?)
}

/// IPv4 addresses of this machine that other devices could plausibly reach (interfaces that
/// are up, not loopback, not point-to-point tunnels).
pub fn lan_addresses() -> Vec<std::net::Ipv4Addr> {
    use nix::net::if_::InterfaceFlags;
    let Ok(interfaces) = nix::ifaddrs::getifaddrs() else {
        return Vec::new();
    };
    interfaces
        .filter(|ifa| {
            ifa.flags.contains(InterfaceFlags::IFF_UP)
                && !ifa
                    .flags
                    .intersects(InterfaceFlags::IFF_LOOPBACK | InterfaceFlags::IFF_POINTOPOINT)
        })
        .filter_map(|ifa| Some(ifa.address?.as_sockaddr_in()?.ip()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trips_and_rejects_garbage() {
        let token =
            Token::generate("-----BEGIN CERTIFICATE-----\nAAAA\nBBBB\n-----END CERTIFICATE-----\n");
        let parsed = Token::parse(&token.to_string()).unwrap();
        assert_eq!(parsed.fingerprint, token.fingerprint);
        assert_eq!(parsed.secret, token.secret);
        assert_ne!(parsed.peer_key(), parsed.secret);
        assert!(Token::parse("da1.short.x").is_err());
        assert!(Token::parse("nonsense").is_err());
    }

    #[test]
    fn default_port_is_added_only_when_missing() {
        for (input, expected) in [
            ("10.0.0.5", "10.0.0.5:7443"),
            ("10.0.0.5:9000", "10.0.0.5:9000"),
            ("https://pool.lan/", "pool.lan:7443"),
            ("pool.lan:8443", "pool.lan:8443"),
            ("::1", "[::1]:7443"),
            ("[fe80::1]", "[fe80::1]:7443"),
            ("[::1]:9000", "[::1]:9000"),
        ] {
            assert_eq!(with_default_port(input), expected, "{input}");
        }
    }

    #[test]
    fn token_comparison_is_exact() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "abcd"));
    }

    #[test]
    fn token_parse_trims_whitespace_and_requires_the_prefix() {
        let fp = "0".repeat(32);
        let parsed = Token::parse(&format!("  da1.{fp}.secret\n")).expect("valid token");
        assert_eq!(parsed.secret, "secret");
        assert!(Token::parse(&format!("da2.{fp}.secret")).is_err());
        assert!(Token::parse(&format!("da1.{fp}.")).is_err());
        assert!(Token::parse(&format!("da1.{fp}.a.b")).is_err());
    }

    #[test]
    fn fingerprint_ignores_pem_line_wrapping() {
        let a = "-----BEGIN CERTIFICATE-----\nAAAABBBB\n-----END CERTIFICATE-----\n";
        let b = "-----BEGIN CERTIFICATE-----\r\nAAAA\r\nBBBB\r\n-----END CERTIFICATE-----";
        assert_eq!(fingerprint(a), fingerprint(b));
        assert_ne!(fingerprint(a), fingerprint(&a.replace("AAAA", "AAAB")));
    }
}
