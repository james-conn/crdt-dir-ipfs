use futures_util::Stream;
use futures_util::StreamExt;
use tokio_util::codec::{FramedRead, LinesCodec};
use serde::{Deserialize,Deserializer,Serialize,Serializer};
use reqwest::Client;

use super::keys::IpnsKey;
use super::ipfs::IpfsCid;

use std::str::FromStr;
use anyhow::{anyhow, Result};

use futures_util::TryStreamExt;


/// Represents an IPFS Path which can be either:
/// - `/ipfs/<cid>`
/// - `/ipns/<name>` where `<name>` is either an IPNS key (string) or a CID
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpfsPath {
    Ipfs(IpfsCid),
    Ipns(IpnsKey),
}

impl IpfsPath {
    pub fn as_str(&self) -> String {
        match self {
            IpfsPath::Ipfs(cid) => format!("/ipfs/{}", cid),
            IpfsPath::Ipns(cid) => format!("/ipns/{}", cid),
        }
    }
}

impl FromStr for IpfsPath {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if let Some(stripped) = s.strip_prefix("/ipfs/") {
            let cid = IpfsCid::from_str(stripped)
                .map_err(|_| anyhow!("Invalid CID in /ipfs/ path"))?;
            Ok(IpfsPath::Ipfs(cid))
        } else if let Some(stripped) = s.strip_prefix("/ipns/") {
            let key = IpnsKey::from_str(stripped)
                .map_err(|_| anyhow!("Invalid IPNS key in /ipns/ path: {}", stripped))?;
            Ok(IpfsPath::Ipns(key))
        } else {
            Err(anyhow!("IPFS path must start with /ipfs/ or /ipns/"))
        }
    }
}

impl<'de> Deserialize<'de> for IpfsPath {
    fn deserialize<D>(deserializer: D) -> Result<IpfsPath, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        IpfsPath::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Serialize for IpfsPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_str())
    }
}

#[cfg(test)]
mod path_test {
    use super::*;

    #[test]
    fn test_parse_ipfs_path_ipfs() {
        let cid_str = "QmYwAPJzv5CZsnAzt8auV2hSLg7T5kr3hA1NSSp1u93w6X";
        let path_str = format!("/ipfs/{}", cid_str);
        let path = path_str.parse::<IpfsPath>().expect("should parse");

        match path {
            IpfsPath::Ipfs(cid) => {
                assert_eq!(cid.to_string(), cid_str);
            }
            _ => panic!("Expected Ipfs variant"),
        }
    }

    #[test]
    fn test_parse_ipfs_path_ipns_cid() {
        let key_str = "k51qzi5uqu5dgndmfpeorlwuar7u66p9g9l0dolwy2v7sm6dt5sorjityev4ib";
        let path_str = format!("/ipns/{}", key_str);
        let path = path_str.parse::<IpfsPath>().expect("should parse");

        match path {
            IpfsPath::Ipns(cid) => {
                assert_eq!(cid.to_string(), key_str);
            }
            _ => panic!("Expected Ipns variant"),
        }
    }

    #[test]
    fn test_as_str() {
        let cid_str = "QmYwAPJzv5CZsnAzt8auV2hSLg7T5kr3hA1NSSp1u93w6X";

        let ipfs_path = IpfsPath::Ipfs(IpfsCid::from_str(cid_str).unwrap());
        assert_eq!(ipfs_path.as_str(), format!("/ipfs/{}", cid_str));

        let key_str = "k51qzi5uqu5dgndmfpeorlwuar7u66p9g9l0dolwy2v7sm6dt5sorjityev4ib";

        let ipns_cid = IpfsPath::Ipns(IpnsKey::from_str(key_str).unwrap());
        assert_eq!(ipns_cid.as_str(), format!("/ipns/{}", key_str));
    }

    #[test]
    fn test_invalid_paths() {
        let bad_paths = ["", "/foo/bar", "/ipfs/", "/ipns/"];
        for p in &bad_paths {
            assert!(p.parse::<IpfsPath>().is_err(), "Path '{}' should be invalid", p);
        }
    }

    #[test]
    fn ipfspath_from_str_valid_ipfs() {
        let s = "/ipfs/QmYwAPJzv5CZsnAztbCQo6P1Db8PeH6UX5nq4MF6G8aH9A";
        let ipfs_path = IpfsPath::from_str(s).expect("should parse");
        match ipfs_path {
            IpfsPath::Ipfs(cid) => {
                assert_eq!(cid.to_string(), "QmYwAPJzv5CZsnAztbCQo6P1Db8PeH6UX5nq4MF6G8aH9A");
            }
            _ => panic!("Expected Ipfs variant"),
        }
    }

    #[test]
    fn ipfspath_from_str_valid_ipns_cid() {
        // Just use a valid CID string after /ipns/
        let s = "/ipns/k51qzi5uqu5dgndmfpeorlwuar7u66p9g9l0dolwy2v7sm6dt5sorjityev4ib";
        let ipfs_path = IpfsPath::from_str(s).expect("should parse");
        match ipfs_path {
            IpfsPath::Ipns(cid) => {
                assert_eq!(cid.to_string(), "k51qzi5uqu5dgndmfpeorlwuar7u66p9g9l0dolwy2v7sm6dt5sorjityev4ib");
            }
            _ => panic!("Expected Ipns variant"),
        }
    }

    #[test]
    fn ipfspath_from_str_invalid() {
        let s = "/invalid/path";
        assert!(IpfsPath::from_str(s).is_err());
    }
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
pub struct PublishResponse {
    pub Name: String,      // IPNS key name as string
    pub Value: IpfsPath,   // IPFS path parsed into IpfsPath enum
}


/// Publishes an IPFS path under an IPNS key.
/// - `base_url`: base URL of the IPFS API daemon, e.g. "http://127.0.0.1:5001"
/// - `ipfs_path`: The IPFS/IPNS path to publish.
/// - `key`: Optional key name (e.g., "self").
/// - `lifetime`: Optional lifetime string (e.g., "24h").
/// - `ttl`: Optional ttl string.
/// Returns a `PublishResponse`.
///
pub async fn name_publish(
    base_url: &str,
    ipfs_path: &IpfsPath,
    key: &IpnsKey,
    lifetime: Option<&str>,
    ttl: Option<&str>,
) -> Result<PublishResponse> {
    let client = Client::new();

    let mut params = vec![
        ("arg", ipfs_path.as_str()),
        ("key", key.to_string()),
    ];

    if let Some(l) = lifetime {
        params.push(("lifetime", l.to_string()));
    }
    if let Some(t) = ttl {
        params.push(("ttl", t.to_string()));
    }

    let response = client
        .post(&format!("{}/api/v0/name/publish", base_url))
        .query(&params)
        .send()
        .await?;

    let parsed = response.json::<PublishResponse>().await?;
    Ok(parsed)
}

/// Resolves an IPNS key asynchronously.
/// If `stream` is true, returns a stream of `ResolveResponse` as they arrive.
/// Otherwise, returns a single-item stream with the resolved path.
/// Optional query params control behavior.
///
/// - `base_url`: IPFS API base URL
/// - `name`: IPNS key name string
///
pub async fn name_resolve_streaming(
    base_url: &str,
    name: &IpnsKey,
    stream: bool,
    recursive: Option<bool>,
    nocache: Option<bool>,
    dht_record_count: Option<u32>,
    dht_timeout: Option<&str>,
) -> Result<impl Stream<Item = Result<IpfsPath>>> {
    let client = Client::new();

    let mut params = vec![("arg", name.to_string())];
    if stream {
        params.push(("stream", "true".to_string()));
    }
    if let Some(r) = recursive {
        params.push(("recursive", if r { "true".to_string() } else { "false".to_string() }));
    }
    if let Some(nc) = nocache {
        params.push(("nocache", if nc { "true".to_string() } else { "false".to_string() }));
    }
    if let Some(count) = dht_record_count {
        params.push(("dht-record-count", count.to_string()));
    }
    if let Some(timeout) = dht_timeout {
        params.push(("dht-timeout", timeout.to_string()));
    }

    let response = client
        .post(&format!("{}/api/v0/name/resolve", base_url))
        .query(&params)
        .send()
        .await?;

    let stream = response.bytes_stream();

    let line_stream = FramedRead::new(
        tokio_util::io::StreamReader::new(
            stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        ),
        LinesCodec::new(),
    );

    #[derive(Deserialize, Debug)]
    #[allow(non_snake_case)]
    struct ResolveResponse {
        pub Path: IpfsPath,    // IPFS path parsed into IpfsPath enum
    }

    let parsed_stream = line_stream.map(|line_result| {
        let line = line_result?;
        let item = serde_json::from_str::<ResolveResponse>(&line)?;
        Ok(item.Path)
    });

    Ok(parsed_stream)
}

#[cfg(test)]
mod apitests {
    use super::*;
    use tokio_stream::StreamExt;

    const LOCAL_IPFS_API: &str = "http://127.0.0.1:5001";

    #[tokio::test]
    async fn test_name_publish_and_resolve() -> Result<(), anyhow::Error> {
        // Example: publish /ipfs/Qm... under key "self"
        let cid = IpfsCid::from_str("QmdbWa3wBGwQ4suXjEpPkrigP3UmBMECdJNmkHfz6btqaJ").unwrap();
        let ipfs_path = IpfsPath::Ipfs(cid);

        let ipns_key = IpnsKey::from_str("k51qzi5uqu5dgndmfpeorlwuar7u66p9g9l0dolwy2v7sm6dt5sorjityev4ib").unwrap();

        // Publish with default key "self"
        let publish_resp = name_publish(LOCAL_IPFS_API, &ipfs_path, &ipns_key, None, None).await?;
        println!("Publish response: {:?}", publish_resp);

        // Resolve the published name (with streaming = false)
        let mut resolve_stream = name_resolve_streaming(LOCAL_IPFS_API, &ipns_key, false, None, None, None, None).await?;
        if let Some(res) = resolve_stream.next().await {
            let res = res?;
            println!("Resolve response: {:?}", res);
            assert_eq!(res, ipfs_path);
        } else {
            panic!("Expected at least one resolve result");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_name_resolve_streaming_multiple() -> Result<(), anyhow::Error> {
        let ipns_key = IpnsKey::from_str("k51qzi5uqu5dgndmfpeorlwuar7u66p9g9l0dolwy2v7sm6dt5sorjityev4ib").unwrap();

        // Streaming resolve test on "self"
        let mut stream = name_resolve_streaming(LOCAL_IPFS_API, &ipns_key, true, None, None, None, None).await?;

        // We'll read a few lines from the stream and print them
        for _ in 0..3 {
            if let Some(item) = stream.next().await {
                match item {
                    Ok(resp) => println!("Streamed resolve: {:?}", resp),
                    Err(e) => eprintln!("Stream error: {:?}", e),
                }
            }
        }

        Ok(())
    }
}
