use cid::{Cid, multibase::Base};
use std::fmt;
use reqwest::Client;
use anyhow::{Result, Context,bail};
use serde::{Deserialize,Deserializer};
use reqwest::StatusCode;

use std::str::FromStr;

const LIBP2P_KEY_CODE: u64 = 0x72;

/// Newtype for IPNS keys
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IpnsKey(pub Cid);
impl From<IpnsKey> for Cid {
    fn from(key: IpnsKey) -> Self {
        key.0
    }
}

impl std::convert::TryFrom<Cid> for IpnsKey {
    type Error = &'static str;

    fn try_from(cid: Cid) -> Result<Self, Self::Error> {
        if cid.codec() == LIBP2P_KEY_CODE {
            Ok(IpnsKey(cid))
        } else {
            Err("Not an IPNS key")
        }
    }
}

impl FromStr for IpnsKey {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cid = Cid::from_str(s).map_err(|_| "Invalid CID")?;
        IpnsKey::try_from(cid)
    }
}

impl TryFrom<String> for IpnsKey {
    type Error = &'static str;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(&s)
    }
}

impl<'de> Deserialize<'de> for IpnsKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        IpnsKey::from_str(&s).map_err(serde::de::Error::custom)
    }
}


impl std::fmt::Display for IpnsKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.to_string_of_base(Base::Base36Lower) {
            Ok(encoded) => write!(f, "{}", encoded),
            Err(_) => Err(fmt::Error), // convert cid::Error into fmt::Error
        }
    }
}

#[cfg(test)]
mod ipns_key_test {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_valid_ipns_keys() {
        let inputs = vec![
            ("k51qzi5uqu5dgndmfpeorlwuar7u66p9g9l0dolwy2v7sm6dt5sorjityev4ib", "self"),
            ("k51qzi5uqu5diyjoiyz6khv249l3puwbir19wiw1e3lehe4uw6g28pmtslcgqn", "hi"),
        ];

        for (s, label) in inputs {
            let parsed = IpnsKey::from_str(s);
            assert!(
                parsed.is_ok(),
                "Expected IPNS key '{}' ({}) to parse successfully",
                s, label
            );

            let cid = parsed.unwrap().0;
            assert_eq!(cid.codec(), 0x72, "Expected multicodec 0x72 for {}", label);
        }
    }

    #[test]
    fn test_invalid_ipns_keys_from_ipfs_cids() {
        let ipfs_cids = vec![
            "QmdbWa3wBGwQ4suXjEpPkrigP3UmBMECdJNmkHfz6btqaJ",
            "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
        ];

        for cid_str in ipfs_cids {
            let parsed = IpnsKey::from_str(cid_str);
            assert!(
                parsed.is_err(),
                "Expected IPFS CID '{}' to be rejected as an IPNS key",
                cid_str
            );
        }
    }

    #[test]
    fn test_invalid_format_string() {
        let bad_str = "not-a-cid";
        assert!(
            IpnsKey::from_str(bad_str).is_err(),
            "Expected '{}' to fail parsing as CID", bad_str
        );
    }
}


/// Generates a new IPNS key with the given name.
pub async fn generate_ipns_key(base_url: &str, name: &str) -> Result<IpnsKey> {
    let url = format!("{}/api/v0/key/gen", base_url);
    let client = Client::new();

    let response = client
        .post(&url)
        .query(&[("arg", name)])
        .send()
        .await
        .context("Failed to send request to /key/gen")?;

    #[derive(Debug, Deserialize)]
    #[allow(non_snake_case)]
    struct KeyGenResponse {
        Name: String,
        Id: IpnsKey,
    }

    #[derive(Debug, Deserialize)]
    #[allow(non_snake_case)]
    struct IpfsErrorResponse {
        Message: String,
        Code: u32,
        Type: String,
    }

    if response.status() == StatusCode::OK {
        let key_info: KeyGenResponse = response
            .json()
            .await
            .context("Failed to deserialize key generation response")?;

        IpnsKey::try_from(key_info.Id).context("Invalid IPNS key returned by daemon")
    } else {
        let err_body: IpfsErrorResponse = response.json().await.unwrap_or_else(|_| IpfsErrorResponse {
            Message: "Unknown error".to_string(),
            Code: 0,
            Type: "error".to_string(),
        });
        bail!("IPFS key generation failed: {}", err_body.Message)
    }
}

#[cfg(test)]
mod api_tests {
    use super::*;

    // Base URL of a running local IPFS daemon
    const LOCAL_IPFS: &str = "http://127.0.0.1:5001";

    #[tokio::test]
    async fn test_generate_ipns_key() {
        // Give the key a unique-ish name
        let key_name = "test-key-gen-ipns";


        // delete the key beforehand, just in case
        let client = reqwest::Client::new();
        let url = format!("{}/api/v0/key/rm", LOCAL_IPFS);
        let res = client
            .post(&url)
            .query(&[("arg", key_name)])
            .send()
            .await
            .expect("Failed to send key remove request");

        let result = generate_ipns_key(LOCAL_IPFS, key_name).await;
        let ipns_key = result.expect("Expected key generation to succeed");

        // Verify that it’s a valid CID wrapped in IpnsKey
        let base36 = ipns_key.to_string();
        assert!(
            base36.starts_with("k") || base36.starts_with("b"),
            "Expected IPNS key to be base36 or base32 CID, got: {}",
            base36
        );

        // if we do it again, it fails
        let result2 = generate_ipns_key(LOCAL_IPFS, key_name).await;
        assert!(result2.is_err());
    }
}
