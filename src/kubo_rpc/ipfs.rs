use cid::{Cid, multibase::Base};
use reqwest::Client;
use std::fmt;
use reqwest::multipart;
use std::str::FromStr;
use anyhow::{anyhow, Result};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IpfsCid(pub Cid);
impl From<IpfsCid> for Cid {
    fn from(key: IpfsCid) -> Self {
        key.0
    }
}

impl std::convert::TryFrom<Cid> for IpfsCid {
    type Error = &'static str;

    fn try_from(cid: Cid) -> Result<Self, Self::Error> {
        match cid.codec() {
            0x70 | 0x55 => Ok(IpfsCid(cid)), // dag-pb or raw
            _ => Err("Unsupported codec for IPFS CID"),
        }
    }
}

impl FromStr for IpfsCid {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cid = Cid::from_str(s).map_err(|_| "Invalid CID")?;
        IpfsCid::try_from(cid)
    }
}


impl std::fmt::Display for IpfsCid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.to_string_of_base(Base::Base58Btc) {
            Ok(encoded) => write!(f, "{}", encoded),
            Err(_) => Err(fmt::Error), // convert cid::Error into fmt::Error
        }
    }
}

#[cfg(test)]
mod ipfs_cid_test {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_valid_ipns_keys() {
        let inputs = vec![
            "QmdbWa3wBGwQ4suXjEpPkrigP3UmBMECdJNmkHfz6btqaJ",
            "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
        ];

        for cid in inputs {
            let parsed = IpfsCid::from_str(cid).expect("should parse");

            assert_eq!(parsed.to_string(), cid);

            // assert_eq!(parsed.0.codec(), 0x72, "Expected multicodec 0x72 for {}", label);
        }
    }

    #[test]
    fn test_invalid_ipns_keys_from_ipfs_cids() {
        let inputs = vec![
            "k51qzi5uqu5dgndmfpeorlwuar7u66p9g9l0dolwy2v7sm6dt5sorjityev4ib",
            "k51qzi5uqu5diyjoiyz6khv249l3puwbir19wiw1e3lehe4uw6g28pmtslcgqn",
        ];

        for key in inputs {
            let parsed = IpfsCid::from_str(key);
            assert!(
                parsed.is_err(),
                "Expected IPFS key '{}' to be rejected as an IPFS CID",
                key
            );
        }
    }

    #[test]
    fn test_invalid_format_string() {
        let bad_str = "not-a-cid";
        assert!(
            IpfsCid::from_str(bad_str).is_err(),
            "Expected '{}' to fail parsing as CID", bad_str
        );
    }
}


/// Fetches a block by CID from the IPFS daemon at `base_url`.
pub async fn get_block(
    base_url: &str,
    cid: &IpfsCid,
) -> Result<Vec<u8>> {
    let client = Client::builder()
        .read_timeout(Duration::from_secs(10))
        .build()?;


    let response = client
        .post(&format!("{}/api/v0/block/get", base_url))
        .query(&[("arg", cid.to_string())])
        .send()
        .await?;

    let bytes = response.bytes().await?;
    Ok(bytes.to_vec())
}

/// Puts a block of data into IPFS daemon at `base_url`.
pub async fn put_block(
    base_url: &str,
    data: &[u8],
) -> Result<IpfsCid> {
    let client = Client::new();

    let part = multipart::Part::bytes(data.to_vec()).file_name("block.data");
    let form = multipart::Form::new().part("data", part);

    let response = client
        .post(&format!("{}/api/v0/block/put", base_url))
        .multipart(form)
        .send()
        .await?;

    #[derive(serde::Deserialize)]
    #[allow(non_snake_case)]
    struct PutBlockResponse {
        Key: String,
    }

    let resp_json = response.json::<PutBlockResponse>().await?;
    let cid = IpfsCid::from_str(&resp_json.Key).map_err(|a| anyhow!(a))?;

    Ok(cid)
}


#[cfg(test)]
mod api_tests {
    use super::*;
    use std::str::FromStr;

    const LOCAL_IPFS: &str = "http://127.0.0.1:5001";

    #[tokio::test]
    async fn test_put_and_get_block() -> Result<()> {
        // Some arbitrary data
        let data = b"hello from rust integration test";

        // Put the block
        let cid = put_block(LOCAL_IPFS, data).await?;
        println!("Stored CID: {}", cid);

        // Get it back
        let retrieved = get_block(LOCAL_IPFS, &cid).await?;
        assert_eq!(retrieved.as_slice(), data);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_block_invalid_cid() {
        // This CID is fake / random
        let fake_cid = IpfsCid::from_str("QmYwAPJzv5CZsnAzt8auV2uYLZj1zWLf9khMoJjGB7pGeZ").unwrap();

        let result = get_block(LOCAL_IPFS, &fake_cid).await;
        assert!(
            result.is_err(),
            "Expected an error when retrieving a nonexistent CID"
        );
    }
}
