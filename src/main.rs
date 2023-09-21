use cid::Cid;
use fred::prelude::*;
use hex::ToHex;
use hex_literal::hex;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::VecDeque;
use std::env;
use std::fmt::{self, Display};
use tokio::time::sleep;
use web3::contract::{Contract, Options};
use web3::ethabi::decode;
use web3::futures::future::join_all;
use web3::futures::StreamExt;
use web3::transports::WebSocket;
use web3::types::{Bytes, Log, H256, U64};
use web3::{
    api::Namespace,
    types::{FilterBuilder, H160},
    Web3,
};

use eyre::{eyre, Result, WrapErr};
use serde::{Deserialize, Serialize};
const CONTENTHASH_CHANGED: H256 = H256(hex!(
    "e379c1624ed7e714cc0937528a32359d69d5281337765313dba4e081b72d7578"
));
const ENS_REGISTRY_ADDRESS: &str = "00000000000C2E074eC69A0dFb2997BA6C7d2e1e";

#[allow(dead_code)]
struct Config<T: web3::Transport> {
    pub ens: web3::contract::ens::Ens<T>,
    pub w3: Web3<T>,
    ens_registry_raw: Contract<T>,
    redis: RedisClient,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContentHashChangedLog {
    address: H160,
    node_hash: H256,
    data: Vec<u8>,
    block_number: U64,
    transaction_hash: H256,
    block_hash: H256,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
struct CODEC {
    name: String,
    version: u8,
}
impl CODEC {
    pub fn new(codec_header: [u8; 2]) -> Option<Self> {
        match codec_header {
            [229, v] => Some(Self {
                name: "ipns".into(),
                version: v,
            }),
            [227, v] => Some(Self {
                name: "ipfs".into(),
                version: v,
            }),

            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContentHash {
    as_of_block: U64,
    log: Option<ContentHashChangedLog>,
    value: Option<ContentHashTuple>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
struct ContentHashTuple {
    codec: CODEC,
    cid: String,
}

impl ContentHashTuple {
    fn new(codec: CODEC, cid: String) -> Self {
        Self { codec, cid }
    }
}

impl ContentHash {
    pub fn new(
        as_of_block: U64,
        log: Option<ContentHashChangedLog>,
        value: Option<ContentHashTuple>,
    ) -> Self {
        Self {
            as_of_block,
            log,
            value,
        }
    }
}

impl Display for ContentHashTuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}://{}", self.codec.name, self.cid)
    }
}

impl TryFrom<Log> for ContentHashChangedLog {
    type Error = eyre::Report;
    fn try_from(value: Log) -> Result<Self> {
        if value.topics.len() < 2 {
            return Err(eyre!("Topic length too short"));
        }
        if value.topics[0] != CONTENTHASH_CHANGED {
            return Err(eyre!("Bad topic for conversion"));
        }
        if value.is_removed() {
            return Err(eyre!("Refusing to encode removed block"));
        }

        let node_hash = value.topics[1];
        let address = value.address;
        let data_encoded = value.data.0;
        let data_decoded = decode(
            &[web3::ethabi::param_type::ParamType::Bytes],
            data_encoded.as_slice(),
        )?;

        let data = data_decoded[0]
            .clone()
            .into_bytes()
            .ok_or_else(|| eyre!("Couldn't decode ABI data packet"))?; //this should be safe, the eth filter + fixed params are enough to assert type safety
        let block_number = value
            .block_number
            .ok_or_else(|| eyre!("Refusing to encode log without block number"))?;

        let transaction_hash = value
            .transaction_hash
            .ok_or_else(|| eyre!("Refusing to encode log without transaction hash"))?;
        let block_hash = value
            .block_hash
            .ok_or_else(|| eyre!("Refusing to encode log without block hash"))?;

        Ok(Self {
            address,
            node_hash,
            data,
            block_number,
            transaction_hash,
            block_hash,
        })
    }
}

impl Config<web3::transports::WebSocket> {
    pub async fn new(rpc_address: &str, redis_address: &str) -> Result<Self> {
        let transport = web3::transports::WebSocket::new(rpc_address)
            .await
            .wrap_err("Failed to open web3 transport")?;

        let ens = web3::contract::ens::Ens::new(transport.clone());
        let eth = Web3::new(transport.clone()).eth();
        let ens_registry_raw_abi = include_bytes!("ABI/ENSRegistry.json");
        let ens_registry_raw = Contract::from_json(
            eth,
            ENS_REGISTRY_ADDRESS
                .parse()
                .wrap_err("Couldn't parse ENS registry address")?,
            ens_registry_raw_abi,
        )?;
        let redisconfig = RedisConfig::from_url(redis_address)?;
        let redisclient = RedisClient::new(redisconfig);
        let _ = redisclient.connect(Some(ReconnectPolicy::default()));
        let _ = redisclient.wait_for_connect().await?;

        Ok(Self {
            ens,
            w3: web3::Web3::new(transport.clone()),
            ens_registry_raw,
            redis: redisclient,
        })
    }
    pub async fn get_resolver_of_node(&self, node: H256, block_id: Option<U64>) -> Result<H160> {
        let options = Options::default();
        self.ens_registry_raw
            .query("resolver", node, None, options, block_id.map(|x| x.into()))
            .await
            .wrap_err("Query failed")
    }

    pub fn get_log_entry_key(&self, node_hash: H256) -> String {
        self.get_log_entry_key_str(node_hash.encode_hex::<String>())
    }

    pub fn get_log_entry_key_str(&self, entry: String) -> String {
        format!("hot_cache:node:{}:resolved", entry)
    }

    pub fn extract_nodehash_from_log_entry_key(&self, key: String) -> Result<String> {
        key.split(":")
            .collect::<Vec<&str>>()
            .get(2)
            .ok_or_else(|| eyre!("Malformed log entry key"))
            .map(|x| x.clone().into())
    }

    pub async fn process_log(&mut self, log: Result<Log>) -> Result<()> {
        let unwrapped_log = log?;
        println!("{:?}", unwrapped_log);
        let parsed_log: ContentHashChangedLog = unwrapped_log.clone().try_into()?;
        let block_number = parsed_log.block_number;
        let node_hash = parsed_log.node_hash;
        let cid = build_cid_from_log_data(
            parsed_log.data.clone(),
            block_number,
            Some(parsed_log.clone()),
        );
        if self
            .get_resolver_of_node(parsed_log.node_hash, Some(parsed_log.block_number))
            .await?
            != parsed_log.address
        {
            return Err(eyre!("Forged or erroneous log caught"));
        }
        self.process_cid_update(cid, node_hash).await
    }

    pub async fn process_cid_update(
        &mut self,
        new_entry: ContentHash,
        node_hash: H256,
    ) -> Result<()> {
        loop {
            let entry_key = self.get_log_entry_key(node_hash);
            println!("{:?} DECODED", new_entry);
            let old_entry = self
                .redis
                .get::<Option<String>, _>(entry_key.clone())
                .await?;
            let replace = old_entry.is_none()
                || serde_json::from_str::<ContentHash>(
                    old_entry
                        .expect("Unwrapping a Some should not fail")
                        .as_str(),
                )
                .map_or(true, |x| x.as_of_block <= new_entry.as_of_block);
            if replace {
                let _ = self.redis.watch(entry_key.clone()).await?;
                let trx = self.redis.multi(false).await?;
                trx.set::<RedisValue, String, String>(
                    entry_key,
                    serde_json::to_string(&new_entry)?,
                    None,
                    None,
                    false,
                )
                .await?;
                let res = trx.exec::<()>().await;
                match res {
                    Ok(_) => break,
                    Err(e) => {
                        if !e.is_canceled() {
                            return Err(e.into());
                        }
                    }
                }
            } else {
                break;
            };
        }

        println!("");
        Ok(())
    }
    #[allow(dead_code)]
    pub async fn find_contract_creation_block(&self, contract_address: H160) -> Result<U64> {
        let mut start: U64 = 0.into();
        let mut end = self.w3.eth().block_number().await?;
        let mut mid;
        let b = self
            .w3
            .eth()
            .code(contract_address, Some(end.into()))
            .await?;
        if b.0.len() == 0 {
            return Err(eyre!(
                "Address 0x{} is not a contract",
                contract_address.as_bytes().encode_hex::<String>()
            ));
        }

        loop {
            if end < start {
                panic!("Binary search underflow, this should not happen");
            }
            if start == end {
                break;
            }
            mid = (start + end) / 2;
            let b = self
                .w3
                .eth()
                .code(contract_address, Some(mid.into()))
                .await?;
            if b.0.len() == 0 {
                start = mid + 1;
            } else {
                end = mid;
            }
        }

        Ok(end)
    }
    async fn contenthash_fixup_scan_do_work(
        &mut self,
        node: String,
        block_number: U64,
        log: Option<ContentHashChangedLog>,
    ) -> Result<()> {
        let namehash: H256 = node.parse()?;
        let res = self
            .get_resolver_of_node(namehash, Some(block_number))
            .await?;
        let ct = self.get_ens_resolver_contract(res)?;
        let contenthash: Result<Bytes> = ct
            .query(
                "contenthash",
                namehash,
                None,
                Options::default(),
                Some(block_number.into()),
            )
            .await
            .map_err(|x| x.into());
        let contenthash = match contenthash {
            Ok(b) => build_cid_from_log_data(b.0, block_number, log),
            Err(_) => build_cid_from_log_data(vec![], block_number, log),
        };
        self.process_cid_update(contenthash, namehash).await?;

        Ok(())
    }
    pub async fn contenthash_fixup_scan(&mut self, block_number: U64) -> Result<()> {
        let keyspace = self.get_log_entry_key_str("*".into());
        let mut iter = self.redis.scan(keyspace, None, None);

        while let Some(Ok(mut page)) = iter.next().await {
            if let Some(keys) = page.take_results() {
                let client = page.create_client();
                for key_ in keys.into_iter() {
                    let res = client.get::<String, _>(key_.clone()).await;
                    if let Ok(key) = self.extract_nodehash_from_log_entry_key(
                        key_.clone().into_string().ok_or_else(|| {
                            eyre!("Redis keys should always be strings, but got {:?}", key_)
                        })?,
                    ) {
                        match res {
                            Ok(val) => {
                                let val_deserialized =
                                    serde_json::from_str::<ContentHash>(val.as_str());
                                let (rescan, old_log) = val_deserialized
                                    .map_or((true, None), |v| {
                                        ((v.as_of_block + 30 <= block_number), v.log)
                                    });
                                if rescan {
                                    println!("Rescanning for key {}", key.as_str());
                                    if let Err(e) = self
                                        .contenthash_fixup_scan_do_work(
                                            key.clone(),
                                            block_number,
                                            old_log,
                                        )
                                        .await
                                    {
                                        println!(
                                            "Error while running fixup on key {} {:?}",
                                            key, e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                println!("Error while scanning key {} {:?}", key.as_str(), e);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn get_ens_resolver_contract(
        &mut self,
        address: H160,
    ) -> Result<web3::contract::Contract<WebSocket>> {
        let abi = include_bytes!("ABI/ENSResolver.json");
        let eth = self.w3.eth();
        let ct = Contract::from_json(eth, address, abi)?;
        Ok(ct)
    }
}

async fn build_config() -> Result<Config<web3::transports::WebSocket>> {
    let w3_transport_url =
        env::var("ETH_RPC_WS").wrap_err("ETH_RPC_WS environment variable not found")?;
    let redis_transport_url =
        env::var("REDIS_URL").wrap_err("REDIS_URL environment variable not found")?;
    let config = Config::new(&w3_transport_url, &redis_transport_url).await?;

    Ok(config)
}

fn build_codec_tuple_from_log_data(data: Vec<u8>) -> Option<(CODEC, Cid)> {
    if data.len() == 0
        || !data
            .iter()
            .map(|x| x.clone() != 0) //ensure there is at least one non-zero byte
            .fold(false, |x, y| x || y)
    {
        return None;
    }
    let mut d: VecDeque<u8> = VecDeque::from(data.clone());
    let mut header: [u8; 2] = [0; 2];
    if d.len() < header.len() {
        return None;
    }
    for i in 0..header.len() {
        header[i] = d.pop_front().unwrap();
    }
    let mut cid: Vec<u8> = vec![];
    for i in d {
        cid.push(i);
    }

    Some((CODEC::new(header)?, Cid::try_from(cid.clone()).ok()?))
}

fn build_cid_from_log_data(
    data: Vec<u8>,
    as_of_block: U64,
    log: Option<ContentHashChangedLog>,
) -> ContentHash {
    let x = build_codec_tuple_from_log_data(data)
        .map(|(codec, cid)| ContentHashTuple::new(codec, cid.to_string()));

    ContentHash::new(as_of_block, log, x)
}

async fn consume_logs() -> Result<()> {
    let mut config = build_config().await?;
    let filter = FilterBuilder::default()
        .topics(Some(vec![CONTENTHASH_CHANGED]), None, None, None)
        .build();
    let mut sub = config
        .w3
        .eth_subscribe()
        .subscribe_logs(filter)
        .await
        .wrap_err("Couldn't subscribe")?;
    loop {
        let opt_log = sub.next().await;
        if let Some(log) = opt_log {
            if let Err(e) = config.process_log(log.map_err(|x| x.into())).await {
                println!("{:?}", e);
            }
        }
    }
}

async fn repoll_content_hashes() -> Result<()> {
    let mut config = build_config().await?;
    loop {
        let t = ThreadRng::default().gen_range(0..120);
        sleep(std::time::Duration::from_secs(t)).await;
        let block_number = config
            .w3
            .eth()
            .block_number()
            .await
            .clone()
            .context("Error getting block number")?;
        match config.contenthash_fixup_scan(block_number).await {
            Ok(_) => {}
            Err(e) => println!("Error when running contenthash_fixup_scan {:?}", e),
        }
        sleep(std::time::Duration::from_secs(60 * 10 as u64)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    //FIXME: https://github.com/tomusdrw/rust-web3/pull/643
    // we can't just pass config into these futures
    stable_eyre::install()?;
    let log_handle = tokio::spawn(consume_logs());
    let repoll_handle = tokio::spawn(repoll_content_hashes());
    let res = join_all([log_handle, repoll_handle]).await;
    for r in res {
        let err = match r.map_err(|x| x.into()) {
            Err(e) => Some(e),
            Ok(Err(e)) => Some(e),
            _ => None,
        };

        if let Some(e) = err {
            println!("{:?}", e);
        }
    }
    Ok(())
}
//TODO: periodically search from LATEST downward, that way we can get the logs of updates before we get previous logs
#[cfg(test)]
mod tests {
    use crate::build_config;
    use eyre::Result;
    use hex::ToHex;
    use web3::types::H256;
    #[tokio::test]
    async fn test_resolver_query() -> Result<()> {
        let config = build_config().await?;
        let res = config
            .get_resolver_of_node(
                "ee6c4522aab0003e8d14cd40a6af439055fd2577951148c14b6cea9a53475835".parse()?,
                Some(15973299.into()),
            )
            .await?;
        assert_eq!(res, "4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".parse()?);
        Ok(())
    }

    #[tokio::test]
    async fn test_contract_start_search() -> Result<()> {
        let config = build_config().await?;
        let search = config
            .find_contract_creation_block("4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41".parse()?)
            .await?;
        assert_eq!(search, 9412610.into());
        Ok(())
    }
    #[tokio::test]
    async fn test_key_homomorphism() -> Result<()> {
        let config = build_config().await?;
        let h = H256::random();
        let key = config.get_log_entry_key(h);
        let out = config.extract_nodehash_from_log_entry_key(key)?;
        assert_eq!(h.encode_hex::<String>(), out);
        Ok(())
    }
}
