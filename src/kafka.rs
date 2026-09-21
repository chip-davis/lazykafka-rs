use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(5);

use rdkafka::{
    ClientConfig,
    admin::AdminClient,
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer, DefaultConsumerContext},
};

pub struct KafkaConfig {
    config: ClientConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Partition {
    pub id: i32,
    pub leader: i32,
    pub low: i64,
    pub high: i64,
}

pub fn admin_client_config() -> KafkaConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", "localhost:9092");
    KafkaConfig { config }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Topic {
    pub name: String,
    pub partition_count: usize,
}

pub fn build_admin_client(
    cfg: &KafkaConfig,
) -> color_eyre::Result<AdminClient<DefaultClientContext>> {
    Ok(cfg.config.create()?)
}

pub fn build_consumer(
    cfg: &KafkaConfig,
) -> color_eyre::Result<BaseConsumer<DefaultConsumerContext>> {
    Ok(cfg.config.create()?)
}

pub async fn list_topics(consumer: &BaseConsumer) -> color_eyre::Result<Vec<Topic>> {
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(5))?;
    let mut topics: Vec<Topic> = metadata
        .topics()
        .iter()
        .map(|topic| Topic {
            name: topic.name().to_string(),
            partition_count: topic.partitions().len(),
        })
        .collect();
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(topics)
}

pub async fn list_partitions(
    consumer: &BaseConsumer,
    topic: &str,
) -> color_eyre::Result<Vec<Partition>> {
    let metadata = consumer.fetch_metadata(Some(topic), TIMEOUT)?;
    let topic_meta = metadata
        .topics()
        .first()
        .ok_or_else(|| color_eyre::eyre::eyre!("topic {topic} not found"))?;
    let mut partitions = vec![];
    for p in topic_meta.partitions() {
        let (low, high) = consumer.fetch_watermarks(topic, p.id(), TIMEOUT)?;
        partitions.push(Partition {
            id: p.id(),
            leader: p.leader(),
            low,
            high,
        });
    }
    partitions.sort_by_key(|p| p.id);
    Ok(partitions)
}
