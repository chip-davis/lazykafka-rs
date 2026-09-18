use std::time::Duration;

use rdkafka::{
    ClientConfig,
    admin::AdminClient,
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer, DefaultConsumerContext},
};

pub struct KafkaConfig {
    config: ClientConfig,
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
