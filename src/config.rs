use serde_yaml;

use rdkafka::ClientConfig;

use std::fs::File;
use std::{collections::HashMap, path::PathBuf};

fn map_to_client_config(config_map: &HashMap<String, String>) -> ClientConfig {
    config_map
        .iter()
        .fold(ClientConfig::new(), |mut config, (key, value)| {
            config.set(key, value);
            config
        })
}

fn or_expect<T: Clone>(first: &Option<T>, second: &Option<T>, name: &str) -> T {
    first
        .as_ref()
        .cloned()
        .or_else(|| second.clone())
        .expect(&format!("Missing configuration parameter: {}", name))
}

//
// ********** PRODUCER CONFIG **********
//

/// The on-file producer benchmark format.
#[derive(Debug, Serialize, Deserialize)]
struct ProducerBenchmarkFileConfig {
    default: ProducerScenarioFileConfig,
    scenarios: HashMap<String, ProducerScenarioFileConfig>,
}

impl ProducerBenchmarkFileConfig {
    fn from_file(path: &str) -> ProducerBenchmarkFileConfig {
        let input_file = File::open(path).expect("Failed to open configuration file");
        serde_yaml::from_reader(input_file).expect("Failed to parse configuration file")
    }
}

/// The on-file producer scenario benchmark format.
#[derive(Debug, Serialize, Deserialize)]
struct ProducerScenarioFileConfig {
    repeat_times: Option<u64>,
    repeat_pause: Option<u64>,
    threads: Option<u64>,
    producer_type: Option<ProducerType>,
    message_size: Option<u64>,
    message_count: Option<u64>,
    message_file: Option<PathBuf>,
    topic: Option<String>,
    producer_config: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProducerType {
    BaseProducer,
}

/// The producer scenario configuration used in benchmarks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerScenario {
    pub repeat_times: u64,
    pub repeat_pause: u64,
    pub threads: u64,
    pub producer_type: ProducerType,
    pub message_size: u64,
    pub message_count: u64,
    pub message_file: PathBuf,
    pub topic: String,
    pub producer_config: HashMap<String, String>,
}

impl ProducerScenario {
    fn from_file_config(
        default: &ProducerScenarioFileConfig,
        scenario: &ProducerScenarioFileConfig,
    ) -> ProducerScenario {
        let mut producer_config = default.producer_config.clone().unwrap_or_default();
        if let Some(ref config) = scenario.producer_config {
            for (key, value) in config {
                producer_config.insert(key.clone(), value.clone());
            }
        }
        if producer_config.is_empty() {
            panic!("No producer configuration provided")
        }
        ProducerScenario {
            repeat_times: or_expect(
                &scenario.repeat_times,
                &default.repeat_times,
                "repeat_times",
            ),
            repeat_pause: or_expect(
                &scenario.repeat_pause,
                &default.repeat_pause,
                "repeat_pause",
            ),
            threads: or_expect(&scenario.threads, &default.threads, "threads"),
            producer_type: or_expect(
                &scenario.producer_type,
                &default.producer_type,
                "producer_type",
            ),
            message_size: or_expect(
                &scenario.message_size,
                &default.message_size,
                "message_size",
            ),
            message_count: or_expect(
                &scenario.message_count,
                &default.message_count,
                "message_count",
            ),
            message_file: or_expect(
                &scenario.message_file,
                &default.message_file,
                "message_file",
            ),
            topic: or_expect(&scenario.topic, &default.topic, "topic"),
            producer_config,
        }
    }

    pub fn client_config(&self) -> ClientConfig {
        map_to_client_config(&self.producer_config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerBenchmark {
    pub scenarios: HashMap<String, ProducerScenario>,
}

impl ProducerBenchmark {
    pub fn from_file(path: &str) -> ProducerBenchmark {
        let raw_config = ProducerBenchmarkFileConfig::from_file(path);
        let defaults = raw_config.default;
        ProducerBenchmark {
            scenarios: raw_config
                .scenarios
                .into_iter()
                .map(|(name, scenario)| {
                    (
                        name,
                        ProducerScenario::from_file_config(&defaults, &scenario),
                    )
                })
                .collect(),
        }
    }
}
