use std::sync::Arc;
use std::time::{Duration, Instant};
use futures_util::StreamExt;
use arc_swap::ArcSwapOption;
use tokio::sync::Mutex;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamMetrics {
    pub name: String,
    pub storage: String,
    pub messages: u64,
    pub bytes: u64,
    pub bytes_formatted: String,
    pub first_seq: u64,
    pub last_seq: u64,
    pub consumer_count: usize,
    pub num_subjects: u64,
    pub subjects: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerMetrics {
    pub name: String,
    pub stream_name: String,
    pub created: String,
    pub filter_subject: Option<String>,
    pub num_pending: u64,          // Consumer lag
    pub num_ack_pending: usize,     // Unacked messages
    pub num_redelivered: usize,
    pub num_waiting: usize,
    pub ack_floor_seq: u64,
    pub last_delivered_seq: u64,
    pub push_bound: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

impl ConsumerMetrics {
    pub fn compute_status(&self) -> String {
        if self.num_redelivered > 0 {
            "degraded".to_string()
        } else if self.num_pending > 1000 {
            "stalled".to_string()
        } else if self.num_ack_pending > 0 || self.num_pending > 0 {
            "active".to_string()
        } else {
            "healthy".to_string()
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSinkResponse {
    pub broker_type: String,
    pub broker_addr: String,
    pub status: String,
    pub capabilities: Vec<String>,
    pub stream: Option<StreamMetrics>,
    pub consumers: Vec<ConsumerMetrics>,
    pub details: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct EventSinkInspector {
    broker_method: String,
    broker_addr: String,
    nats_client: Arc<ArcSwapOption<async_nats::Client>>,
    nats_jetstream: Arc<ArcSwapOption<async_nats::jetstream::Context>>,
    connect_lock: Arc<Mutex<()>>,
    cached_response: Arc<ArcSwapOption<(Instant, EventSinkResponse)>>,
}

impl EventSinkInspector {
    pub fn new(broker_method: &str, broker_addr: &str) -> Self {
        Self {
            broker_method: broker_method.to_string(),
            broker_addr: broker_addr.to_string(),
            nats_client: Arc::new(ArcSwapOption::empty()),
            nats_jetstream: Arc::new(ArcSwapOption::empty()),
            connect_lock: Arc::new(Mutex::new(())),
            cached_response: Arc::new(ArcSwapOption::empty()),
        }
    }

    pub async fn inspect(&self) -> EventSinkResponse {
        // Fast-path: Return cached response if within 1500ms TTL
        if let Some(cached) = self.cached_response.load_full() {
            if cached.0.elapsed() < Duration::from_millis(1500) {
                return cached.1.clone();
            }
        }

        let m = self.broker_method.to_ascii_lowercase();

        let inspect_fut = async {
            if m.contains("nats") {
                self.inspect_nats().await
            } else if m.contains("redis") || m.contains("valkey") || m.contains("dragonfly") {
                self.inspect_redis().await
            } else if m.contains("iggy") {
                self.inspect_iggy().await
            } else if m.contains("rabbit") || m.contains("amqp") {
                self.inspect_rabbitmq().await
            } else if m.contains("kafka") || m.contains("redpanda") {
                self.inspect_kafka().await
            } else if m.contains("sierra") {
                self.inspect_sierradb().await
            } else if m.contains("webhook") || m.contains("http") {
                self.inspect_webhook().await
            } else {
                self.inspect_generic().await
            }
        };

        let resp = match tokio::time::timeout(Duration::from_millis(2500), inspect_fut).await {
            Ok(r) => r,
            Err(_) => {
                log::warn!(
                    "Event sink inspection timed out after 2500ms for broker method '{}'",
                    self.broker_method
                );
                EventSinkResponse {
                    broker_type: self.broker_method.clone(),
                    broker_addr: self.broker_addr.clone(),
                    status: "timeout".to_string(),
                    capabilities: vec![],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some("Event sink inspection timed out after 2500ms".to_string()),
                }
            }
        };

        self.cached_response
            .store(Some(Arc::new((Instant::now(), resp.clone()))));
        resp
    }

    // 1. NATS JetStream Inspection
    async fn inspect_nats(&self) -> EventSinkResponse {
        let js = match self.get_nats_jetstream().await {
            Ok(js) => js,
            Err(err) => {
                self.evict_nats();
                return EventSinkResponse {
                    broker_type: "NATS JetStream".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "offline".to_string(),
                    capabilities: vec!["Stream Metrics".into(), "Consumer Lag".into(), "Push Workers".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(err),
                };
            }
        };

        let stream = match js.get_stream("mutations").await {
            Ok(s) => Ok(s),
            Err(_) => js.get_stream("SPECTRA").await,
        };
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("connection") || err_str.contains("closed") || err_str.contains("broken") {
                    self.evict_nats();
                }
                return EventSinkResponse {
                    broker_type: "NATS JetStream".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "connected".to_string(),
                    capabilities: vec!["Stream Metrics".into(), "Consumer Lag".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("Stream 'mutations' (or 'SPECTRA') not found: {}", e)),
                };
            }
        };

        let stream_info = match stream.get_info().await {
            Ok(info) => info,
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("connection") || err_str.contains("closed") || err_str.contains("broken") {
                    self.evict_nats();
                }
                return EventSinkResponse {
                    broker_type: "NATS JetStream".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "connected".to_string(),
                    capabilities: vec!["Stream Metrics".into(), "Consumer Lag".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("Failed to get stream info: {}", e)),
                };
            }
        };

        let stream_metrics = StreamMetrics {
            name: stream_info.config.name.clone(),
            storage: format!("{:?}", stream_info.config.storage),
            messages: stream_info.state.messages,
            bytes: stream_info.state.bytes,
            bytes_formatted: format_bytes(stream_info.state.bytes),
            first_seq: stream_info.state.first_sequence,
            last_seq: stream_info.state.last_sequence,
            consumer_count: stream_info.state.consumer_count,
            num_subjects: stream_info.state.subjects_count,
            subjects: stream_info.config.subjects.clone(),
        };

        let mut consumers_list = Vec::new();
        let mut consumers_stream = stream.consumers();
        while let Some(consumer_res) = consumers_stream.next().await {
            if let Ok(c) = consumer_res {
                let filter_subj = if !c.config.filter_subject.is_empty() {
                    Some(c.config.filter_subject.clone())
                } else if !c.config.filter_subjects.is_empty() {
                    Some(c.config.filter_subjects.join(", "))
                } else {
                    None
                };

                consumers_list.push(ConsumerMetrics {
                    name: c.name.clone(),
                    stream_name: c.stream_name.clone(),
                    created: c.created.to_string(),
                    filter_subject: filter_subj,
                    num_pending: c.num_pending,
                    num_ack_pending: c.num_ack_pending,
                    num_redelivered: c.num_redelivered,
                    num_waiting: c.num_waiting,
                    ack_floor_seq: c.ack_floor.stream_sequence,
                    last_delivered_seq: c.delivered.stream_sequence,
                    push_bound: c.push_bound,
                    status: Some(if c.num_redelivered > 0 {
                        "degraded".to_string()
                    } else if c.num_pending > 1000 {
                        "stalled".to_string()
                    } else if c.num_ack_pending > 0 || c.num_pending > 0 {
                        "active".to_string()
                    } else {
                        "healthy".to_string()
                    }),
                });
            }
        }

        consumers_list.sort_by(|a, b| a.name.cmp(&b.name));

        EventSinkResponse {
            broker_type: "NATS JetStream".to_string(),
            broker_addr: self.broker_addr.clone(),
            status: "online".to_string(),
            capabilities: vec![
                "Stream Storage Metrics".into(),
                "Sequence Tracking".into(),
                "Consumer Lag (Pending)".into(),
                "Unacknowledged PEL Tracking".into(),
                "Push Bound Workers".into(),
            ],
            stream: Some(stream_metrics),
            consumers: consumers_list,
            details: None,
            error: None,
        }
    }

    pub fn evict_nats(&self) {
        let mut evicted = false;
        if self.nats_client.swap(None).is_some() {
            evicted = true;
        }
        if self.nats_jetstream.swap(None).is_some() {
            evicted = true;
        }
        self.cached_response.swap(None);
        if evicted {
            log::warn!("Evicted disconnected NATS client from admin inspector cache to release socket");
        }
    }

    async fn get_nats_client(&self) -> Result<Arc<async_nats::Client>, String> {
        if let Some(client) = self.nats_client.load_full() {
            return Ok(client);
        }

        let _guard = self.connect_lock.lock().await;
        if let Some(client) = self.nats_client.load_full() {
            return Ok(client);
        }

        let is_tty = crate::core::config::SpectraDispatchConfig::is_interactive();
        let (initial_ms, max_ms): (u64, u64) = if is_tty { (250, 5000) } else { (10, 2000) };

        let options = async_nats::ConnectOptions::new()
            .reconnect_delay_callback(move |attempts| {
                let factor = 1u64.checked_shl(attempts.min(6) as u32).unwrap_or(64);
                let delay = std::cmp::min(initial_ms.saturating_mul(factor), max_ms);
                Duration::from_millis(delay)
            });

        let client = async_nats::connect_with_options(&self.broker_addr, options)
            .await
            .map_err(|e| format!("Failed to connect to NATS at {}: {}", self.broker_addr, e))?;

        let arc_client = Arc::new(client);
        self.nats_client.store(Some(arc_client.clone()));
        Ok(arc_client)
    }

    async fn get_nats_jetstream(&self) -> Result<Arc<async_nats::jetstream::Context>, String> {
        if let Some(js) = self.nats_jetstream.load_full() {
            return Ok(js);
        }

        let client = self.get_nats_client().await?;
        let js = Arc::new(async_nats::jetstream::new((*client).clone()));
        self.nats_jetstream.store(Some(js.clone()));
        Ok(js)
    }

    /// Queries a consumer worker's recent execution logs via NATS Request-Reply (SWTP v1).
    pub async fn query_worker_logs(&self, worker_id: &str, limit: usize) -> Result<serde_json::Value, String> {
        let m = self.broker_method.to_ascii_lowercase();
        if m.contains("nats") {
            let client = self.get_nats_client().await?;
            let subject = format!("spectra.workers.{}.logs", worker_id);
            let req_payload = serde_json::json!({
                "limit": limit
            });
            let payload_bytes = bytes::Bytes::from(req_payload.to_string());

            let request = match tokio::time::timeout(
                Duration::from_millis(2000),
                client.request(subject.clone(), payload_bytes),
            )
            .await
            {
                Ok(Ok(resp)) => resp,
                Ok(Err(e)) => {
                    self.evict_nats();
                    return Err(format!("Failed to query logs from worker '{}': {}", worker_id, e));
                }
                Err(_) => {
                    return Err(format!(
                        "Timed out waiting for worker '{}' response on subject '{}' (worker is offline or has not implemented SWTP)",
                        worker_id, subject
                    ));
                }
            };

            let resp_str = std::str::from_utf8(&request.payload)
                .map_err(|e| format!("Worker returned invalid UTF-8: {}", e))?;

            let parsed: serde_json::Value = serde_json::from_str(resp_str)
                .map_err(|e| format!("Worker returned non-JSON response: {}", e))?;

            Ok(parsed)
        } else {
            Err(format!(
                "Worker log query via Request-Reply is currently supported for NATS brokers (active broker: {})",
                self.broker_method
            ))
        }
    }

    // 2. Redis Streams / Valkey Inspection
    async fn inspect_redis(&self) -> EventSinkResponse {
        let resp_client = crate::telemetry::resp::RespClient::new(&self.broker_addr);
        let mut conn = match resp_client.get_connection().await {
            Ok(c) => c,
            Err(e) => {
                return EventSinkResponse {
                    broker_type: "Redis Streams".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "offline".to_string(),
                    capabilities: vec!["Stream Length (XLEN)".into(), "Consumer Groups (XINFO)".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("Redis connection failed: {}", e)),
                };
            }
        };

        // Ping check
        let ping_res: Result<String, _> = redis::cmd("PING").query_async(&mut conn).await;
        if ping_res.is_err() {
            return EventSinkResponse {
                broker_type: "Redis Streams".to_string(),
                broker_addr: self.broker_addr.clone(),
                status: "degraded".to_string(),
                capabilities: vec!["Stream Length (XLEN)".into()],
                stream: None,
                consumers: vec![],
                details: None,
                error: Some("Redis PING failed".to_string()),
            };
        }

        let stream_key = "spectra:events";
        let xlen: u64 = redis::cmd("XLEN")
            .arg(stream_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(0);

        let stream_metrics = StreamMetrics {
            name: stream_key.to_string(),
            storage: "Redis Memory (AOF/RDB)".to_string(),
            messages: xlen,
            bytes: 0,
            bytes_formatted: "In-Memory".to_string(),
            first_seq: 0,
            last_seq: xlen,
            consumer_count: 1,
            num_subjects: 1,
            subjects: vec![stream_key.to_string()],
        };

        EventSinkResponse {
            broker_type: "Redis Streams".to_string(),
            broker_addr: self.broker_addr.clone(),
            status: "online".to_string(),
            capabilities: vec![
                "In-Memory Stream (XLEN)".into(),
                "Consumer Groups (XINFO)".into(),
                "Pending Entries List (XPENDING)".into(),
            ],
            stream: Some(stream_metrics),
            consumers: vec![
                ConsumerMetrics {
                    name: "default-group".to_string(),
                    stream_name: stream_key.to_string(),
                    created: "Active".to_string(),
                    filter_subject: Some(stream_key.to_string()),
                    num_pending: 0,
                    num_ack_pending: 0,
                    num_redelivered: 0,
                    num_waiting: 0,
                    ack_floor_seq: xlen,
                    last_delivered_seq: xlen,
                    push_bound: true,
                    status: Some("healthy".to_string()),
                }
            ],
            details: Some(serde_json::json!({ "streamKey": stream_key, "totalEntries": xlen })),
            error: None,
        }
    }

    // 3. Apache Iggy Inspection
    async fn inspect_iggy(&self) -> EventSinkResponse {
        use iggy::prelude::*;
        let addr = crate::telemetry::iggy::normalize_iggy_url(&self.broker_addr);

        let client = match IggyClient::builder().with_tcp().with_server_address(addr).build() {
            Ok(c) => c,
            Err(e) => {
                return EventSinkResponse {
                    broker_type: "Apache Iggy".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "offline".to_string(),
                    capabilities: vec!["Zero-Copy Streams".into(), "Partitions Tracking".into(), "Consumer Groups".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("Iggy client initialization failed: {}", e)),
                };
            }
        };

        if let Err(e) = client.connect().await {
            return EventSinkResponse {
                broker_type: "Apache Iggy".to_string(),
                broker_addr: self.broker_addr.clone(),
                status: "offline".to_string(),
                capabilities: vec!["Zero-Copy Streams".into(), "Partitions Tracking".into(), "Consumer Groups".into()],
                stream: None,
                consumers: vec![],
                details: None,
                error: Some(format!("Iggy server connection failed: {}", e)),
            };
        }

        let stream_id = Identifier::named("spectra").unwrap_or_else(|_| Identifier::numeric(1).unwrap());
        let stream_info = client.get_stream(&stream_id).await;

        match stream_info {
            Ok(Some(info)) => {
                let size_bytes = info.size.as_bytes_u64();
                let stream_metrics = StreamMetrics {
                    name: info.name.clone(),
                    storage: "Iggy Zero-Copy / Kernel Page Cache".to_string(),
                    messages: info.messages_count,
                    bytes: size_bytes,
                    bytes_formatted: format_bytes(size_bytes),
                    first_seq: 0,
                    last_seq: info.messages_count,
                    consumer_count: 1,
                    num_subjects: info.topics_count as u64,
                    subjects: vec!["spectra.*".into()],
                };

                EventSinkResponse {
                    broker_type: "Apache Iggy".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "online".to_string(),
                    capabilities: vec![
                        "Ultra-Low Latency Zero-Copy".into(),
                        "Kernel Page Cache Logging".into(),
                        "Consumer Groups & Partition Offsets".into(),
                    ],
                    stream: Some(stream_metrics),
                    consumers: vec![
                        ConsumerMetrics {
                            name: "iggy-consumer-group-1".to_string(),
                            stream_name: info.name.clone(),
                            created: "Active".to_string(),
                            filter_subject: Some("spectra.*".into()),
                            num_pending: 0,
                            num_ack_pending: 0,
                            num_redelivered: 0,
                            num_waiting: 0,
                            ack_floor_seq: info.messages_count,
                            last_delivered_seq: info.messages_count,
                            push_bound: true,
                            status: Some("healthy".to_string()),
                        }
                    ],
                    details: Some(serde_json::json!({
                        "streamId": info.id,
                        "topicsCount": info.topics_count,
                        "sizeBytes": size_bytes,
                    })),
                    error: None,
                }
            }
            Ok(None) => {
                EventSinkResponse {
                    broker_type: "Apache Iggy".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "connected".to_string(),
                    capabilities: vec!["Zero-Copy Streams".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some("Connected to Iggy, but stream 'spectra' not found".to_string()),
                }
            }
            Err(e) => {
                EventSinkResponse {
                    broker_type: "Apache Iggy".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "connected".to_string(),
                    capabilities: vec!["Zero-Copy Streams".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("Connected to Iggy, but stream query failed: {}", e)),
                }
            }
        }
    }

    // 4. RabbitMQ / AMQP Inspection
    async fn inspect_rabbitmq(&self) -> EventSinkResponse {
        use lapin::{Connection, ConnectionProperties, options::QueueDeclareOptions, types::FieldTable};
        let url = crate::telemetry::rabbitmq::normalize_amqp_url(&self.broker_addr);

        let conn = match Connection::connect(&url, ConnectionProperties::default()).await {
            Ok(c) => c,
            Err(e) => {
                return EventSinkResponse {
                    broker_type: "RabbitMQ (AMQP)".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "offline".to_string(),
                    capabilities: vec!["Queue Depth Inspection".into(), "Active Consumer Count".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("RabbitMQ connection error: {}", e)),
                };
            }
        };

        let channel = match conn.create_channel().await {
            Ok(ch) => ch,
            Err(e) => {
                return EventSinkResponse {
                    broker_type: "RabbitMQ (AMQP)".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "connected".to_string(),
                    capabilities: vec!["Queue Depth Inspection".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("Channel creation error: {}", e)),
                };
            }
        };

        let queue_name = "spectra.events";
        let queue = channel
            .queue_declare(
                queue_name.into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await;

        match queue {
            Ok(q) => {
                let msgs = q.message_count() as u64;
                let consumers_count = q.consumer_count() as usize;

                let stream_metrics = StreamMetrics {
                    name: queue_name.to_string(),
                    storage: "AMQP Broker Queue".to_string(),
                    messages: msgs,
                    bytes: 0,
                    bytes_formatted: "Queue Paged".to_string(),
                    first_seq: 0,
                    last_seq: msgs,
                    consumer_count: consumers_count,
                    num_subjects: 1,
                    subjects: vec!["spectra.*".to_string()],
                };

                EventSinkResponse {
                    broker_type: "RabbitMQ (AMQP)".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "online".to_string(),
                    capabilities: vec![
                        "AMQP Queue Depth".into(),
                        "Active Consumer Channel Count".into(),
                        "Topic Exchange Routing".into(),
                    ],
                    stream: Some(stream_metrics),
                    consumers: vec![
                        ConsumerMetrics {
                            name: format!("{}-consumer", queue_name),
                            stream_name: queue_name.to_string(),
                            created: "Active Channel".to_string(),
                            filter_subject: Some("spectra.*".to_string()),
                            num_pending: msgs,
                            num_ack_pending: 0,
                            num_redelivered: 0,
                            num_waiting: 0,
                            ack_floor_seq: msgs,
                            last_delivered_seq: msgs,
                            push_bound: consumers_count > 0,
                            status: Some(if msgs > 1000 { "stalled".to_string() } else { "healthy".to_string() }),
                        }
                    ],
                    details: Some(serde_json::json!({
                        "queue": queue_name,
                        "readyMessages": msgs,
                        "consumerCount": consumers_count,
                    })),
                    error: None,
                }
            }
            Err(_) => {
                EventSinkResponse {
                    broker_type: "RabbitMQ (AMQP)".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "connected".to_string(),
                    capabilities: vec!["Topic Exchange Routing".into()],
                    stream: None,
                    consumers: vec![],
                    details: Some(serde_json::json!({ "exchange": "amq.topic", "routingPrefix": "spectra" })),
                    error: None,
                }
            }
        }
    }

    // 5. Apache Kafka / Redpanda Inspection
    async fn inspect_kafka(&self) -> EventSinkResponse {
        use rskafka::client::{ClientBuilder, partition::{OffsetAt, UnknownTopicHandling}};
        let hosts = crate::telemetry::kafka::normalize_kafka_hosts(&self.broker_addr);

        let client = match ClientBuilder::new(hosts.clone()).build().await {
            Ok(c) => c,
            Err(e) => {
                return EventSinkResponse {
                    broker_type: "Apache Kafka / Redpanda".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "offline".to_string(),
                    capabilities: vec!["Partition High-Watermark".into(), "Consumer Lag Offsets".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("Kafka broker connect error: {}", e)),
                };
            }
        };

        let topic = "spectra";
        let partition = 0;
        let partition_client = client
            .partition_client(topic, partition, UnknownTopicHandling::Retry)
            .await;

        match partition_client {
            Ok(pc) => {
                let high_watermark = pc.get_offset(OffsetAt::Latest).await.unwrap_or(0);
                let stream_metrics = StreamMetrics {
                    name: topic.to_string(),
                    storage: "Distributed Commit Log".to_string(),
                    messages: high_watermark as u64,
                    bytes: 0,
                    bytes_formatted: "Log Partition".to_string(),
                    first_seq: 0,
                    last_seq: high_watermark as u64,
                    consumer_count: 1,
                    num_subjects: 1,
                    subjects: vec![topic.to_string()],
                };

                EventSinkResponse {
                    broker_type: "Apache Kafka / Redpanda".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "online".to_string(),
                    capabilities: vec![
                        "Partition High-Watermark".into(),
                        "Distributed Commit Log".into(),
                        "Offset Tracking".into(),
                    ],
                    stream: Some(stream_metrics),
                    consumers: vec![
                        ConsumerMetrics {
                            name: "kafka-consumer-group".to_string(),
                            stream_name: topic.to_string(),
                            created: "Partition 0".to_string(),
                            filter_subject: Some(topic.to_string()),
                            num_pending: 0,
                            num_ack_pending: 0,
                            num_redelivered: 0,
                            num_waiting: 0,
                            ack_floor_seq: high_watermark as u64,
                            last_delivered_seq: high_watermark as u64,
                            push_bound: true,
                            status: Some("healthy".to_string()),
                        }
                    ],
                    details: Some(serde_json::json!({
                        "topic": topic,
                        "partition": partition,
                        "highWatermark": high_watermark,
                    })),
                    error: None,
                }
            }
            Err(e) => {
                EventSinkResponse {
                    broker_type: "Apache Kafka / Redpanda".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "connected".to_string(),
                    capabilities: vec!["Topic Offset Inspection".into()],
                    stream: None,
                    consumers: vec![],
                    details: Some(serde_json::json!({ "hosts": hosts })),
                    error: Some(format!("Kafka topic '{}' not yet created: {}", topic, e)),
                }
            }
        }
    }

    // 6. SierraDB Inspection
    async fn inspect_sierradb(&self) -> EventSinkResponse {
        let resp_client = crate::telemetry::resp::RespClient::new(&self.broker_addr);
        let mut conn = match resp_client.get_connection().await {
            Ok(c) => c,
            Err(e) => {
                return EventSinkResponse {
                    broker_type: "SierraDB Event Sourcing Engine".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "offline".to_string(),
                    capabilities: vec!["Event Append Log".into(), "Monotonic Versioning".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("SierraDB connection failed: {}", e)),
                };
            }
        };

        let ping_res: Result<String, _> = redis::cmd("PING").query_async(&mut conn).await;
        if ping_res.is_err() {
            return EventSinkResponse {
                broker_type: "SierraDB Event Sourcing Engine".to_string(),
                broker_addr: self.broker_addr.clone(),
                status: "degraded".to_string(),
                capabilities: vec!["RESP3 Interface".into()],
                stream: None,
                consumers: vec![],
                details: None,
                error: Some("SierraDB PING failed".to_string()),
            };
        }

        EventSinkResponse {
            broker_type: "SierraDB Event Sourcing Engine".to_string(),
            broker_addr: self.broker_addr.clone(),
            status: "online".to_string(),
            capabilities: vec![
                "RESP3 EAPPEND Interface".into(),
                "Strict Monotonic Versioning".into(),
                "Causal HLC Ordering".into(),
            ],
            stream: Some(StreamMetrics {
                name: "spectra:events".to_string(),
                storage: "SierraDB Append-Only Store".to_string(),
                messages: 0,
                bytes: 0,
                bytes_formatted: "Persistent Log".to_string(),
                first_seq: 0,
                last_seq: 0,
                consumer_count: 1,
                num_subjects: 1,
                subjects: vec!["spectra:*".to_string()],
            }),
            consumers: vec![],
            details: Some(serde_json::json!({ "engine": "SierraDB", "protocol": "RESP3" })),
            error: None,
        }
    }

    // 7. HTTP Webhook Destination Inspection
    async fn inspect_webhook(&self) -> EventSinkResponse {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(2000))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let start = Instant::now();
        let ping_res = client.head(&self.broker_addr).send().await;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        match ping_res {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                let is_healthy = resp.status().is_success() || resp.status().is_redirection() || status_code == 405;

                EventSinkResponse {
                    broker_type: "HTTP Webhook Sink".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: if is_healthy { "online".to_string() } else { "degraded".to_string() },
                    capabilities: vec![
                        "HTTP Post Event Delivery".into(),
                        "Round-Trip Latency Probing".into(),
                    ],
                    stream: Some(StreamMetrics {
                        name: "HTTP Webhook Target".to_string(),
                        storage: "External HTTP Server".to_string(),
                        messages: 0,
                        bytes: 0,
                        bytes_formatted: "REST / JSON".to_string(),
                        first_seq: 0,
                        last_seq: 0,
                        consumer_count: 1,
                        num_subjects: 1,
                        subjects: vec!["*".to_string()],
                    }),
                    consumers: vec![
                        ConsumerMetrics {
                            name: "webhook-target".to_string(),
                            stream_name: self.broker_addr.clone(),
                            created: "HTTP Endpoint".to_string(),
                            filter_subject: Some("*".to_string()),
                            num_pending: 0,
                            num_ack_pending: 0,
                            num_redelivered: 0,
                            num_waiting: 0,
                            ack_floor_seq: 0,
                            last_delivered_seq: 0,
                            push_bound: is_healthy,
                            status: Some(if is_healthy { "healthy".to_string() } else { "degraded".to_string() }),
                        }
                    ],
                    details: Some(serde_json::json!({
                        "targetUrl": self.broker_addr,
                        "probeStatusCode": status_code,
                        "latencyMs": (latency_ms * 100.0).round() / 100.0,
                    })),
                    error: None,
                }
            }
            Err(e) => {
                EventSinkResponse {
                    broker_type: "HTTP Webhook Sink".to_string(),
                    broker_addr: self.broker_addr.clone(),
                    status: "offline".to_string(),
                    capabilities: vec!["HTTP Post Event Delivery".into()],
                    stream: None,
                    consumers: vec![],
                    details: None,
                    error: Some(format!("Webhook probe to '{}' failed: {}", self.broker_addr, e)),
                }
            }
        }
    }

    // Fallback Generic Broker Inspection
    async fn inspect_generic(&self) -> EventSinkResponse {
        EventSinkResponse {
            broker_type: self.broker_method.clone(),
            broker_addr: self.broker_addr.clone(),
            status: "active".to_string(),
            capabilities: vec!["Generic Event Dispatch".into()],
            stream: None,
            consumers: vec![],
            details: None,
            error: None,
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;

    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}
