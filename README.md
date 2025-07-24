# Kafka Wikimedia Compression Benchmark

This project demonstrates how different Kafka compression algorithms perform when handling real-time events from Wikimedia.

## Setup

This project requires five Kafka topics, each configured with a different compression strategy. You can create them using the following CLI commands:

> ⚠️ Make sure your Kafka broker is running on localhost:9094. Adjust the --bootstrap-server parameter if needed.

```bash
kafka-topics \
  --bootstrap-server localhost:9094 \
  --create \
  --topic wikimedia.recentchange \
  --replication-factor 1 \
  --partitions 3
```

```bash
kafka-topics \
  --bootstrap-server localhost:9094 \
  --create \
  --topic wikimedia.recentchange.gzip \
  --replication-factor 1 \
  --partitions 3
```

```bash
kafka-topics \
  --bootstrap-server localhost:9094 \
  --create \
  --topic wikimedia.recentchange.lz4 \
  --replication-factor 1 \
  --partitions 3
```

```bash
kafka-topics \
  --bootstrap-server localhost:9094 \
  --create \
  --topic wikimedia.recentchange.snappy \
  --replication-factor 1 \
  --partitions 3
```

```bash
kafka-topics \
  --bootstrap-server localhost:9094 \
  --create \
  --topic wikimedia.recentchange.zstd \
  --replication-factor 1 \
  --partitions 3
```

## Results

As shown in the figure below, `gzip` and `zstd` achieved better compression rates for the Wikimedia events. This comparison focuses solely on compression ratio. Although it does not account for CPU usage or latency overhead introduced by each algorithm, this benchmark provides a useful perspective on how compression algorithms behave under real-world streaming scenarios.

<img src="assets/result.png">