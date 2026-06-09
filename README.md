# streaming-05-storage

[![API Reference](https://img.shields.io/badge/API--Utils-datafun--streaming-purple)](https://denisecase.github.io/datafun-streaming/api/)
[![Workflow Guide](https://img.shields.io/badge/Pro--Guide-pro--analytics--02-green)](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
[![Python 3.14](https://img.shields.io/badge/python-3.14%2B-blue?logo=python)](./pyproject.toml)
[![MIT](https://img.shields.io/badge/license-see%20LICENSE-yellow.svg)](./LICENSE)

> Streaming data analytics: store processed messages.

Streaming analytics requires working with data in motion
and distributed, scalable systems.
This course builds capabilities through working projects.
In the age of generative AI, durable skills are grounded in real work:
setting up a professional environment,
reading and running code,
understanding the logic,
and pushing work to a shared repository.
Each project follows the structure of professional Python projects.
We learn by doing.

## This Project

This project focuses on storing streaming data after it is consumed.

The project uses Kafka to move sales messages from a producer to a consumer.
The consumer reads each message, validates required fields, computes derived values,
writes processed records to CSV, and stores results in DuckDB.

This module adds persistent storage to the streaming workflow.

The goal is to see how consumed messages can be saved for later inspection,
querying, and analysis.

## Working Files

You'll work with just these areas:

- **data/** - input data and generated output files
- **docs/** - the project narrative and documentation
- **src/streaming/** - producer, consumer, and supporting code
- **pyproject.toml** - update authorship & links
- **zensical.toml** - update authorship & links

## Instructions

Follow the
[step-by-step workflow guide](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to complete:

1. Phase 1. **Start & Run**
2. Phase 2. **Change Authorship**
3. Phase 3. **Read & Understand**
4. Phase 4. **Modify**
5. Phase 5. **Apply**

## Challenges

Challenges are expected.
Sometimes instructions may not quite match your operating system.
When issues occur, share screenshots, error messages, and details about what you tried.
Working through issues is part of implementing professional projects.

## Success

After completing Phase 1. **Start & Run**, you'll have your own GitHub project
running with Kafka.

Use four named terminals:

1. **kafka** - keep the Kafka message broker running
2. **topics** - create, list, or reset Kafka topics
3. **producer** - run the project and producer
4. **consumer** - run the consumer

After the producer and consumer run successfully, you should see:

```shell
========================
Consumer executed successfully!
========================
```

A new file `project.log` will appear in the root project folder
and processed data will appear in data/output/.

## Command Reference

The commands below are used in the workflow guide above.
They are provided here for convenience.

**Important:** the first few times you run a project,
follow the guide with the **complete instructions**.

<details>
<summary>Show command reference</summary>

### In a machine terminal (open in your `Repos` folder)

After you get a copy of this repo in your own GitHub account,
open a machine terminal in your `Repos` folder:

```bash
# Replace username with YOUR GitHub username.
git clone https://github.com/username/streaming-05-storage

cd streaming-05-storage
code .
```

### In VS Code Terminal 1: Start Kafka (kafka)

For full instructions see
[**start kafka**](https://denisecase.github.io/pro-analytics-02/kafka/start-kafka/).

If any command fails,
repeat the steps at
[**install kafka**](https://denisecase.github.io/pro-analytics-02/kafka/install-kafka/)
until starting up is reliable.

Open a new VS Code terminal. Rename it `kafka`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

Step 1. Verify Java and PATH

```bash
echo "$JAVA_HOME"

"$JAVA_HOME/bin/java" --version
```

Step 2. Rebuild ClusterID (as needed)

```bash
cd ~/kafka

rm -rf /tmp/kraft-combined-logs

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

echo "Cluster ID: $KAFKA_CLUSTER_ID"

bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

Step 3. Start kafka server (keep running)

```bash
cd ~/kafka

bin/kafka-server-start.sh config/server.properties
```

### In VS Code terminal 2: Create Topic (topics)

For full instructions see
[**create topic**](https://denisecase.github.io/pro-analytics-02/kafka/create-topic/).

The topic name must match the name defined in your
`.env` file (copy `.env.example` to `.env`).

Open another VS Code terminal. Rename it `topics`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic streaming-05-storage-case
```

### In VS Code Terminal 3: Run Project and Producer (producer)

Open another VS Code terminal. Rename it `producer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.

```shell
# reset uv cache only if/when you start getting strange dependency errors
# uv cache clean

uv self update
uv python pin 3.14
uv sync --extra dev --extra docs --upgrade

uvx pre-commit install

git add -A
uvx pre-commit run --all-files
# repeat if changes were made
git add -A
uvx pre-commit run --all-files

# run the producer
clear
uv run python -m streaming.kafka_producer_case

# do chores
uv run ruff format .
uv run ruff check . --fix
uv run python -m pyright
uv run python -m pytest
uv run python -m zensical build

# save progress
git add -A
git commit -m "update"
git push -u origin main
```

### In VS Code Terminal 4: Run Consumer (consumer)

Open another VS Code terminal. Rename it `consumer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.
Clear the terminal, then start the consumer.

```shell
clear
uv run python -m streaming.kafka_consumer_case
```

To start fresh, see
[manage topics](https://denisecase.github.io/pro-analytics-02/kafka/manage-topics/)
to delete the topic and recreate it.

</details>

## Notes

- Use the **UP ARROW** and **DOWN ARROW** in the terminal to scroll through past commands.
- Use `CTRL+f` to find (and replace) text within a file.
- You do not need to add to or modify `tests/`. They are provided for example only.
- Many files are silent helpers. Explore as you like, but nothing is required.
- You do NOT not to understand everything; understanding builds naturally over time.

## Troubleshooting >>> or

If you see something like this in your terminal: `>>>` or `...`
You accidentally started Python interactive mode.
It happens.
Press `Ctrl+c` (both keys together) or `Ctrl+Z` then `Enter` on Windows.

## Example Producer Output

The example producer output is unchanged from previous projects.

## Example Consumer Output

Look for the text `db`:

```text
| C05 | ========================
| C05 | START consumer main()
| C05 | ========================
| C05 | ROOT_DIR = .
| C05 | DATA_DIR = data
| C05 | OUTPUT_CSV = data\output\consumed_sales.csv
| C05 | OUTPUT_DB = data\output\sales.duckdb
| C05 | REGIONS_CSV = data\regions.csv
| C05 | PRODUCTS_CSV = data\products.csv
| C05 | CURRENCIES_CSV = data\currencies.csv
| C05 | DISCOUNT_CODES_CSV = data\discount_codes.csv
| C05 | ========================
| C05 | SECTION A. Acquire
| C05 | ========================
| C05 | Loading settings from .env...
| C05 | KAFKA_BOOTSTRAP_SERVERS  = localhost:9092
| C05 | KAFKA_TOPIC              = streaming-05-storage-case
| C05 | KAFKA_GROUP_ID           = streaming-consumer-group-A
| C05 | CONSUMER_TIMEOUT_SECONDS = 10.0
| C05 | CONSUMER_MAX_MESSAGES    = 1000
| C05 | Verifying Kafka connection...
| C05 | Kafka port is reachable.
| C05 | Verifying Kafka topic...
%3|1778437824.601|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2040ms in state CONNECT)
%3|1778437826.740|FAIL|rdkafka#producer-1| [thrd:localhost:9092/1]: localhost:9092/1: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2037ms in state CONNECT)
| C05 | Topic 'streaming-05-storage-case' exists.
| C05 | Found 3 message(s) available.
| C05 | Creating Kafka consumer...
| C05 | Subscribed to topic: 'streaming-05-storage-case' (reading from beginning)
| C05 | ========================
| C05 | SECTION C. Consume and Process Messages
| C05 | ========================
| C05 | Initializing output...
| C05 | Output CSV cleared: consumed_sales.csv
| C05 | Database initialized: sales.duckdb
| C05 | Loading enrichment reference data...
| C05 | Found 6 region tax rates.
| C05 | Consuming messages...
| C05 | Waiting for up to 1000 message(s).
| C05 | Press CTRL+C to stop early.

| C05 | {'currency_code': 'USD', 'customer_id': 'CUST-4150', 'customer_note': 'Gift for my team', 'datetime': '2026-05-04T08:11:00Z', 'device_type': 'tablet', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id': 'e7324981-a9f0-419f-b708-d0a333451fff', 'payment_method': 'paypal', 'product_id': 'PY-STREAM-005', 'quantity': '3', 'referral_source': 'paid_search', 'region_id': 'US-TX', 'unit_price': '59.99', '_kafka_key': 'US-TX', '_kafka_partition': 0, '_kafka_offset': 0}
| C05 | subtotal=179.97
| C05 | tax=14.85
| C05 | total=194.82
| C05 | running_total=194.82
| C05 | Wrote valid record to DuckDB:
| C05 |   order=e7324981-a9f0-419f-b708-d0a333451fff
| C05 | MESSAGE ACCEPTED
| C05 | order=e7324981-a9f0-419f-b708-d0a333451fff
| C05 | total=$194.82
| C05 | consumed=1
| C05 | RUNNING STATS
| C05 | total_sales=$194.82
| C05 | average=$194.82
| C05 | min=$194.82
| C05 | max=$194.82
| C05 | {'currency_code': 'USD', 'customer_id': 'CUST-1106', 'customer_note': 'Gift for my team', 'datetime': '2026-05-04T08:23:00Z', 'device_type': 'mobile', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id': 'd61943e0-f543-4b5f-9c9a-18605ea4cfe5', 'payment_method': 'paypal', 'product_id': 'PY-DATA-002', 'quantity': '1', 'referral_source': 'paid_search', 'region_id': 'US-TX', 'unit_price': '49.99', '_kafka_key': 'US-TX', '_kafka_partition': 0, '_kafka_offset': 1}
| C05 | subtotal=49.99
| C05 | tax=4.12
| C05 | total=54.11
| C05 | running_total=248.93
| C05 | Wrote valid record to DuckDB:
| C05 |   order=d61943e0-f543-4b5f-9c9a-18605ea4cfe5
| C05 | MESSAGE ACCEPTED
| C05 | order=d61943e0-f543-4b5f-9c9a-18605ea4cfe5
| C05 | total=$54.11
| C05 | consumed=2
| C05 | RUNNING STATS
| C05 | total_sales=$248.93
| C05 | average=$124.47
| C05 | min=$54.11
| C05 | max=$194.82
| C05 | {'currency_code': 'CAD', 'customer_id': 'CUST-2133', 'customer_note': 'Learning at my own pace', 'datetime': '2026-05-04T08:28:00Z', 'device_type': 'desktop', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id': '14da1915-8e74-47be-9e10-f7275d31af46', 'payment_method': 'paypal', 'product_id': 'PY-NLP-006', 'quantity': '1', 'referral_source': 'organic', 'region_id': 'CA-QC', 'unit_price': '54.99', '_kafka_key': 'CA-QC', '_kafka_partition': 0, '_kafka_offset': 2}
| C05 | subtotal=54.99
| C05 | tax=8.23
| C05 | total=63.22
| C05 | running_total=312.15
| C05 | Wrote valid record to DuckDB:
| C05 |   order=14da1915-8e74-47be-9e10-f7275d31af46
| C05 | MESSAGE ACCEPTED
| C05 | order=14da1915-8e74-47be-9e10-f7275d31af46
| C05 | total=$63.22
| C05 | consumed=3
| C05 | RUNNING STATS
| C05 | total_sales=$312.15
| C05 | average=$104.05
| C05 | min=$54.11
| C05 | max=$194.82
| C05 | No message received within 10.0s timeout.
| C05 | Producer finished or paused. Stopping consumer.
| C05 | Kafka consumer closed.
| C05 | Saving artifacts...
| C05 | WROTE OUTPUT_CSV = data\output\consumed_sales.csv
| C05 | WROTE OUTPUT_DB = data\output\sales.duckdb
| C05 | ========================
| C05 | SECTION E. Exit
| C05 | ========================
| C05 | Summary:
| C05 | Consumed 3 message(s) from topic 'streaming-05-storage-case'.
| C05 | Skipped  0 message(s).
| C05 | OUTPUT_CSV = data\output\consumed_sales.csv
| C05 |   Total sales:  $312.15
| C05 |   Average sale: $104.05
| C05 |   Minimum sale: $54.11
| C05 |   Maximum sale: $194.82
| C05 | ========================
| C05 | Consumer executed successfully!
| C05 | ========================
```
