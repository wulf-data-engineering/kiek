![kiek logo which is bascially the kafka logo with magnifying glasses instead of the circles](kiek.svg)

[![Continuous Integration](https://github.com/wulf-data-engineering/kiek/actions/workflows/main.yml/badge.svg)](https://github.com/wulf-data-engineering/kiek/actions/workflows/main.yml)

`kiek` (/ˈkiːk/ - Northern German for _Look!_) is a command line tool to look into Kafka topics, especially, if they are
_complicated_, e.g.

* in AWS MSK behind IAM authentication,
* requiring assuming an IAM role,
* containing AVRO encoded messages with schemas in AWS Glue Schema Registry,
* in different development environments ...

`kiek` **analyzes the messages** in a topic, **looks up corresponding schemas** if necessary and prints the payloads
with syntax highlighting in a **human-readable format**.

## Installation

### Homebrew on macOS and Linux *(latest release)*

**Only Apple Silicon and x86 64-bit Linux are supported for now.**

Installs completions for _zsh_, _bash_ and _fish shell_.

```shell
brew install wulf-data-engineering/tap/kiek
```

### Cargo on all platforms *(trunk)*

**Supports all platforms capable of compiling Rust.**

Install cargo from https://rustup.rs/ and run

```shell
cargo install --git https://github.com/wulf-data-engineering/kiek
```

Make sure to have the `~/.cargo/bin` directory in your `PATH`.

## Usage

### Connect to local cluster

```shell
kiek
```

Connects to [Redpanda](https://www.redpanda.com) ([suggested](docker-compose.yml) locally) or Kafka on 172.0.0.1:9092
without authentication and lists topics for selection.

### List topics

```shell
kiek --list                       # list topics from local cluster
kiek --list-topics                # same as above
kiek --list -b kafka.example.com:9092 # list topics from remote cluster
```

Lists all available topics in the Kafka cluster with their partition counts.

### Follow a topic

```shell
kiek some-topic                           # local broker, starts at the beginning
kiek some-topic -b kafka.example.com:9092 # remote broker, starts at the end
```

Follows all partitions of _some-topic_.  
On a local broker, it starts at the beginning of the topic, on a remote broker at the end.

### Follow a partition

```shell
kiek some-topic-1 -o=-5
```

If you add a partition number to the topic name, it follows only that partition: partition _1_ of _some-topic_.  
The **--offset**, **-o** option sets the offset to start from.
`-5` starts five messages before the end of the topic. `earliest` and `latest` are also valid.

### Scan for a key

```shell
kiek some-topic -o=beginning -k=some-key
```

If you are looking for a specific key, you can scan the topic for it.
kiek will calculate the partition for the key, follow that partition and print all messages with that key.

**Please note:** That requires the default partitioning strategy of the Java Kafka cliebt (murmur2 hash). If you use a
different strategy, you have to calculate the partition yourself. The librdkafka library (Kafka client for C/C++) by
default does not use the murmur2 hash.

### Filter messages

```shell
kiek some-topic -o=beginning -f="some string"
```
The **--filter**, **-f** option lets kiek just print messages that contain a string in key or payload.  
For string values it is a simple substring search, for AVRO payloads it is a check on the JSON representation.

### Start or stop at a specific point in time

```shell
kiek some-topic --from=now
kiek some-topic --from=-10s
kiek some-topic --from=-60m --to=-30m
kiek some-topic --from=-60m --to=now
kiek some-topic --from=-24h
kiek some-topic --from=-7d
kiek some-topic --from=-"2023-10-01"          --to=-"2023-10-01"       # 24h
kiek some-topic --from=-"2023-10-01 12:34"    --to=-"2023-10-01 12:34" # 1 minute
kiek some-topic --from=-"2023-10-01 12:34:56"
```

The **--from** and **--to** options let you specify a time range to start or stop reading messages.

Absolute timestamps are in **local time**, so that they match the timestamps in the messages you read.  
If you pass an absolute day, minute or second, kiek will treat them inclusively:

* `--from=-"2023-10-01"` means `2023-10-01 00:00:00` while
* `--to=-"2023-10-01"` means `2023-10-01 23:59:59.999999`.

### Authenticate at remote broker

```shell
kiek -b kafka.example.com:9092 -u alice             # uses SASL/PLAIN
kiek -b kafka.example.com:9092 -u alice:password    # uses SASL/PLAIN
kiek -b kafka.example.com:9092 -u alice -p password # uses SASL/PLAIN
kiek -b kafka.example.com:9092 -u alice -a plain
kiek -b kafka.example.com:9092 -u alice -a sha256
kiek -b kafka.example.com:9092 -u alice -a sha512
```

If you connect to a remote broker, you most likely need to authenticate.
kiek supports the standard Kafka mechanisms SASL/PLAIN, SASL/SCRAM-SHA-256 and SASL/SCRAM-SHA-512 for now.

kiek will prompt for a password if you have not provided it.

### Authenticate at AWS MSK

```shell
kiek -b kafka.example.com:9092 -a msk-iam                   # uses the default profile
kiek -b kafka.example.com:9092 -p my-profile
kiek -b kafka.example.com:9092 -p my-profile --role my-role # assumes the role
kiek -b kafka.example.com:9092 -p my-profile -r eu-east-1
```

If you connect to an AWS MSK cluster, you can use the `msk-iam` authentication method.  
The MSK IAM authentication is also assumed if you pass the **--profile**, **-p** option for an AWS profile.

kiek checks if SSO is involved and informs you if the token expired.

If kiek encounters a binary payload in the AVRO format of the AWS Glue Schema Registry (a header and schema id), it
looks up the AVRO schema in the same AWS account.

### Configure Confluent Schema Registry

```shell
kiek -b kafka.example.com:9092 --schema-registry-url https://schema-registry.example.com:8081
kiek -b kafka.example.com:9092 --schema-registry-url https://schema-registry.example.com:8081 -u user          # uses Basic Auth
kiek -b kafka.example.com:9092 --schema-registry-url https://schema-registry.example.com:8081 -u user:password # uses Basic Auth
```

If kiek encounters a binary payload in the AVRO format of the Confluent Schema Registry, it looks up the schema in the
configured registry.

Default is `http://localhost:8081` if the broker is local.

If credentials are provided to the broker, the credentials are passed via Basic Auth to the schema registry, too.

### Show the help

```shell
kiek -h     # short help
kiek --help # detailed help
```

See the help for all options.

## Releasing

To release a new version, start
the [Prepare Release](https://github.com/wulf-data-engineering/kiek/actions/workflows/release_pr.yml) workflow with
major, minor or
patch as input.

It creates a pull request for the new version.

Merging that pull request starts
the [Release](https://github.com/wulf-data-engineering/kiek/actions/workflows/release.yml) workflow, which creates a new
draft release with the binaries for macOS and Linux.
It also drafts a pull request for
the [Homebrew tap Formula](https://github.com/wulf-data-engineering/homebrew-tap/blob/main/Formula/kiek.rb).

## Roadmap

### Publish

- Landing Page
- Revise docs
  - installation.md
  - usage.md
  - release.md
  - roadmap.md

### Increment Capabilities

- Services
    - Support OAuth (SASL OAUTHBEARER) via --token
    - OAuth (SASL OAUTHBEARER) for Schema Registry (Authorization: Bearer < --token >)
    - Add overrides for security settings for Schema Registry
    - Support Protocol Buffers (How can we detect them for Confluent and AWS Glue?)
- UX
    - -d --describe[-topic]
    - Explain schema lookup failures
    - Indicate reached head of topic with --earliest
    - Topic Profiles / --env for environment profiles
    - **Derive authentication / no-ssl default from ports**: https://docs.aws.amazon.com/msk/latest/developerguide/port-info.html
    - Jump Host support
    - Default limit and continue with <enter>
    - Output Formats: Key, Value, Timestamp, Offset, Partition, Topic

### Increment to kieker

- Tauri
- Cluster Configuration
- Topic List
    - Favorites (already scanned)
- Datadog view
    - Table view
        - Topic Partition
        - Offset
    - Key Filter
    - Timestamp / Offset Input
    - Timeline
        - no. of messages -> bars
        - null / non-null -> transparency
        - schema versions -> color
