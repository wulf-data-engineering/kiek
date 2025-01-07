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

### Follow a topic

```shell
kiek some-topic                           # local broker, starts at the beginning
kiek some-topic -b kafka.example.com:9092 # remote broker, starts at the end
```

Follows all partitions of _some-topic_ and prints all messages on a local broker and new messages on remote brokers.

### Follow a partition

```shell
kiek some-topic-1 -o=-5
```

Follows partition _1_ of _some-topic_ and prints the newest five and all newer messages.

### Scan for a key

```shell
kiek some-topic -o=beginning -k=some-key
```

### Authenticate at remote broker

```shell
kiek -b kafka.example.com:9092 -u alice          # uses SASL/PLAIN
kiek -b kafka.example.com:9092 -u alice:password # uses SASL/PLAIN
kiek -b kafka.example.com:9092 -u alice -a plain
kiek -b kafka.example.com:9092 -u alice -a sha256
kiek -b kafka.example.com:9092 -u alice -a sha512
```

Prompts for password if not provided.

### Authenticate at AWS MSK

```shell
kiek -b kafka.example.com:9092 -a msk-iam                   # uses the default profile
kiek -b kafka.example.com:9092 -p my-profile
kiek -b kafka.example.com:9092 -p my-profile --role my-role # assumes the role
```

Checks if SSO is involved and the token expired.  
If a message payload is binary and contains a header and schema id, it looks up the AVRO schema in AWS Glue Schema
Registry with the same credentials.

### Configure Confluent Schema Registry

```shell
kiek -b kafka.example.com:9092 --schema-registry-url https://schema-registry.example.com:8081
kiek -b kafka.example.com:9092 --schema-registry-url https://schema-registry.example.com:8081 -u user          # uses Basic Auth
kiek -b kafka.example.com:9092 --schema-registry-url https://schema-registry.example.com:8081 -u user:password # uses Basic Auth
```

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

- Notarize macOS binary
    - pkg: https://stackoverflow.com/questions/43525852/create-pkg-installer-with-bare-executable
    - https://users.rust-lang.org/t/distributing-cli-apps-on-macos/70223
- Integration Testing against Redpanda

### Increment Capabilities

- Services
    - Support OAuth (SASL OAUTHBEARER) via --token
    - OAuth (SASL OAUTHBEARER) for Schema Registry (Authorization: Bearer < --token >)
    - Add overrides for security settings for Schema Registry
    - Support non-string keys
- UX
    - Explain schema lookup failures
    - Indicate reached head of topic with --earliest
    - Topic Profiles / --env for environment profiles
    - Ask for MFA token for SSO
- Navigation
    - search since timestamp (fixed, relative)
    - Default limit and continue with <enter>
- Output Formats
    - Key, Value, Timestamp, Offset, Partition, Topic

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
