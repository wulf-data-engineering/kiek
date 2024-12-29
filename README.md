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

### Homebrew on macOS and Linux (latest release)

```shell
brew install wulf-data-engineering/tap/kiek
```

### Cargo on macOS, Linux and Windows (current main)

Install cargo from https://rustup.rs/ and run

```shell
cargo install --git https://github.com/wulf-data-engineering/kiek
```

Make sure to have the `~/.cargo/bin` directory in your `PATH`.

## Roadmap

### Publish

- Publish to homebrew with auto complete
  - https://extrawurst.medium.com/github-actions-homebrew-%EF%B8%8F-2789ae5023fd
  - https://github.com/akeru-inc/xcnotary (taps, instructions)
  - https://github.com/BurntSushi/ripgrep/blob/31adff6f3c4bfefc9e77df40871f2989443e6827/pkg/brew/ripgrep-bin.rb
  - https://rust-cli.github.io/book/in-depth/docs.html
- Notarize macOS binary
  - pkg: https://stackoverflow.com/questions/43525852/create-pkg-installer-with-bare-executable
  - https://users.rust-lang.org/t/distributing-cli-apps-on-macos/70223
- More Integration Testing

### Increment Capabilities

- Services
  - Support OAuth (SASL OAUTHBEARER) via --token
  - Support (Confluent) Schema Registry
    - Basic Auth (SASL PLAIN/SCRAM)
    - OAuth (SASL OAUTHBEARER)
    - Add overrides for security settings for Schema Registry
  - Support non-string keys
- UX
  - Explain schema lookup failures
  - Indicate reached head of topic with --earliest
  - Topic Profiles / --env for environment profiles
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
