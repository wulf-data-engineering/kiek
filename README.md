![kiek logo which is bascially the kafka logo with magnifying glasses instead of the circles](kiek.svg)

`kiek` (/ˈkiːk/ - Northern German for _Look!_) is a command line tool to look into Kafka topics, especially, if they are
_complicated_, e.g.

* in AWS MSK behind IAM authentication,
* requiring assuming an IAM role,
  containing AVRO encoded messages with schemas in AWS Glue Schema Registry,
* in different development environments ...

`kiek` **analyzes the messages** in a topic, **looks up corresponding schemas** if necessary and prints the payloads
with syntax highlighting in a **human-readable format**.

## Roadmap

### Publish

- Publish as binary releases to Github
  - https://github.com/rustwasm/wasm-pack/blob/51e6351c28fbd40745719e6d4a7bf26dadd30c85/.travis.yml#L74-L91
- Publish to homebrew with auto complete
  - https://github.com/BurntSushi/ripgrep/blob/31adff6f3c4bfefc9e77df40871f2989443e6827/pkg/brew/ripgrep-bin.rb
  - https://rust-cli.github.io/book/in-depth/docs.html
- More Integration Testing

### Increment Capabilities

- Services
  - Support OAuth (SASL OAUTHBEARER) via --token
  - Support (Confluent) Schema Registry
    - Basic Auth (SASL PLAIN/SCRAM)
    - OAuth (SASL OAUTHBEARER)
    - Support non-string keys
    - Add overrides for security settings for Schema Registry
- UX
  - Retry/Reconnect on Broker Transport Failure without captured failure
  - Failing Gracefully and Explaining Errors: schema lookup failure
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