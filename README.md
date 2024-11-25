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

- (local) Kafka without auth
- Integration Testing
- CI/CD
- Publish to homebrew

### Increment Capabilities

- Services
  - SASL/SCRAM with user & password
  - Schema Registry
  - Support Redpanda
  - Support Confluent Schema Registry
- UX
  - Indicate reached head of topic with --earliest
  - Failing Gracefully and Explaining Errors: schema lookup failure
  - Topic Profiles / --env for environment profiles
  - Try 19202 after 9202 if no brokers are configured
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