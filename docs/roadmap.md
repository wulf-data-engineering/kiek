# Roadmap

## Increment Capabilities

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
    - Jump Host support
    - Default limit and continue with <enter>
    - Output Formats: Key, Value, Timestamp, Offset, Partition, Topic

## Increment to kieker

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
