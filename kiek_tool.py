import subprocess
import json

def list_topics(bootstrap_servers: str = "127.0.0.1:9092") -> str:
    """
    Lists all topics in a Kafka cluster.

    Args:
        bootstrap_servers: The Kafka cluster address.
    """
    command = ["kiek", "--bootstrap-servers", bootstrap_servers]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return f"Error listing topics: {e.stderr}"

def read_topic(
    topic: str,
    bootstrap_servers: str = "127.0.0.1:9092",
    offset: str = None,
    from_datetime: str = None,
    to_datetime: str = None,
    max_messages: int = None,
    timeout: int = 5,
    key: str = None,
    filter: str = None,
    authentication: str = None,
    username: str = None,
    password: str = None,
    schema_registry_url: str = None,
    aws_profile: str = None,
    aws_region: str = None,
    aws_role_arn: str = None,
    indent: bool = False,
    silent: bool = False,
    verbose: bool = False,
) -> str:
    """
    Reads messages from a Kafka topic. If no terminating condition is specified
    (max_messages or to_datetime), it will default to reading a maximum of 500 messages.

    Args:
        topic: The name of the topic to read from.
        bootstrap_servers: The Kafka cluster address.
        offset: Start consuming from a specific offset (e.g., 'earliest', 'latest', '-n').
        from_datetime: Start consuming from a specific timestamp (e.g., '2023-10-27T10:00:00Z').
        to_datetime: Stop consuming at a specific timestamp.
        max_messages: Stop after consuming a specific number of messages.
        timeout: Stop after a given number of seconds of inactivity.
        key: Filter messages by key.
        filter: Filter messages by payload content.
        authentication: Authentication mechanism (e.g., 'plain', 'msk-iam').
        username: SASL username.
        password: SASL password.
        schema_registry_url: URL for Confluent Schema Registry.
        aws_profile: AWS profile for MSK IAM and Glue Schema Registry.
        aws_region: AWS region for MSK IAM and Glue Schema Registry.
        aws_role_arn: AWS role ARN for MSK IAM.
        indent: Indent JSON output.
        silent: Omit progress indicators and warnings.
        verbose: Activate logging.
    """
    command = ["kiek", topic, "--bootstrap-servers", bootstrap_servers]
    if offset:
        command.extend(["--offset", offset])
    if from_datetime:
        command.extend(["--from", from_datetime])
    if to_datetime:
        command.extend(["--to", to_datetime])

    # If no terminating condition is set, add a default limit to prevent hanging.
    if max_messages:
        command.extend(["--max", str(max_messages)])
    if timeout:
        command.extend(["--timeout", str(timeout)])
    elif not to_datetime:
        command.extend(["--max", "500"])

    if key:
        command.extend(["--key", key])
    if filter:
        command.extend(["--filter", filter])
    if authentication:
        command.extend(["--authentication", authentication])
    if username:
        command.extend(["--username", username])
    if password:
        command.extend(["--password", password])
    if schema_registry_url:
        command.extend(["--schema-registry-url", schema_registry_url])
    if aws_profile:
        command.extend(["--profile", aws_profile])
    if aws_region:
        command.extend(["--region", aws_region])
    if aws_role_arn:
        command.extend(["--role-arn", aws_role_arn])
    if indent:
        command.append("--indent")
    if silent:
        command.append("--silent")
    if verbose:
        command.append("--verbose")

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        return f"Error reading topic '{topic}': {e.stderr}"