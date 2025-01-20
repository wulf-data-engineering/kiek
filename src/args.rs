use crate::aws::list_profiles;
use crate::error::KiekError;
use crate::feedback::Feedback;
use crate::highlight::Highlighting;
use crate::kafka::{StartOffset, TopicOrPartition, DEFAULT_BROKER_STRING};
use crate::Result;
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use regex::Regex;
use serde::Serialize;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::io::IsTerminal;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::OnceLock;
use termion::clear;

#[derive(Parser, Debug)]
/// kiek (/ˈkiːk/ - Nothern German for Look!) helps you to look into Kafka topics, especially, if
/// they are in AWS MSK behind IAM authentication or even a role to assume and contain AVRO encoded
/// messages with schemas in AWS Glue Schema Registry.
/// kiek analyzes the message payloads in a topic, if necessary looks up corresponding AVRO schemas
/// and prints the payloads to stdout in a human-readable format.
#[command(version, about)]
pub struct Args {
    /// Kafka topic/partition name
    ///
    /// If a topic name is provided, kiek will consume from all partitions.
    /// If a topic/partition (e.g. "topic-0") is provided, kiek will consume from the specific partition.
    #[arg(value_name = "TOPIC/PARTITION")]
    pub topic_or_partition: Option<TopicOrPartition>,

    /// Kafka cluster broker string
    ///
    /// The Kafka cluster to connect to. Default is 127.0.0.1:9092.
    #[arg(short, long, value_name = "127.0.0.1:9092", aliases = ["brokers", "broker-string"])]
    pub bootstrap_servers: Option<String>,

    /// Optional start offset for the consumer: earliest, latest or a negative integer.
    ///
    /// If not set, the consumer will start from the earliest offset on localhost brokers and
    /// latest on remote brokers.
    ///
    /// Please note: in compacted topics the n latest offsets may not exist!
    #[arg(group = "start-offset", short, long, value_name = "earliest|latest|-n")]
    offset: Option<StartOffset>,

    /// Short option for --offset=earliest: Start from the beginning of the topic
    #[arg(group = "start-offset", short, long, action, aliases = ["beginning", "from-beginning"], hide_short_help = true
    )]
    earliest: bool,

    /// Short option for --offset=latest: Start from the end of the topic and wait for just new messages
    #[arg(group = "start-offset", short, long, action, hide_short_help = true)]
    latest: bool,

    /// Optional key to search scanning just the partition with the key
    ///
    /// If set, only messages with the given key are printed.
    /// kiek will just scan the partition that contains the key.
    /// Please note: this works only for the default partitioner. If you use a custom partitioner,
    /// you need to provide the topic/partition.
    #[arg(short, long)]
    pub key: Option<String>,

    /// Specifies the authentication mechanism
    ///
    /// If omitted and -u, --username is set, assumes SASL/PLAIN authentication.
    /// If omitted and AWS related options are set, assumes MSK IAM authentication.
    /// Otherwise, no authentication is attempted.
    #[arg(short, long, aliases = ["auth"], value_name = "plain|msk-iam|...")]
    authentication: Option<Authentication>,

    /// Username for SASL authentication
    ///
    /// If set but no authentication mechanism is specified, assumes SASL authentication.
    /// Optionally takes the password as well, separated by a colon.
    /// If no password is provided, kiek will ask for it.
    #[arg(
        short,
        long,
        aliases = ["user"],
        required_if_eq_any([("authentication", "plain"),("authentication", "sha256"), ("authentication", "sha512")]),
        value_name = "USER[:PASSWORD]")]
    username: Option<Username>,

    /// Password for SASL authentication
    ///
    /// If a username is set but no password provided, kiek will ask for it.
    #[arg(long, aliases = ["pw"], requires = "username")]
    password: Option<Password>,

    /// URL of the Confluent Schema Registry
    ///
    /// Used to decode AVRO messages with a Confluent Schema Registry id.
    /// If credentials are set, kiek will use them for Basic Auth to the schema registry, too.
    /// http://localhost:8081 is used as a default for local brokers.
    #[arg(long)]
    schema_registry_url: Option<String>,

    /// Optional specific AWS profile
    ///
    /// Used for MSK IAM authentication and Glue Schema Registry.
    /// If set but no authentication mechanism is specified, assumes MSK IAM authentication.
    /// If not set, $AWS_PROFILE or otherwise "default" is used.
    #[arg(short, long, value_name = "default")]
    pub profile: Option<String>,

    /// Optional specific AWS region
    ///
    /// Used for MSK IAM authentication and Glue Schema Registry.
    /// If set but no authentication mechanism is specified, assumes MSK IAM authentication.
    /// If not set, $AWS_REGION or otherwise "eu-central-1" is used.
    #[arg(short, long, value_name = "eu-central-1")]
    pub region: Option<String>,

    /// Optional AWS role to assume for MSK IAM authentication and Glue Schema Registry
    ///
    /// Used for MSK IAM authentication and Glue Schema Registry.
    /// If set, the AWS credentials provider will assume the role.
    /// If set but no authentication mechanism is specified, assumes MSK IAM authentication.
    #[arg(long)]
    pub role_arn: Option<String>,

    /// Deactivates syntax highlighting
    ///
    /// When running in a terminal kiek uses syntax highlighting for better readability.
    /// This option deactivates it if the terminal does not support colors.
    /// Piping the output to a file or another program automatically deactivates colors.
    #[arg(long, action, aliases = ["plain"], hide_short_help = true)]
    no_colors: bool,

    /// Omit everything but the Kafka messages
    ///
    /// If set, kiek omits all progress indicators, warnings and just prints the Kafka messages.
    /// Dialogs like asking for a topic name or schema registry URL are replaced with fatal errors.
    /// Piping the output to a file or another program is automatically silent.
    ///
    #[arg(group = "verbosity", short, long, action, hide_short_help = true)]
    pub silent: bool,

    /// Activates logging on stdout
    #[arg(group = "verbosity", short, long, action, hide_short_help = true)]
    pub verbose: bool,

    #[arg(long, action, hide = true)]
    pub no_ssl: bool,
}

impl Args {
    ///
    /// Parse and validate the command line arguments.
    ///
    pub async fn validated() -> Self {
        let args = Args::parse();
        args.validate().await.unwrap_or_else(|e| Args::fail(e));
        args
    }

    #[cfg(test)]
    async fn try_validated_from<I, T>(itr: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let args = Args::try_parse_from(itr)?;
        args.validate().await?;
        Ok(args)
    }

    async fn validate(&self) -> Result<()> {
        match &self.username {
            Some(Username(_, Some(_))) if self.password.is_some() => {
                return Err(KiekError::new(
                    "Use either --password or --username with password.",
                ));
            }
            Some(Username(_, None)) if self.password.is_none() && self.profile.is_some() => {
                let profile = self.profile.clone().unwrap();
                let profiles = list_profiles().await.unwrap_or(vec![profile.clone()]);
                if !profiles.contains(&profile) {
                    return Err(KiekError::new(format!("You passed a username but no password. You passed a non-existing AWS profile instead. Please note: {bold}-p, --profile{bold:#} specifies the AWS profile. {bold}--pw, --password{bold:#} sets the password for the username.", bold = self.highlighting().bold)));
                }
            }
            _ => {}
        }

        Ok(())
    }

    ///
    /// Let command exit with message of given error.
    ///
    pub fn fail(e: Box<dyn Error>) -> ! {
        let mut cmd = Self::command();
        println!("{}", clear::CurrentLine);
        cmd.error(ErrorKind::Io, e.to_string()).exit();
    }

    pub fn colors(&self) -> bool {
        !self.no_colors && std::io::stdout().is_terminal()
    }

    pub fn highlighting(&self) -> Highlighting {
        if self.colors() {
            Highlighting::colors()
        } else {
            Highlighting::plain()
        }
    }

    pub fn feedback(&self) -> Feedback {
        Feedback::prepare(&Highlighting::colors(), self.silent)
    }

    pub fn bootstrap_servers(&self) -> String {
        self.bootstrap_servers
            .clone()
            .unwrap_or(DEFAULT_BROKER_STRING.to_string())
    }

    pub fn schema_registry_url(&self) -> Option<String> {
        self.schema_registry_url.clone().or_else(|| {
            if is_local(&self.bootstrap_servers()) {
                Some("http://localhost:8081".to_string())
            } else {
                None
            }
        })
    }

    pub fn username(&self) -> Option<String> {
        self.username.clone()?.0.into()
    }

    pub fn password(&self) -> Option<Password> {
        self.username.clone()?.1.or(self.password.clone())
    }

    ///
    /// Returns the authentication mechanism based on the configuration:
    /// - If mechanism is specified, returns it
    /// - If username is set, returns SASL/PLAIN
    /// - If profile, region or role_arn is set, returns MSK IAM
    /// - Otherwise, no authentication is attempted
    ///
    pub fn authentication(&self) -> Authentication {
        self.authentication.clone().unwrap_or_else(|| {
            if self.username().is_some() {
                Authentication::Plain
            } else if self.profile.is_some() || self.region.is_some() || self.role_arn.is_some() {
                Authentication::MskIam
            } else {
                Authentication::None
            }
        })
    }

    ///
    /// Calculates the start offset for the consumer based on configuration:
    ///
    /// - `--earliest` beats `--latest` beats `--offset`
    /// - If no offset is set, `--earliest` is used for local brokers, `--latest` for remote brokers
    ///
    pub fn start_offset(&self) -> StartOffset {
        if self.earliest {
            StartOffset::Earliest
        } else if self.latest {
            StartOffset::Latest
        } else {
            match &self.offset {
                Some(offset) => offset.clone(),
                None if is_local(&self.bootstrap_servers()) => StartOffset::Earliest,
                None => StartOffset::Latest,
            }
        }
    }
}

fn topic_partition_regex() -> &'static Regex {
    static TOPIC_PARTITION_REGEX: OnceLock<Regex> = OnceLock::new();
    TOPIC_PARTITION_REGEX.get_or_init(|| Regex::new(r"^(.+)-([0-9]+)$").unwrap())
}

///
/// clap parser for topic names and topic-partition names
///
impl FromStr for TopicOrPartition {
    type Err = String;

    fn from_str(string: &str) -> std::result::Result<Self, Self::Err> {
        match topic_partition_regex().captures(string) {
            Some(captures) => {
                let topic = captures.get(1).unwrap().as_str().to_string();
                let partition = captures.get(2).unwrap().as_str().parse().unwrap();
                Ok(TopicOrPartition::TopicPartition(topic, partition))
            }
            None => Ok(TopicOrPartition::Topic(string.to_string())),
        }
    }
}

const INVALID_OFFSET: &str = "Offsets must be 'earliest', 'latest' or a negative integer.";

///
/// clap parser for Kafka consumer start offsets
/// * earliest/beginning: start from the beginning of the topic
/// * latest/end: start from current offset, that is, waits for future records
/// * negative integer: start from the latest n messages for each partition
///
impl FromStr for StartOffset {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "earliest" | "beginning" => Ok(StartOffset::Earliest),
            "latest" | "end" => Ok(StartOffset::Latest),
            s => {
                let offset: i64 = s.parse().map_err(|_| INVALID_OFFSET)?;
                if offset >= 0 {
                    Err(INVALID_OFFSET.to_string())
                } else {
                    Ok(StartOffset::Relative(-offset))
                }
            }
        }
    }
}

#[derive(clap::ValueEnum, PartialEq, Clone, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Authentication {
    Plain,
    #[value(alias("iam"))]
    MskIam,
    Sha256,
    Sha512,
    #[value(hide = true)]
    None,
}

///
/// Wrapper for a username with optional password
///
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Username(String, Option<Password>);

///
/// clap parser for username with optional password
///
impl FromStr for Username {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if let Some(colon) = s.find(':') {
            let username = s[..colon].to_string();
            let password = s[colon + 1..]
                .parse()
                .map_err(|_| "Could not parse password.")?;
            Ok(Username(username, Some(password)))
        } else {
            Ok(Username(s.to_string(), None))
        }
    }
}

///
/// Wrapper for the password to avoid leaking it in output
///
#[derive(Clone, PartialEq)]
pub(crate) struct Password(String);

impl Password {
    pub fn plain(&self) -> &str {
        &self.0
    }
}

impl Debug for Password {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "*****")
    }
}

impl Display for Password {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "*****")
    }
}

///
/// clap parser for passwords
///
impl FromStr for Password {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Password(s.to_string()))
    }
}

///
/// Check if the given bootstrap servers are local
///
fn is_local(bootstrap_servers: &str) -> bool {
    bootstrap_servers
        .split(',')
        .all(|server| match server.split(':').next() {
            None => false,
            Some(host) => {
                if host == "localhost" {
                    true
                } else {
                    match IpAddr::from_str(host) {
                        Ok(ip) => ip.is_loopback(),
                        Err(_) => false,
                    }
                }
            }
        })
}

// Test the arg parser
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parser() {
        let args = Args::parse_from([
            "kiek",
            "test-topic",
            "--bootstrap-servers",
            "localhost:9092",
        ]);
        assert_eq!(
            args.topic_or_partition,
            Some(TopicOrPartition::Topic("test-topic".to_string()))
        );
        assert_eq!(args.bootstrap_servers, Some("localhost:9092".to_string()));
        assert_eq!(args.key, None);
        assert_eq!(args.authentication, None);
        assert_eq!(args.username, None);
        assert_eq!(args.password, None);
        assert_eq!(args.profile, None);
        assert_eq!(args.region, None);
        assert_eq!(args.role_arn, None);
        assert!(!args.verbose);
        assert_eq!(args.offset, None);

        let args = Args::parse_from([
            "kiek",
            "test-topic",
            "--bootstrap-servers",
            "localhost:9092",
            "--profile",
            "test-profile",
            "--region",
            "us-west-1",
            "--verbose",
            "--offset",
            "earliest",
            "--key",
            "test-key",
        ]);
        assert_eq!(
            args.topic_or_partition,
            Some(TopicOrPartition::Topic("test-topic".to_string()))
        );
        assert_eq!(args.bootstrap_servers, Some("localhost:9092".to_string()));
        assert_eq!(args.key, Some("test-key".to_string()));
        assert_eq!(args.profile, Some("test-profile".to_string()));
        assert_eq!(args.region, Some("us-west-1".to_string()));
        assert_eq!(args.role_arn, None);
        assert!(args.verbose);
        assert_eq!(args.offset, Some(StartOffset::Earliest));

        let args = Args::parse_from([
            "kiek",
            "test-topic-1",
            "--bootstrap-servers",
            "localhost:9092",
            "--profile",
            "test-profile",
            "--region",
            "us-west-1",
            "--role-arn",
            "arn:aws:iam::123456789012:role/test-role",
            "--offset=-3",
            "-k",
            "test-key",
        ]);
        assert_eq!(
            args.topic_or_partition,
            Some(TopicOrPartition::TopicPartition(
                "test-topic".to_string(),
                1
            ))
        );
        assert_eq!(args.bootstrap_servers, Some("localhost:9092".to_string()));
        assert_eq!(args.key, Some("test-key".to_string()));
        assert_eq!(args.profile, Some("test-profile".to_string()));
        assert_eq!(args.region, Some("us-west-1".to_string()));
        assert_eq!(
            args.role_arn,
            Some("arn:aws:iam::123456789012:role/test-role".to_string())
        );
        assert!(!args.verbose);
        assert_eq!(args.offset, Some(StartOffset::Relative(3)));
    }

    #[tokio::test]
    async fn test_max_one_offset_configuration() {
        assert!(Args::try_validated_from([
            "kiek",
            "test-topic",
            "--offset=latest",
            "--offset=earliest"
        ])
        .await
        .is_err());
        assert!(
            Args::try_validated_from(["kiek", "test-topic", "--latest", "--earliest"])
                .await
                .is_err()
        );
        assert!(Args::try_validated_from([
            "kiek",
            "test-topic",
            "--offset=earliest",
            "--earliest"
        ])
        .await
        .is_err());
        assert!(
            Args::try_validated_from(["kiek", "test-topic", "--offset=-1", "--latest"])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_auth_configuration() {
        assert_eq!(
            Args::try_validated_from(["kiek", "-a", "plain", "-u", "required"])
                .await
                .unwrap()
                .authentication,
            Some(Authentication::Plain)
        );
        assert_eq!(
            Args::try_validated_from(["kiek", "--authentication", "plain", "-u", "required"])
                .await
                .unwrap()
                .authentication,
            Some(Authentication::Plain)
        );

        assert_eq!(
            Args::try_validated_from(["kiek", "-a", "msk-iam"])
                .await
                .unwrap()
                .authentication,
            Some(Authentication::MskIam)
        );
        assert_eq!(
            Args::try_validated_from(["kiek", "-a", "iam"])
                .await
                .unwrap()
                .authentication,
            Some(Authentication::MskIam)
        );

        assert_eq!(
            Args::try_validated_from(["kiek", "-a", "sha256", "-u", "required"])
                .await
                .unwrap()
                .authentication,
            Some(Authentication::Sha256)
        );
        assert_eq!(
            Args::try_validated_from(["kiek", "-a", "sha512", "-u", "required"])
                .await
                .unwrap()
                .authentication,
            Some(Authentication::Sha512)
        );

        // SASL/* requires username
        assert!(Args::try_validated_from(["kiek", "-a", "plain"])
            .await
            .is_err());
        assert!(Args::try_validated_from(["kiek", "-a", "sha256"])
            .await
            .is_err());
        assert!(Args::try_validated_from(["kiek", "-a", "sha512"])
            .await
            .is_err());

        assert!(Args::try_validated_from(["kiek", "-a", "msk-iam"])
            .await
            .is_ok());
        assert!(Args::try_validated_from(["kiek", "-a", "iam"])
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_password_requires_username() {
        assert!(Args::try_validated_from(["kiek", "--password", "bar"])
            .await
            .is_err());
        assert!(Args::try_validated_from(["kiek", "--pw", "bar"])
            .await
            .is_err());
        assert!(
            Args::try_validated_from(["kiek", "--pw", "bar", "-u", "foo"])
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_just_one_password() {
        assert!(Args::try_validated_from(["kiek", "-u", "foo:bar"])
            .await
            .is_ok());
        assert!(
            Args::try_validated_from(["kiek", "-u", "foo:bar", "--pw", "bar"])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_authentication() {
        let args =
            Args::try_validated_from(["kiek", "-a", "plain", "-u", "foo:bar", "-p", "aws-profile"])
                .await
                .unwrap();
        assert_eq!(args.authentication(), Authentication::Plain);
    }

    #[test]
    fn test_topic_partition_parser() {
        assert_eq!(
            TopicOrPartition::from_str("test-topic"),
            Ok(TopicOrPartition::Topic("test-topic".to_string()))
        );
        assert_eq!(
            TopicOrPartition::from_str("test-topic-0"),
            Ok(TopicOrPartition::TopicPartition(
                "test-topic".to_string(),
                0
            ))
        );
        assert_eq!(
            TopicOrPartition::from_str("test-topic-1"),
            Ok(TopicOrPartition::TopicPartition(
                "test-topic".to_string(),
                1
            ))
        );
        assert_eq!(
            TopicOrPartition::from_str("test-topic-10"),
            Ok(TopicOrPartition::TopicPartition(
                "test-topic".to_string(),
                10
            ))
        );
        assert_eq!(
            TopicOrPartition::from_str("test-topic-"),
            Ok(TopicOrPartition::Topic("test-topic-".to_string()))
        );
        assert_eq!(
            TopicOrPartition::from_str("test-topic-abc"),
            Ok(TopicOrPartition::Topic("test-topic-abc".to_string()))
        );
    }

    #[test]
    fn test_offsets_parser() {
        assert_eq!(StartOffset::from_str("earliest"), Ok(StartOffset::Earliest));
        assert_eq!(
            StartOffset::from_str("beginning"),
            Ok(StartOffset::Earliest)
        );
        assert_eq!(StartOffset::from_str("latest"), Ok(StartOffset::Latest));
        assert_eq!(StartOffset::from_str("end"), Ok(StartOffset::Latest));
        assert_eq!(StartOffset::from_str("-1"), Ok(StartOffset::Relative(1)));
        assert_eq!(StartOffset::from_str("-10"), Ok(StartOffset::Relative(10)));
        assert_eq!(StartOffset::from_str("0"), Err(INVALID_OFFSET.to_string()));
        assert_eq!(StartOffset::from_str("1"), Err(INVALID_OFFSET.to_string()));
        assert_eq!(
            StartOffset::from_str("latest1"),
            Err(INVALID_OFFSET.to_string())
        );
    }

    #[test]
    fn test_username_parser() {
        let username: Username = "foo".parse().unwrap();
        assert_eq!(username, Username("foo".to_string(), None));

        let username: Username = "foo-bar".parse().unwrap();
        assert_eq!(username, Username("foo-bar".to_string(), None));

        let username: Username = "foo:bar".parse().unwrap();
        assert_eq!(
            username,
            Username("foo".to_string(), Some(Password("bar".to_string())))
        );

        let username: Username = "foo:bar:baz".parse().unwrap();
        assert_eq!(
            username,
            Username("foo".to_string(), Some(Password("bar:baz".to_string())))
        );
    }

    #[test]
    fn test_password_handling() {
        let password: Password = "test-password".parse().unwrap();
        assert_eq!(password.plain(), "test-password");
        assert_eq!(format!("{password}"), "*****");
        assert_eq!(format!("{:?}", password), "*****");
    }

    #[test]
    fn test_local_check() {
        assert!(is_local("localhost"));
        assert!(is_local("127.0.0.1"));
        assert!(is_local("localhost:9092"));
        assert!(is_local("localhost:9092,localhost:19092"));
        assert!(is_local("127.0.0.1:9092"));
        assert!(is_local("127.0.0.1:9092,127.0.0.1:19092"));

        assert!(!is_local("123.542.123.123:9092,123.542.123.124:9092"));
        assert!(!is_local(
            "b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198"
        ));
        assert!(!is_local("123.542.123.123:9092localhost:9092"));
        assert!(!is_local("localhost:9092,123.542.123.123:9092"));
    }
}
