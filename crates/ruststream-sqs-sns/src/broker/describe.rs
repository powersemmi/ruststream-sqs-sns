//! The server coordinate a broker reports to the `AsyncAPI` document.

use aws_config::{Region, SdkConfig};
use aws_sdk_sqs::config::endpoint::{DefaultResolver, Params, ResolveEndpoint};
use futures::FutureExt;
use ruststream::{DescribeServer, ServerSpec};

use super::SqsBroker;

impl DescribeServer for SqsBroker {
    /// Where the clients connect: the endpoint's host and port when an endpoint is known (named
    /// on the broker, or the `endpoint_url` of the config given to [`SqsBroker::from_config`]),
    /// otherwise the regional host of the known region (`sqs.eu-west-1.amazonaws.com`, or the
    /// host of the region's partition, `sqs.cn-north-1.amazonaws.com.cn`), and
    /// `sqs.amazonaws.com` when neither is known before `connect` resolves the environment.
    fn describe_server(&self) -> ServerSpec {
        let configured = self.sdk_config.as_ref();
        // An endpoint that carries no host at all (an empty override) says nothing about where
        // clients connect, so the next known coordinate answers instead.
        let host = [
            self.endpoint.as_deref(),
            configured.and_then(SdkConfig::endpoint_url),
        ]
        .into_iter()
        .flatten()
        .map(ServerSpec::host_from_url)
        .find(|host| !host.is_empty())
        .or_else(|| {
            let region = self
                .region
                .as_deref()
                .or_else(|| configured.and_then(SdkConfig::region).map(Region::as_ref))?;
            regional_host(region, configured)
        })
        .unwrap_or_else(|| "sqs.amazonaws.com".to_owned());
        ServerSpec::new(host, "sqs")
    }
}

/// The host the SDK's own endpoint rules give the SQS client for `region`, so a partition with
/// a suffix of its own (China, the isolated regions) is described the way the client reaches it.
fn regional_host(region: &str, configured: Option<&SdkConfig>) -> Option<String> {
    let params = Params::builder()
        .region(region)
        .set_use_fips(configured.and_then(SdkConfig::use_fips))
        .set_use_dual_stack(configured.and_then(SdkConfig::use_dual_stack))
        .build()
        .ok()?;
    // The default rules answer without awaiting anything: the future is ready when returned.
    let endpoint = DefaultResolver::new()
        .resolve_endpoint(&params)
        .now_or_never()?
        .ok()?;
    Some(ServerSpec::host_from_url(endpoint.url())).filter(|host| !host.is_empty())
}

#[cfg(test)]
mod tests {
    use aws_config::{Region, SdkConfig};
    use ruststream::DescribeServer;

    use super::SqsBroker;

    /// The host `broker` describes.
    fn host(broker: &SqsBroker) -> String {
        broker
            .describe_server()
            .host
            .expect("a broker describes a host")
    }

    /// A prebuilt config that names a region and nothing else.
    fn in_region(region: &str) -> SdkConfig {
        SdkConfig::builder()
            .region(Region::new(region.to_owned()))
            .build()
    }

    #[test]
    fn a_region_named_on_the_broker_describes_the_regional_host() {
        assert_eq!(
            host(&SqsBroker::new().region("eu-west-1")),
            "sqs.eu-west-1.amazonaws.com"
        );
    }

    #[test]
    fn the_region_of_a_prebuilt_config_describes_the_regional_host() {
        assert_eq!(
            host(&SqsBroker::from_config(in_region("ap-south-1"))),
            "sqs.ap-south-1.amazonaws.com"
        );
    }

    #[test]
    fn a_region_named_on_the_broker_wins_over_the_prebuilt_one() {
        assert_eq!(
            host(&SqsBroker::from_config(in_region("ap-south-1")).region("eu-west-1")),
            "sqs.eu-west-1.amazonaws.com"
        );
    }

    /// A region outside the commercial partition is served under its partition's own suffix.
    #[test]
    fn a_china_region_describes_the_host_of_its_partition() {
        assert_eq!(
            host(&SqsBroker::new().region("cn-north-1")),
            "sqs.cn-north-1.amazonaws.com.cn"
        );
    }

    #[test]
    fn the_endpoint_of_a_prebuilt_config_is_described() {
        assert_eq!(
            host(&SqsBroker::from_config(
                SdkConfig::builder()
                    .region(Region::new("us-east-1"))
                    .endpoint_url("http://localstack:4566")
                    .build()
            )),
            "localstack:4566"
        );
    }

    #[test]
    fn an_endpoint_named_on_the_broker_wins_over_the_prebuilt_one() {
        let configured = SdkConfig::builder()
            .endpoint_url("http://localstack:4566")
            .build();
        let broker = SqsBroker::from_config(configured).endpoint("http://named:4566");
        assert_eq!(host(&broker), "named:4566");
    }

    #[test]
    fn an_endpoint_wins_over_a_region() {
        let broker = SqsBroker::new()
            .region("eu-west-1")
            .endpoint("http://localstack:4566");
        assert_eq!(host(&broker), "localstack:4566");
    }

    /// The host a description carries for `endpoint`.
    fn described(endpoint: &str) -> String {
        SqsBroker::new()
            .endpoint(endpoint)
            .describe_server()
            .host
            .expect("a configured endpoint describes a host")
    }

    #[test]
    fn a_description_carries_the_host_and_port_of_every_endpoint_form() {
        assert_eq!(described("http://localstack:4566"), "localstack:4566");
        assert_eq!(described("localstack:4566"), "localstack:4566");
        assert_eq!(
            described("https://sqs.eu-west-1.amazonaws.com"),
            "sqs.eu-west-1.amazonaws.com"
        );
        assert_eq!(
            described("http://localstack:4566/queues/"),
            "localstack:4566"
        );
        assert_eq!(
            described("http://user:pass@localstack:4566"),
            "localstack:4566"
        );
    }

    #[test]
    fn a_description_carries_neither_a_scheme_nor_credentials() {
        for endpoint in [
            "http://localstack:4566",
            "localstack:4566",
            "https://sqs.eu-west-1.amazonaws.com",
            "http://localstack:4566/queues/",
            "http://user:pass@localstack:4566",
        ] {
            let host = described(endpoint);
            assert!(
                !host.contains("://") && !host.contains('@'),
                "the description of {endpoint:?} published {host:?}",
            );
        }
    }

    #[test]
    fn a_path_that_carries_an_at_sign_is_not_mistaken_for_credentials() {
        // The cuts are ordered for this case: taking the credentials out first would leave the
        // tail of the path as the host.
        assert_eq!(described("https://localstack:4566/a@b"), "localstack:4566");
    }

    #[test]
    fn the_public_service_is_described_when_no_endpoint_is_configured() {
        let described = SqsBroker::new().describe_server();
        assert_eq!(described.host.as_deref(), Some("sqs.amazonaws.com"));
        assert_eq!(described.protocol, "sqs");
    }
}
