//! The permission a topic needs to deliver into a queue, against a local stack.
//!
//! SNS sends to an SQS endpoint only when the queue's access policy lets the SNS service call
//! `sqs:SendMessage` on behalf of that topic. Without it the subscription is created and every
//! notification is dropped on the way, and a local stack delivers regardless, so a delivery test
//! cannot see the difference. What these tests read instead is the policy the queue holds after
//! `subscribe_queue_to_topic`.
//!
//! Start a stack with `just brokers-up`, then:
//! `SQS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

#![cfg(feature = "sns")]

use aws_config::{BehaviorVersion, Region};
use aws_sdk_sqs::types::QueueAttributeName;
use ruststream::ConnectedBroker;
use ruststream_sqs_sns::{ConnectedSqsBroker, SqsQueue};
use serde_json::{Value, json};

use crate::live;

use live::{admin, connect, queue_attribute, unique};

/// The stack these tests run against, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead of skipping.
fn test_endpoint() -> Option<String> {
    live::endpoint("SQS_TEST_ENDPOINT")
}

/// The topic's ARN as the service reports it. `CreateTopic` returns an existing topic as-is.
async fn topic_arn(endpoint: &str, topic: &str) -> String {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await;
    aws_sdk_sns::Client::new(&config)
        .create_topic()
        .name(topic)
        .send()
        .await
        .expect("the topic is there")
        .topic_arn()
        .expect("CreateTopic returns the ARN")
        .to_owned()
}

/// The statements of the queue's access policy, as the service holds them.
async fn statements(endpoint: &str, queue: &str) -> Vec<Value> {
    let policy = queue_attribute(endpoint, queue, QueueAttributeName::Policy).await;
    assert!(!policy.is_empty(), "the queue holds no access policy");
    let policy: Value = serde_json::from_str(&policy).expect("the policy is JSON");
    match &policy["Statement"] {
        Value::Array(statements) => statements.clone(),
        statement @ Value::Object(_) => vec![statement.clone()],
        other => panic!("the policy has no statements: {other}"),
    }
}

/// Whether `statement` lets the SNS service send to the queue on behalf of `topic_arn`.
fn grants_topic(statement: &Value, topic_arn: &str, queue_arn: &str) -> bool {
    statement["Effect"] == "Allow"
        && statement["Principal"]["Service"] == "sns.amazonaws.com"
        && statement["Action"] == "sqs:SendMessage"
        && statement["Resource"] == queue_arn
        && statement["Condition"]["ArnEquals"]["aws:SourceArn"] == topic_arn
}

/// How many statements of the queue's policy grant `topic` the right to send to it.
async fn grants(endpoint: &str, topic: &str, queue: &str) -> usize {
    let topic_arn = topic_arn(endpoint, topic).await;
    let queue_arn = queue_attribute(endpoint, queue, QueueAttributeName::QueueArn).await;
    statements(endpoint, queue)
        .await
        .iter()
        .filter(|statement| grants_topic(statement, &topic_arn, &queue_arn))
        .count()
}

/// Opens `queue` on the stack, creating it, so the topic has somewhere to deliver.
async fn create_queue(connected: &ConnectedSqsBroker, queue: &str) {
    drop(
        connected
            .subscribe_queue(SqsQueue::new(queue).create_if_missing())
            .await
            .expect("the queue opens"),
    );
}

// The whole defect: a subscription the topic cannot deliver through is a subscription that drops
// every notification. After the call, the queue lets this topic, and only through the SNS
// service, send to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribing_grants_the_topic_the_right_to_send_to_the_queue() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;
    let topic = unique("policy-grant");
    let queue = unique("policy-grant-inbox");
    create_queue(&connected, &queue).await;

    connected
        .subscribe_queue_to_topic(&topic, &queue)
        .await
        .expect("queue subscribes to topic");

    assert_eq!(grants(&endpoint, &topic, &queue).await, 1);
    connected.shutdown().await.expect("shutdown succeeds");
}

// The queue's policy belongs to its operator. A statement already on it stays there next to the
// topic's grant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_operators_statement_survives_the_grant() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;
    let topic = unique("policy-keep");
    let queue = unique("policy-keep-inbox");
    create_queue(&connected, &queue).await;
    let queue_arn = queue_attribute(&endpoint, &queue, QueueAttributeName::QueueArn).await;
    let operator = json!({
        "Sid": "OperatorReads",
        "Effect": "Allow",
        "Principal": { "AWS": "arn:aws:iam::000000000000:root" },
        "Action": "sqs:ReceiveMessage",
        "Resource": queue_arn,
    });
    let client = admin(&endpoint).await;
    let url = client
        .get_queue_url()
        .queue_name(&queue)
        .send()
        .await
        .expect("the queue is there")
        .queue_url()
        .expect("GetQueueUrl returns the URL")
        .to_owned();
    client
        .set_queue_attributes()
        .queue_url(url)
        .attributes(
            QueueAttributeName::Policy,
            json!({ "Version": "2012-10-17", "Statement": [operator.clone()] }).to_string(),
        )
        .send()
        .await
        .expect("the operator's policy is written");

    connected
        .subscribe_queue_to_topic(&topic, &queue)
        .await
        .expect("queue subscribes to topic");

    let statements = statements(&endpoint, &queue).await;
    assert!(
        statements.contains(&operator),
        "the operator's statement was lost: {statements:?}",
    );
    assert_eq!(grants(&endpoint, &topic, &queue).await, 1);
    connected.shutdown().await.expect("shutdown succeeds");
}

// A service subscribes its queues on every start, so the grant is written once however many times
// the call runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribing_twice_grants_once() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;
    let topic = unique("policy-twice");
    let queue = unique("policy-twice-inbox");
    create_queue(&connected, &queue).await;

    for _ in 0..2 {
        connected
            .subscribe_queue_to_topic(&topic, &queue)
            .await
            .expect("queue subscribes to topic");
    }

    assert_eq!(statements(&endpoint, &queue).await.len(), 1);
    assert_eq!(grants(&endpoint, &topic, &queue).await, 1);
    connected.shutdown().await.expect("shutdown succeeds");
}

// One queue fed by two topics needs a grant for each: the second subscription adds its own
// statement rather than replacing the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_topic_gets_its_own_grant() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let connected = connect(&endpoint).await;
    let first = unique("policy-first");
    let second = unique("policy-second");
    let queue = unique("policy-both-inbox");
    create_queue(&connected, &queue).await;

    for topic in [&first, &second] {
        connected
            .subscribe_queue_to_topic(topic, &queue)
            .await
            .expect("queue subscribes to topic");
    }

    assert_eq!(grants(&endpoint, &first, &queue).await, 1);
    assert_eq!(grants(&endpoint, &second, &queue).await, 1);
    connected.shutdown().await.expect("shutdown succeeds");
}
