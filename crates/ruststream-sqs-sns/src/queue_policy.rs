//! The access policy statement that lets a topic deliver into a queue.
//!
//! SNS sends to an SQS endpoint as the SNS service principal, and the queue accepts the send
//! only when its own access policy allows that principal `sqs:SendMessage` on behalf of the
//! topic. A subscription without that statement is created, confirmed and silently useless:
//! every notification is dropped on the way. The queue's policy is the operator's document, so
//! the grant is merged into it, one statement per topic, and never replaces what is there.

use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sqs::types::QueueAttributeName;
use serde_json::{Map, Value, json};

use crate::broker::Aws;
use crate::error::{SqsError, sdk_err};

/// The principal SNS delivers to a queue as.
const SNS_SERVICE: &str = "sns.amazonaws.com";

/// The action a delivery into the queue is.
const SEND_MESSAGE: &str = "sqs:SendMessage";

/// The condition operators that pin a statement to one source ARN.
const ARN_CONDITIONS: [&str; 2] = ["ArnEquals", "ArnLike"];

impl Aws {
    /// Makes sure the queue's access policy lets `topic_arn` deliver into it.
    ///
    /// Reads the queue's policy, adds the topic's statement when no statement grants it yet, and
    /// writes the result back. A policy that already grants the topic is left untouched, so a
    /// service that subscribes its queues on every start writes nothing after the first one.
    pub(crate) async fn grant_topic_send(
        &self,
        names: (&str, &str),
        queue_url: &str,
        queue_arn: &str,
        topic_arn: &str,
    ) -> Result<(), SqsError> {
        let (topic, queue) = names;
        let failed = |source| SqsError::QueuePolicy {
            topic: topic.to_owned(),
            queue: queue.to_owned(),
            source,
        };
        // SQS offers no conditional write, so two grants racing on one queue would each write
        // back the policy they read and drop the other's statement. Within one broker they wait
        // for each other here.
        let _writing = self.policy_writes.lock().await;
        let current = self
            .clients
            .sqs(SqsClient::get_queue_attributes)
            .queue_url(queue_url)
            .attribute_names(QueueAttributeName::Policy)
            .send()
            .await
            .map_err(|e| failed(sdk_err(&e)))?
            .attributes()
            .and_then(|map| map.get(&QueueAttributeName::Policy))
            .cloned();
        let Some(policy) = with_topic_grant(current.as_deref(), queue_arn, topic_arn)
            .map_err(|reason| failed(Box::from(reason)))?
        else {
            return Ok(());
        };
        self.clients
            .sqs(SqsClient::set_queue_attributes)
            .queue_url(queue_url)
            .attributes(QueueAttributeName::Policy, policy)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| failed(sdk_err(&e)))
    }
}

/// The queue's policy with a statement granting `topic_arn` the right to send to the queue, or
/// `None` when a statement of `current` already grants it.
///
/// An empty or missing policy starts a new document.
fn with_topic_grant(
    current: Option<&str>,
    queue_arn: &str,
    topic_arn: &str,
) -> Result<Option<String>, String> {
    let mut policy = match current.map(str::trim) {
        None | Some("") => json!({ "Version": "2012-10-17" }),
        Some(text) => serde_json::from_str(text)
            .map_err(|e| format!("the queue's access policy is not JSON: {e}"))?,
    };
    let document = policy
        .as_object_mut()
        .ok_or("the queue's access policy is not a JSON object")?;
    let mut statements = match document.remove("Statement") {
        None => Vec::new(),
        Some(Value::Array(statements)) => statements,
        Some(statement @ Value::Object(_)) => vec![statement],
        Some(other) => {
            return Err(format!(
                "the queue's access policy holds a Statement that is not a statement: {other}"
            ));
        }
    };
    let granted = statements
        .iter()
        .any(|statement| grants(statement, topic_arn));
    if !granted {
        statements.push(grant(queue_arn, topic_arn));
    }
    document.insert("Statement".to_owned(), Value::Array(statements));
    Ok((!granted).then(|| policy.to_string()))
}

/// The statement that lets the SNS service send to `queue_arn` for `topic_arn` alone.
fn grant(queue_arn: &str, topic_arn: &str) -> Value {
    json!({
        "Sid": sid(topic_arn),
        "Effect": "Allow",
        "Principal": { "Service": SNS_SERVICE },
        "Action": SEND_MESSAGE,
        "Resource": queue_arn,
        "Condition": { "ArnEquals": { "aws:SourceArn": topic_arn } },
    })
}

/// Whether `statement` already lets the SNS service send to the queue for `topic_arn`.
///
/// What decides is the grant, not the statement's id: a statement the operator wrote for the
/// topic counts as well as the one this crate adds.
fn grants(statement: &Value, topic_arn: &str) -> bool {
    let Some(statement) = statement.as_object() else {
        return false;
    };
    statement.get("Effect").and_then(Value::as_str) == Some("Allow")
        && names(
            statement.get("Principal").and_then(|p| p.get("Service")),
            |service| service == SNS_SERVICE,
        )
        && names(statement.get("Action"), |action| {
            action.eq_ignore_ascii_case(SEND_MESSAGE)
        })
        && pins_source(statement, topic_arn)
}

/// Whether the statement's condition pins `aws:SourceArn` to `topic_arn`.
fn pins_source(statement: &Map<String, Value>, topic_arn: &str) -> bool {
    let Some(condition) = statement.get("Condition") else {
        return false;
    };
    ARN_CONDITIONS.iter().any(|operator| {
        names(
            condition.get(operator).and_then(|c| c.get("aws:SourceArn")),
            |arn| arn == topic_arn,
        )
    })
}

/// Whether a policy element (one string, or a list of them) holds a value `wanted` accepts.
fn names(element: Option<&Value>, wanted: impl Fn(&str) -> bool) -> bool {
    match element {
        Some(Value::String(value)) => wanted(value),
        Some(Value::Array(values)) => values.iter().filter_map(Value::as_str).any(wanted),
        _ => false,
    }
}

/// The statement id of a topic's grant: the same for the same topic on every run, and distinct
/// per topic. A statement id takes letters and digits only, so the ARN is folded into a stable
/// 64-bit FNV-1a hash rather than copied.
fn sid(topic_arn: &str) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let hash = topic_arn.bytes().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(PRIME)
    });
    format!("RustStreamSnsSend{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUEUE: &str = "arn:aws:sqs:us-east-1:123456789012:orders";
    const TOPIC: &str = "arn:aws:sns:us-east-1:123456789012:events";

    fn merged(current: Option<&str>) -> Value {
        let policy = with_topic_grant(current, QUEUE, TOPIC)
            .expect("the policy merges")
            .expect("the grant is added");
        serde_json::from_str(&policy).expect("the merged policy is JSON")
    }

    #[test]
    fn a_queue_without_a_policy_gets_a_document_with_the_grant() {
        for current in [None, Some(""), Some("  ")] {
            let policy = merged(current);
            assert_eq!(policy["Version"], "2012-10-17");
            assert_eq!(policy["Statement"], json!([grant(QUEUE, TOPIC)]));
        }
    }

    #[test]
    fn the_grant_is_appended_after_the_operators_statements() {
        let operator = json!({ "Sid": "Ops", "Effect": "Deny", "Principal": "*", "Action": "*" });
        let current = json!({ "Version": "2012-10-17", "Id": "keep", "Statement": [operator] });
        let policy = merged(Some(&current.to_string()));
        assert_eq!(policy["Id"], "keep");
        assert_eq!(policy["Statement"], json!([operator, grant(QUEUE, TOPIC)]));
    }

    #[test]
    fn a_single_statement_object_becomes_a_list() {
        let operator = json!({ "Sid": "Ops", "Effect": "Allow", "Action": "sqs:ReceiveMessage" });
        let current = json!({ "Statement": operator });
        let policy = merged(Some(&current.to_string()));
        assert_eq!(policy["Statement"], json!([operator, grant(QUEUE, TOPIC)]));
    }

    #[test]
    fn a_policy_that_grants_the_topic_is_left_as_it_is() {
        let ours = json!({ "Statement": [grant(QUEUE, TOPIC)] }).to_string();
        assert_eq!(with_topic_grant(Some(&ours), QUEUE, TOPIC), Ok(None));

        // An operator's statement in the other forms a policy allows grants the topic too.
        let operators = json!({ "Statement": {
            "Effect": "Allow",
            "Principal": { "Service": ["events.amazonaws.com", "sns.amazonaws.com"] },
            "Action": ["sqs:sendmessage"],
            "Condition": { "ArnLike": { "aws:SourceArn": ["arn:other", TOPIC] } },
        }})
        .to_string();
        assert_eq!(with_topic_grant(Some(&operators), QUEUE, TOPIC), Ok(None));
    }

    #[test]
    fn a_statement_short_of_the_grant_does_not_count() {
        let full = grant(QUEUE, TOPIC);
        let short_of = |field: &str, value: Value| {
            let mut statement = full.clone();
            statement[field] = value;
            statement
        };
        let unrelated = [
            json!("not a statement"),
            short_of("Effect", json!("Deny")),
            short_of("Principal", json!({ "AWS": "123456789012" })),
            short_of("Action", json!("sqs:ReceiveMessage")),
            short_of("Action", json!(42)),
            short_of(
                "Condition",
                json!({ "StringEquals": { "aws:SourceArn": TOPIC } }),
            ),
            short_of(
                "Condition",
                json!({ "ArnEquals": { "aws:SourceArn": "arn:other" } }),
            ),
        ];
        let mut without_condition = full;
        without_condition
            .as_object_mut()
            .expect("a statement is an object")
            .remove("Condition");
        for statement in unrelated.into_iter().chain([without_condition]) {
            let current = json!({ "Statement": [statement] }).to_string();
            assert!(
                with_topic_grant(Some(&current), QUEUE, TOPIC)
                    .expect("the policy merges")
                    .is_some(),
                "counted as the grant: {current}",
            );
        }
    }

    #[test]
    fn a_policy_that_is_not_a_document_is_refused() {
        for current in ["not json", "[]", r#"{"Statement": "everything"}"#] {
            assert!(
                with_topic_grant(Some(current), QUEUE, TOPIC).is_err(),
                "merged into {current}",
            );
        }
    }

    #[test]
    fn the_statement_id_is_stable_per_topic_and_distinct_between_topics() {
        assert_eq!(sid(TOPIC), sid(TOPIC));
        assert_ne!(
            sid(TOPIC),
            sid("arn:aws:sns:us-east-1:123456789012:events2")
        );
        assert!(sid(TOPIC).chars().all(|c| c.is_ascii_alphanumeric()));
    }
}
