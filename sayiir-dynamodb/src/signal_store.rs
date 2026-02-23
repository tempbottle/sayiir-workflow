//! [`SignalStore`] implementation for DynamoDB.
//!
//! Overrides the 3 default composite methods with `TransactWriteItems`
//! implementations using version-based optimistic concurrency.

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use sayiir_core::codec::{self, Decoder, Encoder};
use sayiir_core::snapshot::{
    PauseRequest, SignalKind, SignalRequest, SnapshotStatus, WorkflowSnapshot,
};
use sayiir_persistence::{BackendError, SignalStore, SnapshotStore};

use crate::backend::DynamoDbBackend;
use crate::error::sdk_err;
use crate::helpers::{
    completed_task_count, current_task_id, delay_wake_at, error_message, position_kind, status_str,
};
use crate::snapshot_store::snap_pk;

/// Build the partition key for a signal item: `SIG#{instance_id}`.
fn sig_pk(instance_id: &str) -> String {
    format!("SIG#{instance_id}")
}

/// Build the partition key for an event queue item: `{instance_id}#{signal_name}`.
fn event_pk(instance_id: &str, signal_name: &str) -> String {
    format!("{instance_id}#{signal_name}")
}

impl<C> SignalStore for DynamoDbBackend<C>
where
    C: Encoder
        + Decoder
        + codec::sealed::EncodeValue<WorkflowSnapshot>
        + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    async fn store_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
        request: SignalRequest,
    ) -> Result<(), BackendError> {
        tracing::debug!(instance_id, kind = %kind.as_ref(), "storing signal");

        // Validate workflow state first
        let snapshot = self.load_snapshot(instance_id).await?;
        let status = status_str(&snapshot.state);
        validate_signal_allowed(status, kind)?;

        let mut item = std::collections::HashMap::new();
        item.insert("PK".to_string(), AttributeValue::S(sig_pk(instance_id)));
        item.insert(
            "SK".to_string(),
            AttributeValue::S(kind.as_ref().to_string()),
        );
        if let Some(ref reason) = request.reason {
            item.insert("reason".to_string(), AttributeValue::S(reason.clone()));
        }
        if let Some(ref by) = request.requested_by {
            item.insert("requested_by".to_string(), AttributeValue::S(by.clone()));
        }
        let now = chrono::Utc::now().to_rfc3339();
        item.insert("created_at".to_string(), AttributeValue::S(now));

        self.client
            .put_item()
            .table_name(&self.snapshots_table)
            .set_item(Some(item))
            .send()
            .await
            .map_err(sdk_err)?;

        Ok(())
    }

    async fn get_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
    ) -> Result<Option<SignalRequest>, BackendError> {
        tracing::debug!(instance_id, kind = %kind.as_ref(), "getting signal");

        let result = self
            .client
            .get_item()
            .table_name(&self.snapshots_table)
            .key("PK", AttributeValue::S(sig_pk(instance_id)))
            .key("SK", AttributeValue::S(kind.as_ref().to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(sdk_err)?;

        Ok(result.item().map(|item| {
            let reason = item.get("reason").and_then(|v| v.as_s().ok()).cloned();
            let requested_by = item
                .get("requested_by")
                .and_then(|v| v.as_s().ok())
                .cloned();
            let requested_at = item
                .get("created_at")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map_or_else(chrono::Utc::now, |dt| dt.with_timezone(&chrono::Utc));

            SignalRequest {
                reason,
                requested_by,
                requested_at,
            }
        }))
    }

    async fn clear_signal(&self, instance_id: &str, kind: SignalKind) -> Result<(), BackendError> {
        tracing::debug!(instance_id, kind = %kind.as_ref(), "clearing signal");

        self.client
            .delete_item()
            .table_name(&self.snapshots_table)
            .key("PK", AttributeValue::S(sig_pk(instance_id)))
            .key("SK", AttributeValue::S(kind.as_ref().to_string()))
            .send()
            .await
            .map_err(sdk_err)?;

        Ok(())
    }

    async fn send_event(
        &self,
        instance_id: &str,
        signal_name: &str,
        payload: bytes::Bytes,
    ) -> Result<(), BackendError> {
        tracing::debug!(instance_id, signal_name, "buffering external event");

        let ulid = ulid::Ulid::new().to_string();

        let mut item = std::collections::HashMap::new();
        item.insert(
            "PK".to_string(),
            AttributeValue::S(event_pk(instance_id, signal_name)),
        );
        item.insert("SK".to_string(), AttributeValue::S(ulid));
        item.insert(
            "payload".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(payload.to_vec())),
        );

        self.client
            .put_item()
            .table_name(&self.events_table)
            .set_item(Some(item))
            .send()
            .await
            .map_err(sdk_err)?;

        Ok(())
    }

    async fn consume_event(
        &self,
        instance_id: &str,
        signal_name: &str,
    ) -> Result<Option<bytes::Bytes>, BackendError> {
        tracing::debug!(instance_id, signal_name, "consuming oldest buffered event");

        // Query the oldest event
        let result = self
            .client
            .query()
            .table_name(&self.events_table)
            .key_condition_expression("PK = :pk")
            .expression_attribute_values(
                ":pk",
                AttributeValue::S(event_pk(instance_id, signal_name)),
            )
            .scan_index_forward(true)
            .limit(1)
            .send()
            .await
            .map_err(sdk_err)?;

        let items = result.items();
        let Some(item) = items.first() else {
            return Ok(None);
        };

        let sk = item
            .get("SK")
            .and_then(|v| v.as_s().ok())
            .ok_or_else(|| BackendError::Backend("missing SK in event".to_string()))?;

        let payload = item
            .get("payload")
            .and_then(|v| v.as_b().ok())
            .ok_or_else(|| BackendError::Backend("missing payload in event".to_string()))?;
        let payload_bytes = bytes::Bytes::copy_from_slice(payload.as_ref());

        // Delete with condition (the SK must still match — handles concurrent consumers)
        match self
            .client
            .delete_item()
            .table_name(&self.events_table)
            .key("PK", AttributeValue::S(event_pk(instance_id, signal_name)))
            .key("SK", AttributeValue::S(sk.clone()))
            .condition_expression("attribute_exists(PK)")
            .send()
            .await
        {
            Ok(_) => Ok(Some(payload_bytes)),
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_conditional_check_failed_exception() {
                    // Someone else consumed it first — retry
                    Ok(None)
                } else {
                    Err(BackendError::Backend(service_err.to_string()))
                }
            }
        }
    }

    // --- Overridden composites: TransactWriteItems with version conditions ---

    async fn check_and_cancel(
        &self,
        instance_id: &str,
        interrupted_at_task: Option<&str>,
    ) -> Result<bool, BackendError> {
        tracing::debug!(instance_id, "checking for cancel signal");

        // Read the signal
        let Some(request) = self.get_signal(instance_id, SignalKind::Cancel).await? else {
            return Ok(false);
        };

        // Read the snapshot + version
        let mut snapshot = self.load_snapshot(instance_id).await?;
        let version = self.get_current_version(instance_id).await?;

        if !snapshot.state.is_in_progress() {
            return Ok(false);
        }

        snapshot.mark_cancelled(
            request.reason,
            request.requested_by,
            interrupted_at_task.map(String::from),
        );

        let data = self.encode(&snapshot)?;
        let status = status_str(&snapshot.state).to_string();
        let error = error_message(&snapshot).map(ToString::to_string);
        let pos_kind = position_kind(&snapshot).map(ToString::to_string);
        let wake_at = delay_wake_at(&snapshot);
        let now = chrono::Utc::now().to_rfc3339();
        let new_version = version + 1;

        let mut snap_item = std::collections::HashMap::new();
        snap_item.insert("PK".to_string(), AttributeValue::S(snap_pk(instance_id)));
        snap_item.insert("SK".to_string(), AttributeValue::S("current".to_string()));
        snap_item.insert(
            "data".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(data)),
        );
        snap_item.insert(
            "version".to_string(),
            AttributeValue::N(new_version.to_string()),
        );
        snap_item.insert("status".to_string(), AttributeValue::S(status));
        snap_item.insert(
            "definition_hash".to_string(),
            AttributeValue::S(snapshot.definition_hash.clone()),
        );
        snap_item.insert("updated_at".to_string(), AttributeValue::S(now.clone()));
        snap_item.insert(
            "instance_id".to_string(),
            AttributeValue::S(instance_id.to_string()),
        );
        snap_item.insert("completed_at".to_string(), AttributeValue::S(now));
        if let Some(ref err) = error {
            snap_item.insert("error".to_string(), AttributeValue::S(err.clone()));
        }
        if let Some(ref pk) = pos_kind {
            snap_item.insert("position_kind".to_string(), AttributeValue::S(pk.clone()));
        }
        if let Some(ref wa) = wake_at {
            snap_item.insert(
                "delay_wake_at".to_string(),
                AttributeValue::S(wa.to_rfc3339()),
            );
        }

        // TransactWriteItems: update snapshot (with version condition) + delete signal
        self.client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(&self.snapshots_table)
                            .set_item(Some(snap_item))
                            .condition_expression("version = :expected_version")
                            .expression_attribute_values(
                                ":expected_version",
                                AttributeValue::N(version.to_string()),
                            )
                            .build()
                            .map_err(|e| BackendError::Backend(e.to_string()))?,
                    )
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .delete(
                        aws_sdk_dynamodb::types::Delete::builder()
                            .table_name(&self.snapshots_table)
                            .key("PK", AttributeValue::S(sig_pk(instance_id)))
                            .key(
                                "SK",
                                AttributeValue::S(SignalKind::Cancel.as_ref().to_string()),
                            )
                            .build()
                            .map_err(|e| BackendError::Backend(e.to_string()))?,
                    )
                    .build(),
            )
            .send()
            .await
            .map_err(sdk_err)?;

        tracing::info!(instance_id, "workflow cancelled");
        Ok(true)
    }

    #[allow(clippy::too_many_lines)]
    async fn check_and_pause(&self, instance_id: &str) -> Result<bool, BackendError> {
        tracing::debug!(instance_id, "checking for pause signal");

        let Some(request) = self.get_signal(instance_id, SignalKind::Pause).await? else {
            return Ok(false);
        };

        let mut snapshot = self.load_snapshot(instance_id).await?;
        let version = self.get_current_version(instance_id).await?;

        if !snapshot.state.is_in_progress() {
            return Ok(false);
        }

        let reason = request.reason;
        let requested_by = request.requested_by;
        let pause_request = PauseRequest::new(reason, requested_by);
        snapshot.mark_paused(&pause_request);

        let data = self.encode(&snapshot)?;
        let status = status_str(&snapshot.state).to_string();
        let task_id = current_task_id(&snapshot).map(ToString::to_string);
        let task_count = completed_task_count(&snapshot);
        let pos_kind = position_kind(&snapshot).map(ToString::to_string);
        let wake_at = delay_wake_at(&snapshot);
        let now = chrono::Utc::now().to_rfc3339();
        let new_version = version + 1;

        let mut snap_item = std::collections::HashMap::new();
        snap_item.insert("PK".to_string(), AttributeValue::S(snap_pk(instance_id)));
        snap_item.insert("SK".to_string(), AttributeValue::S("current".to_string()));
        snap_item.insert(
            "data".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(data)),
        );
        snap_item.insert(
            "version".to_string(),
            AttributeValue::N(new_version.to_string()),
        );
        snap_item.insert("status".to_string(), AttributeValue::S(status));
        snap_item.insert(
            "definition_hash".to_string(),
            AttributeValue::S(snapshot.definition_hash.clone()),
        );
        snap_item.insert("updated_at".to_string(), AttributeValue::S(now));
        snap_item.insert(
            "instance_id".to_string(),
            AttributeValue::S(instance_id.to_string()),
        );
        snap_item.insert(
            "completed_task_count".to_string(),
            AttributeValue::N(task_count.to_string()),
        );
        if let Some(ref tid) = task_id {
            snap_item.insert(
                "current_task_id".to_string(),
                AttributeValue::S(tid.clone()),
            );
        }
        if let Some(ref pk) = pos_kind {
            snap_item.insert("position_kind".to_string(), AttributeValue::S(pk.clone()));
        }
        if let Some(ref wa) = wake_at {
            snap_item.insert(
                "delay_wake_at".to_string(),
                AttributeValue::S(wa.to_rfc3339()),
            );
        }

        self.client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(&self.snapshots_table)
                            .set_item(Some(snap_item))
                            .condition_expression("version = :expected_version")
                            .expression_attribute_values(
                                ":expected_version",
                                AttributeValue::N(version.to_string()),
                            )
                            .build()
                            .map_err(|e| BackendError::Backend(e.to_string()))?,
                    )
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .delete(
                        aws_sdk_dynamodb::types::Delete::builder()
                            .table_name(&self.snapshots_table)
                            .key("PK", AttributeValue::S(sig_pk(instance_id)))
                            .key(
                                "SK",
                                AttributeValue::S(SignalKind::Pause.as_ref().to_string()),
                            )
                            .build()
                            .map_err(|e| BackendError::Backend(e.to_string()))?,
                    )
                    .build(),
            )
            .send()
            .await
            .map_err(sdk_err)?;

        tracing::info!(instance_id, "workflow paused");
        Ok(true)
    }

    async fn unpause(&self, instance_id: &str) -> Result<WorkflowSnapshot, BackendError> {
        tracing::debug!(instance_id, "unpausing workflow");

        let mut snapshot = self.load_snapshot(instance_id).await?;
        let version = self.get_current_version(instance_id).await?;

        if !snapshot.state.is_paused() {
            let state_name = status_str(&snapshot.state);
            return Err(BackendError::CannotPause(format!(
                "Workflow is not paused (current state: {state_name:?})"
            )));
        }

        snapshot.mark_unpaused();

        let data = self.encode(&snapshot)?;
        let status = status_str(&snapshot.state).to_string();
        let task_id = current_task_id(&snapshot).map(ToString::to_string);
        let task_count = completed_task_count(&snapshot);
        let pos_kind = position_kind(&snapshot).map(ToString::to_string);
        let wake_at = delay_wake_at(&snapshot);
        let now = chrono::Utc::now().to_rfc3339();
        let new_version = version + 1;

        let mut snap_item = std::collections::HashMap::new();
        snap_item.insert("PK".to_string(), AttributeValue::S(snap_pk(instance_id)));
        snap_item.insert("SK".to_string(), AttributeValue::S("current".to_string()));
        snap_item.insert(
            "data".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(data)),
        );
        snap_item.insert(
            "version".to_string(),
            AttributeValue::N(new_version.to_string()),
        );
        snap_item.insert("status".to_string(), AttributeValue::S(status));
        snap_item.insert(
            "definition_hash".to_string(),
            AttributeValue::S(snapshot.definition_hash.clone()),
        );
        snap_item.insert("updated_at".to_string(), AttributeValue::S(now));
        snap_item.insert(
            "instance_id".to_string(),
            AttributeValue::S(instance_id.to_string()),
        );
        snap_item.insert(
            "completed_task_count".to_string(),
            AttributeValue::N(task_count.to_string()),
        );
        if let Some(ref tid) = task_id {
            snap_item.insert(
                "current_task_id".to_string(),
                AttributeValue::S(tid.clone()),
            );
        }
        if let Some(ref pk) = pos_kind {
            snap_item.insert("position_kind".to_string(), AttributeValue::S(pk.clone()));
        }
        if let Some(ref wa) = wake_at {
            snap_item.insert(
                "delay_wake_at".to_string(),
                AttributeValue::S(wa.to_rfc3339()),
            );
        }

        self.client
            .put_item()
            .table_name(&self.snapshots_table)
            .set_item(Some(snap_item))
            .condition_expression("version = :expected_version")
            .expression_attribute_values(
                ":expected_version",
                AttributeValue::N(version.to_string()),
            )
            .send()
            .await
            .map_err(sdk_err)?;

        tracing::info!(instance_id, "workflow unpaused");
        Ok(snapshot)
    }
}

/// Validate that a signal can be sent to a workflow in the given state.
fn validate_signal_allowed(status: &str, kind: SignalKind) -> Result<(), BackendError> {
    use std::str::FromStr;

    let Ok(status) = SnapshotStatus::from_str(status) else {
        return Ok(());
    };

    match kind {
        SignalKind::Cancel => match status {
            SnapshotStatus::Completed | SnapshotStatus::Failed => {
                Err(BackendError::CannotCancel(status.as_ref().to_string()))
            }
            _ => Ok(()),
        },
        SignalKind::Pause => match status {
            SnapshotStatus::Completed | SnapshotStatus::Failed | SnapshotStatus::Cancelled => {
                Err(BackendError::CannotPause(status.as_ref().to_string()))
            }
            _ => Ok(()),
        },
    }
}
