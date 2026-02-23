//! [`SnapshotStore`] implementation for DynamoDB.

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use sayiir_core::codec::{self, Decoder, Encoder};
use sayiir_core::snapshot::WorkflowSnapshot;
use sayiir_persistence::{BackendError, SnapshotStore};

use crate::backend::DynamoDbBackend;
use crate::error::sdk_err;
use crate::helpers::{
    completed_task_count, current_task_id, delay_wake_at, error_message, is_terminal,
    position_kind, status_str,
};

/// Build the partition key for a snapshot item: `SNAP#{instance_id}`.
pub(crate) fn snap_pk(instance_id: &str) -> String {
    format!("SNAP#{instance_id}")
}

/// Build the partition key for a history item: `HIST#{instance_id}`.
fn hist_pk(instance_id: &str) -> String {
    format!("HIST#{instance_id}")
}

impl<C> SnapshotStore for DynamoDbBackend<C>
where
    C: Encoder
        + Decoder
        + codec::sealed::EncodeValue<WorkflowSnapshot>
        + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    #[allow(clippy::too_many_lines)]
    async fn save_snapshot(&self, snapshot: &WorkflowSnapshot) -> Result<(), BackendError> {
        tracing::debug!(
            instance_id = %snapshot.instance_id,
            status = %status_str(&snapshot.state),
            "saving snapshot"
        );

        let data = self.encode(snapshot)?;
        let status = status_str(&snapshot.state).to_string();
        let task_id = current_task_id(snapshot).map(ToString::to_string);
        let task_count = completed_task_count(snapshot);
        let error = error_message(snapshot).map(ToString::to_string);
        let terminal = is_terminal(snapshot);
        let pos_kind = position_kind(snapshot).map(ToString::to_string);
        let wake_at = delay_wake_at(snapshot);
        let now = chrono::Utc::now().to_rfc3339();

        // First: get the current version for history numbering
        let version = self.get_next_version(&snapshot.instance_id).await?;

        // Build the snapshot item
        let mut snap_item = std::collections::HashMap::new();
        snap_item.insert(
            "PK".to_string(),
            AttributeValue::S(snap_pk(&snapshot.instance_id)),
        );
        snap_item.insert("SK".to_string(), AttributeValue::S("current".to_string()));
        snap_item.insert(
            "data".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(data.clone())),
        );
        snap_item.insert(
            "version".to_string(),
            AttributeValue::N(version.to_string()),
        );
        snap_item.insert("status".to_string(), AttributeValue::S(status.clone()));
        snap_item.insert(
            "definition_hash".to_string(),
            AttributeValue::S(snapshot.definition_hash.clone()),
        );
        snap_item.insert("updated_at".to_string(), AttributeValue::S(now.clone()));
        snap_item.insert(
            "instance_id".to_string(),
            AttributeValue::S(snapshot.instance_id.clone()),
        );

        if let Some(ref tid) = task_id {
            snap_item.insert(
                "current_task_id".to_string(),
                AttributeValue::S(tid.clone()),
            );
        }
        snap_item.insert(
            "completed_task_count".to_string(),
            AttributeValue::N(task_count.to_string()),
        );
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
        if terminal {
            snap_item.insert("completed_at".to_string(), AttributeValue::S(now.clone()));
        }

        // Build the history item
        let mut hist_item = std::collections::HashMap::new();
        hist_item.insert(
            "PK".to_string(),
            AttributeValue::S(hist_pk(&snapshot.instance_id)),
        );
        hist_item.insert(
            "SK".to_string(),
            AttributeValue::S(format!("v{version:08}")),
        );
        hist_item.insert(
            "data".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(data)),
        );
        hist_item.insert("status".to_string(), AttributeValue::S(status));
        hist_item.insert("created_at".to_string(), AttributeValue::S(now));
        if let Some(ref tid) = task_id {
            hist_item.insert(
                "current_task_id".to_string(),
                AttributeValue::S(tid.clone()),
            );
        }

        // Transact: upsert snapshot + append history
        self.client
            .transact_write_items()
            .transact_items(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(&self.snapshots_table)
                            .set_item(Some(snap_item))
                            .build()
                            .map_err(|e| BackendError::Backend(e.to_string()))?,
                    )
                    .build(),
            )
            .transact_items(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(&self.snapshots_table)
                            .set_item(Some(hist_item))
                            .build()
                            .map_err(|e| BackendError::Backend(e.to_string()))?,
                    )
                    .build(),
            )
            .send()
            .await
            .map_err(sdk_err)?;

        tracing::debug!(instance_id = %snapshot.instance_id, "snapshot saved");
        Ok(())
    }

    async fn save_task_result(
        &self,
        instance_id: &str,
        task_id: &str,
        output: bytes::Bytes,
    ) -> Result<(), BackendError> {
        tracing::debug!(instance_id, task_id, "saving task result");

        // Load current snapshot
        let mut snapshot = self.load_snapshot(instance_id).await?;
        let old_version = self.get_current_version(instance_id).await?;

        snapshot.mark_task_completed(task_id.to_string(), output);

        let data = self.encode(&snapshot)?;
        let status = status_str(&snapshot.state).to_string();
        let current = current_task_id(&snapshot).map(ToString::to_string);
        let task_count = completed_task_count(&snapshot);
        let now = chrono::Utc::now().to_rfc3339();
        let new_version = old_version + 1;

        let mut item = std::collections::HashMap::new();
        item.insert("PK".to_string(), AttributeValue::S(snap_pk(instance_id)));
        item.insert("SK".to_string(), AttributeValue::S("current".to_string()));
        item.insert(
            "data".to_string(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(data)),
        );
        item.insert(
            "version".to_string(),
            AttributeValue::N(new_version.to_string()),
        );
        item.insert("status".to_string(), AttributeValue::S(status));
        item.insert(
            "definition_hash".to_string(),
            AttributeValue::S(snapshot.definition_hash.clone()),
        );
        item.insert("updated_at".to_string(), AttributeValue::S(now));
        item.insert(
            "instance_id".to_string(),
            AttributeValue::S(instance_id.to_string()),
        );
        item.insert(
            "completed_task_count".to_string(),
            AttributeValue::N(task_count.to_string()),
        );
        if let Some(ref tid) = current {
            item.insert(
                "current_task_id".to_string(),
                AttributeValue::S(tid.clone()),
            );
        }

        // Conditional put with version check for optimistic locking
        self.client
            .put_item()
            .table_name(&self.snapshots_table)
            .set_item(Some(item))
            .condition_expression("version = :expected_version")
            .expression_attribute_values(
                ":expected_version",
                AttributeValue::N(old_version.to_string()),
            )
            .send()
            .await
            .map_err(sdk_err)?;

        tracing::debug!(instance_id, task_id, "task result saved");
        Ok(())
    }

    async fn load_snapshot(&self, instance_id: &str) -> Result<WorkflowSnapshot, BackendError> {
        tracing::debug!(instance_id, "loading snapshot");

        let result = self
            .client
            .get_item()
            .table_name(&self.snapshots_table)
            .key("PK", AttributeValue::S(snap_pk(instance_id)))
            .key("SK", AttributeValue::S("current".to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(sdk_err)?;

        let item = result
            .item()
            .ok_or_else(|| BackendError::NotFound(instance_id.to_string()))?;

        let data = item
            .get("data")
            .and_then(|v| v.as_b().ok())
            .ok_or_else(|| BackendError::Backend("missing data attribute".to_string()))?;

        self.decode(data.as_ref())
    }

    async fn delete_snapshot(&self, instance_id: &str) -> Result<(), BackendError> {
        tracing::debug!(instance_id, "deleting snapshot");

        // Delete with condition to ensure it exists
        self.client
            .delete_item()
            .table_name(&self.snapshots_table)
            .key("PK", AttributeValue::S(snap_pk(instance_id)))
            .key("SK", AttributeValue::S("current".to_string()))
            .condition_expression("attribute_exists(PK)")
            .send()
            .await
            .map_err(|e| {
                let service_err = e.into_service_error();
                if service_err.is_conditional_check_failed_exception() {
                    BackendError::NotFound(instance_id.to_string())
                } else {
                    BackendError::Backend(service_err.to_string())
                }
            })?;

        Ok(())
    }

    async fn list_snapshots(&self) -> Result<Vec<String>, BackendError> {
        tracing::debug!("listing snapshots");

        let mut instance_ids = Vec::new();
        let mut exclusive_start_key = None;

        loop {
            let mut scan = self
                .client
                .scan()
                .table_name(&self.snapshots_table)
                .filter_expression("SK = :current AND begins_with(PK, :snap_prefix)")
                .expression_attribute_values(":current", AttributeValue::S("current".to_string()))
                .expression_attribute_values(
                    ":snap_prefix",
                    AttributeValue::S("SNAP#".to_string()),
                );

            if let Some(key) = exclusive_start_key {
                scan = scan.set_exclusive_start_key(Some(key));
            }

            let result = scan.send().await.map_err(sdk_err)?;

            for item in result.items() {
                if let Some(id) = item.get("instance_id").and_then(|v| v.as_s().ok()) {
                    instance_ids.push(id.clone());
                }
            }

            match result.last_evaluated_key() {
                Some(key) if !key.is_empty() => {
                    exclusive_start_key = Some(key.clone());
                }
                _ => break,
            }
        }

        Ok(instance_ids)
    }
}

impl<C> DynamoDbBackend<C> {
    /// Get the current version number for a snapshot, or 0 if it doesn't exist.
    pub(crate) async fn get_current_version(&self, instance_id: &str) -> Result<u64, BackendError> {
        let result = self
            .client
            .get_item()
            .table_name(&self.snapshots_table)
            .key("PK", AttributeValue::S(snap_pk(instance_id)))
            .key("SK", AttributeValue::S("current".to_string()))
            .consistent_read(true)
            .projection_expression("version")
            .send()
            .await
            .map_err(sdk_err)?;

        match result.item() {
            Some(item) => {
                let version = item
                    .get("version")
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0);
                Ok(version)
            }
            None => Ok(0),
        }
    }

    /// Get the next version number (current + 1).
    async fn get_next_version(&self, instance_id: &str) -> Result<u64, BackendError> {
        Ok(self.get_current_version(instance_id).await? + 1)
    }
}
