//! [`TaskClaimStore`] implementation for DynamoDB.

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{Duration, Utc};
use sayiir_core::codec::{self, Decoder, Encoder};
use sayiir_core::snapshot::{ExecutionPosition, WorkflowSnapshot, WorkflowSnapshotState};
use sayiir_core::task_claim::{AvailableTask, TaskClaim};
use sayiir_persistence::{BackendError, SnapshotStore, TaskClaimStore};

use crate::backend::{DynamoDbBackend, STATUS_UPDATED_INDEX};
use crate::error::sdk_err;

/// Build the partition key for a claim item: `{instance_id}#{task_id}`.
fn claim_pk(instance_id: &str, task_id: &str) -> String {
    format!("{instance_id}#{task_id}")
}

/// Build the partition key for a signal item: `SIG#{instance_id}`.
fn sig_pk(instance_id: &str) -> String {
    format!("SIG#{instance_id}")
}

impl<C> TaskClaimStore for DynamoDbBackend<C>
where
    C: Encoder
        + Decoder
        + codec::sealed::EncodeValue<WorkflowSnapshot>
        + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    async fn claim_task(
        &self,
        instance_id: &str,
        task_id: &str,
        worker_id: &str,
        ttl: Option<Duration>,
    ) -> Result<Option<TaskClaim>, BackendError> {
        tracing::debug!(instance_id, task_id, worker_id, "claiming task");

        let now = Utc::now();
        let claimed_epoch = now.timestamp().cast_unsigned();
        let expires_at = ttl.and_then(|d| now.checked_add_signed(d));
        let expires_epoch = expires_at.map(|e| e.timestamp().cast_unsigned());

        let mut item = std::collections::HashMap::new();
        item.insert(
            "PK".to_string(),
            AttributeValue::S(claim_pk(instance_id, task_id)),
        );
        item.insert(
            "worker_id".to_string(),
            AttributeValue::S(worker_id.to_string()),
        );
        item.insert(
            "instance_id".to_string(),
            AttributeValue::S(instance_id.to_string()),
        );
        item.insert(
            "task_id".to_string(),
            AttributeValue::S(task_id.to_string()),
        );
        item.insert(
            "claimed_at".to_string(),
            AttributeValue::N(claimed_epoch.to_string()),
        );

        if let Some(exp) = expires_epoch {
            item.insert(
                "expires_at_epoch".to_string(),
                AttributeValue::N(exp.to_string()),
            );
        }

        // Conditional put: succeed only if no claim exists, or existing claim is expired
        let now_epoch = now.timestamp().cast_unsigned();
        let result = self
            .client
            .put_item()
            .table_name(&self.claims_table)
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(PK) OR expires_at_epoch < :now")
            .expression_attribute_values(":now", AttributeValue::N(now_epoch.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(Some(TaskClaim {
                instance_id: instance_id.to_string(),
                task_id: task_id.to_string(),
                worker_id: worker_id.to_string(),
                claimed_at: claimed_epoch,
                expires_at: expires_epoch,
            })),
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_conditional_check_failed_exception() {
                    Ok(None) // Already claimed by someone else
                } else {
                    Err(BackendError::Backend(service_err.to_string()))
                }
            }
        }
    }

    async fn release_task_claim(
        &self,
        instance_id: &str,
        task_id: &str,
        worker_id: &str,
    ) -> Result<(), BackendError> {
        tracing::debug!(instance_id, task_id, worker_id, "releasing task claim");

        // Check ownership first
        let result = self
            .client
            .get_item()
            .table_name(&self.claims_table)
            .key("PK", AttributeValue::S(claim_pk(instance_id, task_id)))
            .consistent_read(true)
            .send()
            .await
            .map_err(sdk_err)?;

        let item = result
            .item()
            .ok_or_else(|| BackendError::NotFound(format!("{instance_id}:{task_id}")))?;

        let owner = item
            .get("worker_id")
            .and_then(|v| v.as_s().ok())
            .ok_or_else(|| BackendError::Backend("missing worker_id".to_string()))?;

        if owner != worker_id {
            return Err(BackendError::Backend(format!(
                "Claim owned by different worker: {owner}"
            )));
        }

        // Delete with worker_id condition
        self.client
            .delete_item()
            .table_name(&self.claims_table)
            .key("PK", AttributeValue::S(claim_pk(instance_id, task_id)))
            .condition_expression("worker_id = :expected")
            .expression_attribute_values(":expected", AttributeValue::S(worker_id.to_string()))
            .send()
            .await
            .map_err(sdk_err)?;

        Ok(())
    }

    async fn extend_task_claim(
        &self,
        instance_id: &str,
        task_id: &str,
        worker_id: &str,
        additional_duration: Duration,
    ) -> Result<(), BackendError> {
        tracing::debug!(instance_id, task_id, worker_id, "extending task claim");

        // Get the current claim
        let result = self
            .client
            .get_item()
            .table_name(&self.claims_table)
            .key("PK", AttributeValue::S(claim_pk(instance_id, task_id)))
            .consistent_read(true)
            .send()
            .await
            .map_err(sdk_err)?;

        let item = result
            .item()
            .ok_or_else(|| BackendError::NotFound(format!("{instance_id}:{task_id}")))?;

        let owner = item
            .get("worker_id")
            .and_then(|v| v.as_s().ok())
            .ok_or_else(|| BackendError::Backend("missing worker_id".to_string()))?;

        if owner != worker_id {
            return Err(BackendError::Backend(format!(
                "Claim owned by different worker: {owner}"
            )));
        }

        // Only extend if there's an expiration set
        if let Some(exp_str) = item.get("expires_at_epoch").and_then(|v| v.as_n().ok()) {
            let current_exp: i64 = exp_str
                .parse()
                .map_err(|_| BackendError::Backend("invalid expires_at_epoch".to_string()))?;

            let new_exp = chrono::DateTime::from_timestamp(current_exp, 0)
                .ok_or_else(|| BackendError::Backend("invalid timestamp".to_string()))?
                .checked_add_signed(additional_duration)
                .ok_or_else(|| BackendError::Backend("Time overflow".to_string()))?;

            self.client
                .update_item()
                .table_name(&self.claims_table)
                .key("PK", AttributeValue::S(claim_pk(instance_id, task_id)))
                .update_expression("SET expires_at_epoch = :new_exp")
                .condition_expression("worker_id = :expected")
                .expression_attribute_values(
                    ":new_exp",
                    AttributeValue::N(new_exp.timestamp().cast_unsigned().to_string()),
                )
                .expression_attribute_values(":expected", AttributeValue::S(worker_id.to_string()))
                .send()
                .await
                .map_err(sdk_err)?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn find_available_tasks(
        &self,
        worker_id: &str,
        limit: usize,
    ) -> Result<Vec<AvailableTask>, BackendError> {
        tracing::debug!(worker_id, limit, "finding available tasks");

        // Query the GSI for status=InProgress snapshots
        let result = self
            .client
            .query()
            .table_name(&self.snapshots_table)
            .index_name(STATUS_UPDATED_INDEX)
            .key_condition_expression("#s = :status")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":status", AttributeValue::S("InProgress".to_string()))
            .send()
            .await
            .map_err(sdk_err)?;

        let items = result.items();
        let mut available = Vec::new();

        for item in items {
            // Only process snapshot items (not signal/history items)
            let pk = item.get("PK").and_then(|v| v.as_s().ok());
            let sk = item.get("SK").and_then(|v| v.as_s().ok());
            if !matches!((pk, sk), (Some(p), Some(s)) if p.starts_with("SNAP#") && s == "current") {
                continue;
            }

            let instance_id = match item.get("instance_id").and_then(|v| v.as_s().ok()) {
                Some(id) => id.clone(),
                None => continue,
            };

            // Check for pending signals — skip if any
            let has_signal = self.has_any_signal(&instance_id).await?;
            if has_signal {
                continue;
            }

            // Load the full snapshot (GSI may not have all attributes with eventually consistent reads)
            let Ok(mut snapshot) = self.load_snapshot(&instance_id).await else {
                continue;
            };

            // Check for active claims on the current task
            let current_task = current_task_id_from_snapshot(&snapshot);
            if let Some(ref tid) = current_task
                && self.has_active_claim(&instance_id, tid).await?
            {
                continue;
            }

            match &snapshot.state {
                // Delay: if expired, advance past it
                WorkflowSnapshotState::InProgress {
                    position:
                        ExecutionPosition::AtDelay {
                            wake_at,
                            next_task_id,
                            delay_id,
                            ..
                        },
                    ..
                } if Utc::now() >= *wake_at => {
                    if let Some(next_id) = next_task_id.clone() {
                        snapshot.update_position(ExecutionPosition::AtTask { task_id: next_id });
                        self.save_snapshot(&snapshot).await?;

                        if let WorkflowSnapshotState::InProgress {
                            position: ExecutionPosition::AtTask { task_id },
                            completed_tasks,
                            ..
                        } = &snapshot.state
                            && let Some(task) =
                                build_available_task(&snapshot, task_id, completed_tasks, worker_id)
                        {
                            available.push(task);
                        }
                    } else {
                        let output = snapshot.get_task_result_bytes(delay_id).unwrap_or_default();
                        snapshot.mark_completed(output);
                        self.save_snapshot(&snapshot).await?;
                    }
                }

                // Task: check retry backoff, then add to available
                WorkflowSnapshotState::InProgress {
                    position: ExecutionPosition::AtTask { task_id },
                    completed_tasks,
                    ..
                } => {
                    if completed_tasks.contains_key(task_id) {
                        continue;
                    }

                    if let Some(rs) = snapshot.task_retries.get(task_id)
                        && Utc::now() < rs.next_retry_at
                    {
                        continue;
                    }

                    if let Some(task) =
                        build_available_task(&snapshot, task_id, completed_tasks, worker_id)
                    {
                        available.push(task);
                    }
                }

                _ => continue,
            }

            if available.len() >= limit {
                break;
            }
        }

        tracing::debug!(worker_id, count = available.len(), "available tasks found");
        Ok(available)
    }
}

impl<C> DynamoDbBackend<C> {
    /// Check if any signal exists for the given instance.
    async fn has_any_signal(&self, instance_id: &str) -> Result<bool, BackendError> {
        let result = self
            .client
            .query()
            .table_name(&self.snapshots_table)
            .key_condition_expression("PK = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(sig_pk(instance_id)))
            .limit(1)
            .send()
            .await
            .map_err(sdk_err)?;

        Ok(!result.items().is_empty())
    }

    /// Check if there is an active (non-expired) claim for the given task.
    async fn has_active_claim(
        &self,
        instance_id: &str,
        task_id: &str,
    ) -> Result<bool, BackendError> {
        let result = self
            .client
            .get_item()
            .table_name(&self.claims_table)
            .key("PK", AttributeValue::S(claim_pk(instance_id, task_id)))
            .consistent_read(true)
            .send()
            .await
            .map_err(sdk_err)?;

        let Some(item) = result.item() else {
            return Ok(false);
        };

        // Check if expired
        if let Some(exp_str) = item.get("expires_at_epoch").and_then(|v| v.as_n().ok())
            && let Ok(exp) = exp_str.parse::<i64>()
            && exp < Utc::now().timestamp()
        {
            return Ok(false); // expired
        }

        Ok(true)
    }
}

/// Extract current task ID from snapshot state.
fn current_task_id_from_snapshot(snapshot: &WorkflowSnapshot) -> Option<String> {
    match &snapshot.state {
        WorkflowSnapshotState::InProgress {
            position: ExecutionPosition::AtTask { task_id },
            ..
        } => Some(task_id.clone()),
        _ => None,
    }
}

/// Build an [`AvailableTask`] from a snapshot at a task position.
fn build_available_task(
    snapshot: &WorkflowSnapshot,
    task_id: &str,
    completed_tasks: &std::collections::HashMap<String, sayiir_core::snapshot::TaskResult>,
    _worker_id: &str,
) -> Option<AvailableTask> {
    let input = if completed_tasks.is_empty() {
        snapshot.initial_input_bytes()
    } else {
        snapshot.get_last_task_output()
    };

    input.map(|input_bytes| AvailableTask {
        instance_id: snapshot.instance_id.clone(),
        task_id: task_id.to_string(),
        input: input_bytes,
        workflow_definition_hash: snapshot.definition_hash.clone(),
    })
}
