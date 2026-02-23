//! `DynamoDbBackend` struct and constructors.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};
use sayiir_core::codec::{self, Decoder, Encoder};
use sayiir_core::snapshot::WorkflowSnapshot;
use sayiir_persistence::BackendError;

/// DynamoDB persistence backend for Sayiir workflows.
///
/// Generic over a [`Codec`](sayiir_core::codec::Codec) that determines how
/// snapshots are serialized into the Binary attribute. Use `JsonCodec` for
/// human-readable storage, or a binary codec for faster (de)serialization.
///
/// # Table layout
///
/// Three tables are created with the prefix you supply:
///
/// - `{prefix}_snapshots` — snapshots, history, signals
/// - `{prefix}_events` — FIFO external-event queue
/// - `{prefix}_claims` — distributed task claims with TTL
///
/// # Example (with `sayiir-runtime` JSON codec)
///
/// ```rust,no_run
/// use sayiir_dynamodb::DynamoDbBackend;
/// use sayiir_runtime::serialization::JsonCodec;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
/// let backend = DynamoDbBackend::<JsonCodec>::new(&config, "myapp").await?;
/// # Ok(())
/// # }
/// ```
pub struct DynamoDbBackend<C> {
    pub(crate) client: Client,
    pub(crate) codec: C,
    pub(crate) snapshots_table: String,
    pub(crate) events_table: String,
    pub(crate) claims_table: String,
}

/// Name of the GSI on the snapshots table used by `find_available_tasks`.
pub(crate) const STATUS_UPDATED_INDEX: &str = "status-updated-index";

impl<C> DynamoDbBackend<C>
where
    C: Default,
{
    /// Create a new DynamoDB backend, creating tables if they don't exist.
    ///
    /// # Arguments
    ///
    /// * `config` — AWS SDK config (region, credentials, endpoint override, etc.)
    /// * `prefix` — Table name prefix (e.g. `"sayiir"` → `sayiir_snapshots`, etc.)
    ///
    /// # Errors
    ///
    /// Returns an error if table creation fails.
    pub async fn new(config: &aws_config::SdkConfig, prefix: &str) -> Result<Self, BackendError> {
        let client = Client::new(config);
        let backend = Self {
            client,
            codec: C::default(),
            snapshots_table: format!("{prefix}_snapshots"),
            events_table: format!("{prefix}_events"),
            claims_table: format!("{prefix}_claims"),
        };
        backend.ensure_tables().await?;
        tracing::info!(prefix, "DynamoDB backend ready");
        Ok(backend)
    }

    /// Create a new DynamoDB backend from an existing client.
    ///
    /// # Errors
    ///
    /// Returns an error if table creation fails.
    pub async fn with_client(client: Client, prefix: &str) -> Result<Self, BackendError> {
        let backend = Self {
            client,
            codec: C::default(),
            snapshots_table: format!("{prefix}_snapshots"),
            events_table: format!("{prefix}_events"),
            claims_table: format!("{prefix}_claims"),
        };
        backend.ensure_tables().await?;
        tracing::info!(prefix, "DynamoDB backend ready");
        Ok(backend)
    }

    /// Create all required DynamoDB tables if they don't already exist.
    async fn ensure_tables(&self) -> Result<(), BackendError> {
        self.ensure_snapshots_table().await?;
        self.ensure_events_table().await?;
        self.ensure_claims_table().await?;
        Ok(())
    }

    /// Snapshots table: PK (S) + SK (S) + GSI on (status, updated_at).
    async fn ensure_snapshots_table(&self) -> Result<(), BackendError> {
        if self.table_exists(&self.snapshots_table).await? {
            return Ok(());
        }
        tracing::info!(table = %self.snapshots_table, "creating snapshots table");

        self.client
            .create_table()
            .table_name(&self.snapshots_table)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("PK")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("SK")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("PK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("SK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("status")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("updated_at")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name(STATUS_UPDATED_INDEX)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("status")
                            .key_type(KeyType::Hash)
                            .build()
                            .map_err(|e| BackendError::Backend(e.to_string()))?,
                    )
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("updated_at")
                            .key_type(KeyType::Range)
                            .build()
                            .map_err(|e| BackendError::Backend(e.to_string()))?,
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .map_err(|e| BackendError::Backend(format!("create snapshots table: {e}")))?;

        self.wait_for_table(&self.snapshots_table).await?;
        Ok(())
    }

    /// Events table: PK (S) + SK (S).
    async fn ensure_events_table(&self) -> Result<(), BackendError> {
        if self.table_exists(&self.events_table).await? {
            return Ok(());
        }
        tracing::info!(table = %self.events_table, "creating events table");

        self.client
            .create_table()
            .table_name(&self.events_table)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("PK")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("SK")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("PK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("SK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .map_err(|e| BackendError::Backend(format!("create events table: {e}")))?;

        self.wait_for_table(&self.events_table).await?;
        Ok(())
    }

    /// Claims table: PK (S), with TTL on `expires_at_epoch`.
    async fn ensure_claims_table(&self) -> Result<(), BackendError> {
        if self.table_exists(&self.claims_table).await? {
            return Ok(());
        }
        tracing::info!(table = %self.claims_table, "creating claims table");

        self.client
            .create_table()
            .table_name(&self.claims_table)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("PK")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("PK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .map_err(|e| BackendError::Backend(format!("create claims table: {e}")))?;

        self.wait_for_table(&self.claims_table).await?;

        // Enable TTL on expires_at_epoch
        let _ = self
            .client
            .update_time_to_live()
            .table_name(&self.claims_table)
            .time_to_live_specification(
                aws_sdk_dynamodb::types::TimeToLiveSpecification::builder()
                    .enabled(true)
                    .attribute_name("expires_at_epoch")
                    .build()
                    .map_err(|e| BackendError::Backend(e.to_string()))?,
            )
            .send()
            .await;
        // Ignore TTL errors — LocalStack may not support it fully

        Ok(())
    }

    /// Check if a table already exists.
    async fn table_exists(&self, table_name: &str) -> Result<bool, BackendError> {
        match self
            .client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_resource_not_found_exception() {
                    Ok(false)
                } else {
                    Err(BackendError::Backend(format!(
                        "describe table {table_name}: {service_err}"
                    )))
                }
            }
        }
    }

    /// Wait until a table transitions to ACTIVE status.
    async fn wait_for_table(&self, table_name: &str) -> Result<(), BackendError> {
        for _ in 0..60 {
            let resp = self
                .client
                .describe_table()
                .table_name(table_name)
                .send()
                .await
                .map_err(|e| BackendError::Backend(format!("describe table: {e}")))?;

            if let Some(table) = resp.table()
                && table.table_status() == Some(&aws_sdk_dynamodb::types::TableStatus::Active)
            {
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        Err(BackendError::Backend(format!(
            "table {table_name} did not become active"
        )))
    }
}

impl<C> DynamoDbBackend<C>
where
    C: Encoder + codec::sealed::EncodeValue<WorkflowSnapshot>,
{
    /// Encode a snapshot using the configured codec.
    pub(crate) fn encode(&self, snapshot: &WorkflowSnapshot) -> Result<Vec<u8>, BackendError> {
        self.codec
            .encode(snapshot)
            .map(|b| b.to_vec())
            .map_err(|e| BackendError::Serialization(e.to_string()))
    }
}

impl<C> DynamoDbBackend<C>
where
    C: Decoder + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    /// Decode a snapshot from raw bytes using the configured codec.
    pub(crate) fn decode(&self, data: &[u8]) -> Result<WorkflowSnapshot, BackendError> {
        self.codec
            .decode(bytes::Bytes::copy_from_slice(data))
            .map_err(|e| BackendError::Serialization(e.to_string()))
    }
}
