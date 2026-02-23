use bytes::Bytes;
use sayiir_core::error::BoxError;
use sayiir_core::task::{BytesFuture, CoreTask};
use serde_json::Value;

pub struct LambdaAction {
    pub function_name: String,
    pub region: Option<String>,
    pub qualifier: Option<String>,
}

impl CoreTask for LambdaAction {
    type Input = Bytes;
    type Output = Bytes;
    type Future = BytesFuture;

    fn run(&self, input: Bytes) -> Self::Future {
        let function_name = self.function_name.clone();
        let region = self.region.clone();
        let qualifier = self.qualifier.clone();

        BytesFuture::new(async move {
            let config = if let Some(region) = &region {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(aws_config::Region::new(region.clone()))
                    .load()
                    .await
            } else {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .load()
                    .await
            };

            let client = aws_sdk_lambda::Client::new(&config);

            let payload: Value = if input.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&input).unwrap_or(Value::Null)
            };

            let mut invoke = client.invoke().function_name(&function_name).payload(
                aws_sdk_lambda::primitives::Blob::new(serde_json::to_vec(&payload)?),
            );

            if let Some(q) = &qualifier {
                invoke = invoke.qualifier(q);
            }

            let result = invoke
                .send()
                .await
                .map_err(|e| -> BoxError { format!("Lambda invoke failed: {e}").into() })?;

            let response_payload = result
                .payload()
                .map(|p| p.as_ref().to_vec())
                .unwrap_or_default();

            Ok(Bytes::from(response_payload))
        })
    }
}
