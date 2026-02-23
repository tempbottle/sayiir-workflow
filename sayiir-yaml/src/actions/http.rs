use bytes::Bytes;
use sayiir_core::error::BoxError;
use sayiir_core::task::{BytesFuture, CoreTask};
use serde_json::{Value, json};
use std::collections::HashMap;

pub struct HttpAction {
    pub method: String,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub timeout_secs: Option<u64>,
}

impl CoreTask for HttpAction {
    type Input = Bytes;
    type Output = Bytes;
    type Future = BytesFuture;

    fn run(&self, input: Bytes) -> Self::Future {
        let method = self.method.clone();
        let url = self.url.clone();
        let headers = self.headers.clone();
        let timeout_secs = self.timeout_secs;

        BytesFuture::new(async move {
            let client = reqwest::Client::new();

            let method = method
                .parse::<reqwest::Method>()
                .map_err(|e| -> BoxError { format!("invalid HTTP method: {e}").into() })?;

            let mut builder = client.request(method, &url);

            for (key, value) in &headers {
                builder = builder.header(key.as_str(), value.as_str());
            }

            if let Some(timeout) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs(timeout));
            }

            // Set body from input if non-null
            let body: Value = if input.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&input).unwrap_or(Value::Null)
            };
            if !body.is_null() {
                builder = builder.json(&body);
            }

            let response = builder
                .send()
                .await
                .map_err(|e| -> BoxError { format!("HTTP request failed: {e}").into() })?;

            let status = response.status().as_u16();
            let resp_headers: HashMap<String, String> = response
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
            let body_text = response
                .text()
                .await
                .map_err(|e| -> BoxError { format!("failed to read response body: {e}").into() })?;

            let body_value: Value =
                serde_json::from_str(&body_text).unwrap_or(Value::String(body_text));

            let result = json!({
                "status": status,
                "headers": resp_headers,
                "body": body_value,
            });

            Ok(Bytes::from(serde_json::to_vec(&result)?))
        })
    }
}
