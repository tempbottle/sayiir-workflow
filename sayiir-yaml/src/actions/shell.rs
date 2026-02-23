use bytes::Bytes;
use sayiir_core::error::BoxError;
use sayiir_core::task::{BytesFuture, CoreTask};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

pub struct ShellAction {
    pub command: String,
    pub args: Vec<String>,
}

impl CoreTask for ShellAction {
    type Input = Bytes;
    type Output = Bytes;
    type Future = BytesFuture;

    fn run(&self, input: Bytes) -> Self::Future {
        let command = self.command.clone();
        let args = self.args.clone();

        BytesFuture::new(async move {
            let stdin_data: Option<String> = if input.is_empty() {
                None
            } else {
                serde_json::from_slice::<Value>(&input)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
            };

            let mut cmd = tokio::process::Command::new(&command);
            cmd.args(&args);

            if stdin_data.is_some() {
                cmd.stdin(std::process::Stdio::piped());
            }
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::piped());

            let mut child: tokio::process::Child = cmd.spawn().map_err(|e| -> BoxError {
                format!("failed to spawn command '{command}': {e}").into()
            })?;

            if let Some(data) = &stdin_data
                && let Some(mut stdin) = child.stdin.take()
            {
                stdin
                    .write_all(data.as_bytes())
                    .await
                    .map_err(|e| -> BoxError { format!("failed to write stdin: {e}").into() })?;
                drop(stdin);
            }

            let output = child
                .wait_with_output()
                .await
                .map_err(|e| -> BoxError { format!("failed to wait for command: {e}").into() })?;

            let result = json!({
                "exit_code": output.status.code().unwrap_or(-1),
                "stdout": String::from_utf8_lossy(&output.stdout).into_owned(),
                "stderr": String::from_utf8_lossy(&output.stderr).into_owned(),
            });

            Ok(Bytes::from(serde_json::to_vec(&result)?))
        })
    }
}
