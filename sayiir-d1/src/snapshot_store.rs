//! [`SnapshotStore`] implementation for Cloudflare D1.

use sayiir_core::snapshot::WorkflowSnapshot;
use sayiir_persistence::{BackendError, SnapshotStore};
use wasm_bindgen::JsValue;

use crate::backend::D1Backend;
use crate::bindings::{get_blob_col, get_str_col};
use crate::error::D1Error;
use crate::helpers::dt_to_sqlite;
use crate::js_future::JsFutureExt as _;

impl SnapshotStore for D1Backend {
    async fn save_snapshot(&self, snapshot: &WorkflowSnapshot) -> Result<(), BackendError> {
        let data = self.encode(snapshot)?;
        let status = snapshot.state.as_ref();
        let task_id = snapshot.current_task_id().map(ToString::to_string);
        let task_count = snapshot.completed_task_count();
        let error = snapshot.error_message().map(ToString::to_string);
        let terminal = snapshot.state.is_terminal();
        let pos_kind = snapshot.position_kind();
        let wake_at = dt_to_sqlite(snapshot.delay_wake_at());

        let now = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";
        let completed_at_expr = if terminal { now } else { "NULL" };

        let upsert_sql = format!(
            "INSERT INTO sayiir_workflow_snapshots
                (instance_id, status, definition_hash, current_task_id,
                 completed_task_count, data, error, position_kind, delay_wake_at,
                 trace_parent, completed_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                     {completed_at_expr}, {now})
             ON CONFLICT (instance_id) DO UPDATE SET
                status = ?2,
                definition_hash = ?3,
                current_task_id = ?4,
                completed_task_count = ?5,
                data = ?6,
                error = ?7,
                position_kind = ?8,
                delay_wake_at = ?9,
                trace_parent = ?10,
                completed_at = CASE WHEN {terminal} THEN {now} ELSE sayiir_workflow_snapshots.completed_at END,
                updated_at = {now}",
            terminal = if terminal { "1" } else { "0" },
        );

        let upsert_args = js_sys::Array::new();
        upsert_args.push(&JsValue::from_str(&snapshot.instance_id)); // ?1
        upsert_args.push(&JsValue::from_str(status)); // ?2
        upsert_args.push(&JsValue::from_str(&snapshot.definition_hash)); // ?3
        upsert_args.push(&match &task_id {
            Some(t) => JsValue::from_str(t),
            None => JsValue::NULL,
        }); // ?4
        upsert_args.push(&JsValue::from_f64(f64::from(task_count))); // ?5
        upsert_args.push(&js_sys::Uint8Array::from(data.as_slice()).into()); // ?6
        upsert_args.push(&match &error {
            Some(e) => JsValue::from_str(e),
            None => JsValue::NULL,
        }); // ?7
        upsert_args.push(&match pos_kind {
            Some(p) => JsValue::from_str(p),
            None => JsValue::NULL,
        }); // ?8
        upsert_args.push(&match &wake_at {
            Some(w) => JsValue::from_str(w),
            None => JsValue::NULL,
        }); // ?9
        upsert_args.push(&match snapshot.trace_parent.as_deref() {
            Some(tp) => JsValue::from_str(tp),
            None => JsValue::NULL,
        }); // ?10

        let upsert_stmt = self.db().prepare(&upsert_sql).bind(&upsert_args);

        let history_sql = "INSERT INTO sayiir_workflow_snapshot_history
                (instance_id, version, status, current_task_id, data)
             VALUES (
                ?1,
                (SELECT COALESCE(MAX(version), 0) + 1
                 FROM sayiir_workflow_snapshot_history WHERE instance_id = ?1),
                ?2, ?3, ?4
             )";

        let history_args = js_sys::Array::new();
        history_args.push(&JsValue::from_str(&snapshot.instance_id));
        history_args.push(&JsValue::from_str(status));
        history_args.push(&match &task_id {
            Some(t) => JsValue::from_str(t),
            None => JsValue::NULL,
        });
        history_args.push(&js_sys::Uint8Array::from(data.as_slice()).into());

        let history_stmt = self.db().prepare(history_sql).bind(&history_args);

        let batch = js_sys::Array::new();
        batch.push(&upsert_stmt);
        batch.push(&history_stmt);

        self.db()
            .batch(&batch)
            .into_send_future()
            .await
            .map_err(D1Error)?;

        Ok(())
    }

    async fn save_task_result(
        &self,
        instance_id: &str,
        task_id: &str,
        output: bytes::Bytes,
    ) -> Result<(), BackendError> {
        // Single-Worker: no concurrency, so sequential load → mutate → save is safe.
        let mut snapshot = self.load_snapshot(instance_id).await?;
        snapshot.mark_task_completed(task_id.to_string(), output);
        self.save_snapshot(&snapshot).await
    }

    async fn load_snapshot(&self, instance_id: &str) -> Result<WorkflowSnapshot, BackendError> {
        let sql = "SELECT data, trace_parent FROM sayiir_workflow_snapshots WHERE instance_id = ?1";

        let args = js_sys::Array::new();
        args.push(&JsValue::from_str(instance_id));

        let stmt = self.db().prepare(sql).bind(&args);
        let result = stmt.first().into_send_future().await.map_err(D1Error)?;

        if result.is_null() || result.is_undefined() {
            return Err(BackendError::NotFound(instance_id.to_string()));
        }

        let data = get_blob_col(&result, "data")
            .ok_or_else(|| BackendError::Backend("missing data column".to_string()))?;
        let mut snapshot = self.decode(&data)?;
        snapshot.trace_parent = get_str_col(&result, "trace_parent");
        Ok(snapshot)
    }

    async fn delete_snapshot(&self, instance_id: &str) -> Result<(), BackendError> {
        let sql = "DELETE FROM sayiir_workflow_snapshots WHERE instance_id = ?1";

        let args = js_sys::Array::new();
        args.push(&JsValue::from_str(instance_id));

        let stmt = self.db().prepare(sql).bind(&args);
        let result = stmt.run().into_send_future().await.map_err(D1Error)?;

        // Check meta.changes to see if a row was actually deleted.
        let meta = js_sys::Reflect::get(&result, &JsValue::from_str("meta")).map_err(D1Error)?;
        let changes = js_sys::Reflect::get(&meta, &JsValue::from_str("changes"))
            .map_err(D1Error)?
            .as_f64()
            .unwrap_or(0.0);

        if changes < 1.0 {
            return Err(BackendError::NotFound(instance_id.to_string()));
        }
        Ok(())
    }

    async fn list_snapshots(&self) -> Result<Vec<String>, BackendError> {
        let sql = "SELECT instance_id FROM sayiir_workflow_snapshots";

        let stmt = self.db().prepare(sql);
        let result = stmt.all().into_send_future().await.map_err(D1Error)?;

        let results =
            js_sys::Reflect::get(&result, &JsValue::from_str("results")).map_err(D1Error)?;
        let array = js_sys::Array::from(&results);

        let mut ids = Vec::with_capacity(array.length() as usize);
        for i in 0..array.length() {
            let row = array.get(i);
            if let Some(id) = get_str_col(&row, "instance_id") {
                ids.push(id);
            }
        }
        Ok(ids)
    }
}
