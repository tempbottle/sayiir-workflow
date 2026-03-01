//! [`SignalStore`] implementation for Cloudflare D1.
//!
//! Implements the 5 primitive methods. Composite methods (`check_and_cancel`,
//! `check_and_pause`, `unpause`) use the default implementations from
//! `sayiir-persistence` — safe because a single Cloudflare Worker has no
//! concurrent access to the same D1 instance.

use sayiir_core::snapshot::{SignalKind, SignalRequest};
use sayiir_persistence::{BackendError, SignalStore};
use wasm_bindgen::JsValue;

use crate::backend::D1Backend;
use crate::bindings::{get_blob_col, get_str_col};
use crate::error::D1Error;
use crate::js_future::JsFutureExt as _;
use sayiir_persistence::validation::validate_signal_allowed;

impl SignalStore for D1Backend {
    async fn store_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
        request: SignalRequest,
    ) -> Result<(), BackendError> {
        // Validate workflow state first.
        let status_sql = "SELECT status FROM sayiir_workflow_snapshots WHERE instance_id = ?1";

        let status_args = js_sys::Array::new();
        status_args.push(&JsValue::from_str(instance_id));

        let stmt = self.db().prepare(status_sql).bind(&status_args);
        let result = stmt.first().into_send_future().await.map_err(D1Error)?;

        if result.is_null() || result.is_undefined() {
            return Err(BackendError::NotFound(instance_id.to_string()));
        }

        let status = get_str_col(&result, "status")
            .ok_or_else(|| BackendError::Backend("missing status column".to_string()))?;
        validate_signal_allowed(&status, kind)?;

        // Upsert the signal.
        let sql = "INSERT INTO sayiir_workflow_signals (instance_id, kind, reason, requested_by)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (instance_id, kind) DO UPDATE SET
                reason = ?3, requested_by = ?4,
                created_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')";

        let args = js_sys::Array::new();
        args.push(&JsValue::from_str(instance_id));
        args.push(&JsValue::from_str(kind.as_ref()));
        args.push(&match &request.reason {
            Some(r) => JsValue::from_str(r),
            None => JsValue::NULL,
        });
        args.push(&match &request.requested_by {
            Some(r) => JsValue::from_str(r),
            None => JsValue::NULL,
        });

        let stmt = self.db().prepare(sql).bind(&args);
        stmt.run().into_send_future().await.map_err(D1Error)?;

        Ok(())
    }

    async fn get_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
    ) -> Result<Option<SignalRequest>, BackendError> {
        let sql = "SELECT reason, requested_by, created_at
             FROM sayiir_workflow_signals
             WHERE instance_id = ?1 AND kind = ?2";

        let args = js_sys::Array::new();
        args.push(&JsValue::from_str(instance_id));
        args.push(&JsValue::from_str(kind.as_ref()));

        let stmt = self.db().prepare(sql).bind(&args);
        let result = stmt.first().into_send_future().await.map_err(D1Error)?;

        if result.is_null() || result.is_undefined() {
            return Ok(None);
        }

        let reason = get_str_col(&result, "reason");
        let requested_by = get_str_col(&result, "requested_by");
        let created_at_str = get_str_col(&result, "created_at")
            .ok_or_else(|| BackendError::Backend("missing created_at column".to_string()))?;

        let requested_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now());

        Ok(Some(SignalRequest {
            reason,
            requested_by,
            requested_at,
        }))
    }

    async fn clear_signal(&self, instance_id: &str, kind: SignalKind) -> Result<(), BackendError> {
        let sql = "DELETE FROM sayiir_workflow_signals WHERE instance_id = ?1 AND kind = ?2";

        let args = js_sys::Array::new();
        args.push(&JsValue::from_str(instance_id));
        args.push(&JsValue::from_str(kind.as_ref()));

        let stmt = self.db().prepare(sql).bind(&args);
        stmt.run().into_send_future().await.map_err(D1Error)?;

        Ok(())
    }

    async fn send_event(
        &self,
        instance_id: &str,
        signal_name: &str,
        payload: bytes::Bytes,
    ) -> Result<(), BackendError> {
        let sql = "INSERT INTO sayiir_workflow_events (instance_id, signal_name, payload)
             VALUES (?1, ?2, ?3)";

        let args = js_sys::Array::new();
        args.push(&JsValue::from_str(instance_id));
        args.push(&JsValue::from_str(signal_name));
        args.push(&js_sys::Uint8Array::from(payload.as_ref()).into());

        let stmt = self.db().prepare(sql).bind(&args);
        stmt.run().into_send_future().await.map_err(D1Error)?;

        Ok(())
    }

    async fn consume_event(
        &self,
        instance_id: &str,
        signal_name: &str,
    ) -> Result<Option<bytes::Bytes>, BackendError> {
        // but single-Worker access means we can safely do SELECT then DELETE.

        // 1. Find the oldest event.
        let select_sql = "SELECT id, payload FROM sayiir_workflow_events
             WHERE instance_id = ?1 AND signal_name = ?2
             ORDER BY id ASC LIMIT 1";

        let args = js_sys::Array::new();
        args.push(&JsValue::from_str(instance_id));
        args.push(&JsValue::from_str(signal_name));

        let stmt = self.db().prepare(select_sql).bind(&args);
        let result = stmt.first().into_send_future().await.map_err(D1Error)?;

        if result.is_null() || result.is_undefined() {
            return Ok(None);
        }

        let payload = get_blob_col(&result, "payload")
            .ok_or_else(|| BackendError::Backend("missing payload column".to_string()))?;

        let id = js_sys::Reflect::get(&result, &JsValue::from_str("id"))
            .map_err(D1Error)?
            .as_f64()
            .ok_or_else(|| BackendError::Backend("missing id column".to_string()))?;

        let delete_sql = "DELETE FROM sayiir_workflow_events WHERE id = ?1";

        let delete_args = js_sys::Array::new();
        delete_args.push(&JsValue::from_f64(id));

        let stmt = self.db().prepare(delete_sql).bind(&delete_args);
        stmt.run().into_send_future().await.map_err(D1Error)?;

        Ok(Some(bytes::Bytes::from(payload)))
    }
}
