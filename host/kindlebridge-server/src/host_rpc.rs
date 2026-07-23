//! Bounded request/reply dispatch on the local JSON-RPC link.

use std::sync::atomic::{AtomicBool, Ordering};

use kindlebridge_schema::host_rpc::{self as rpc_method, RpcMethod};
use kindlebridge_schema::{
    DeviceList, EmptyParams, PingResult, RpcError, RpcRequest, RpcResponse, ServerStatus,
    ServerStopResult, ServerVersion,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use super::{DeviceProvider, DeviceRegistry};

static SERVER_STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Dispatches a validated Host RPC. Notifications are executed but have no response.
#[must_use]
pub(super) fn handle(request: RpcRequest, provider: &dyn DeviceProvider) -> Option<RpcResponse> {
    let result = dispatch(&request, provider);
    request.id.map(|id| match result {
        Ok(value) => RpcResponse::success(id, value),
        Err(error) => RpcResponse::failure(id, error),
    })
}

pub(super) fn handle_registry(
    request: RpcRequest,
    registry: &DeviceRegistry,
) -> Option<RpcResponse> {
    let result = registry.rpc(|provider| dispatch(&request, provider));
    request.id.map(|id| match result {
        Ok(value) => RpcResponse::success(id, value),
        Err(error) => RpcResponse::failure(id, error),
    })
}

#[must_use]
pub fn server_stop_requested() -> bool {
    SERVER_STOP_REQUESTED.load(Ordering::Acquire)
}

pub fn reset_server_stop_requested() {
    SERVER_STOP_REQUESTED.store(false, Ordering::Release);
}

fn dispatch(request: &RpcRequest, provider: &dyn DeviceProvider) -> Result<Value, RpcError> {
    match request.method.as_str() {
        method if method == rpc_method::ServerPing::METHOD => {
            require_empty_method::<rpc_method::ServerPing>(request)?;
            to_method_value::<rpc_method::ServerPing>(PingResult { ok: true })
        }
        method if method == rpc_method::ServerVersion::METHOD => {
            require_empty_method::<rpc_method::ServerVersion>(request)?;
            to_method_value::<rpc_method::ServerVersion>(ServerVersion {
                name: "kindlebridge-server".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                api_version: kindlebridge_schema::API_VERSION.to_owned(),
            })
        }
        method if method == rpc_method::ServerStatus::METHOD => {
            require_empty_method::<rpc_method::ServerStatus>(request)?;
            to_method_value::<rpc_method::ServerStatus>(ServerStatus {
                running: true,
                pid: std::process::id(),
            })
        }
        method if method == rpc_method::ServerStop::METHOD => {
            require_empty_method::<rpc_method::ServerStop>(request)?;
            SERVER_STOP_REQUESTED.store(true, Ordering::Release);
            to_method_value::<rpc_method::ServerStop>(ServerStopResult { stopping: true })
        }
        method if method == rpc_method::DeviceList::METHOD => {
            require_empty_method::<rpc_method::DeviceList>(request)?;
            let devices = provider.list()?;
            to_method_value::<rpc_method::DeviceList>(DeviceList { devices })
        }
        method if method == rpc_method::DeviceFeatures::METHOD => {
            let params = parse_method_params::<rpc_method::DeviceFeatures>(
                request,
                "expected { serial: string }",
            )?;
            if params.serial.is_empty() {
                return Err(RpcError::invalid_params("serial must not be empty"));
            }
            let features = provider
                .features(&params.serial)?
                .ok_or_else(|| RpcError::device_not_found(&params.serial))?;
            to_method_value::<rpc_method::DeviceFeatures>(features)
        }
        method if method == rpc_method::DevicePing::METHOD => {
            let params =
                parse_method_params::<rpc_method::DevicePing>(request, "device ping params")?;
            if params.serial.is_empty() {
                return Err(RpcError::invalid_params("serial must not be empty"));
            }
            if !provider.ping(&params.serial)? {
                return Err(RpcError::device_not_found(&params.serial));
            }
            to_method_value::<rpc_method::DevicePing>(PingResult { ok: true })
        }
        method if method == rpc_method::ExecRun::METHOD => {
            let params = parse_method_params::<rpc_method::ExecRun>(
                request,
                "expected serial, non-empty argv, cwd, environment, timeout_ms",
            )?;
            if params.serial.is_empty() || params.argv.is_empty() || params.timeout_ms == 0 {
                return Err(RpcError::invalid_params(
                    "serial and argv must be non-empty; timeout_ms must be positive",
                ));
            }
            let features = provider
                .features(&params.serial)?
                .ok_or_else(|| RpcError::device_not_found(&params.serial))?;
            if !features.features.iter().any(|feature| feature == "exec.v1") {
                return Err(RpcError::feature_unavailable(&params.serial, "exec.v1"));
            }
            let result = provider
                .exec(&params)?
                .ok_or_else(|| RpcError::device_not_found(&params.serial))?;
            to_method_value::<rpc_method::ExecRun>(result)
        }
        method if method == rpc_method::SyncPush::METHOD => {
            let params = parse_method_params::<rpc_method::SyncPush>(request, "sync push params")?;
            to_method_value::<rpc_method::SyncPush>(provider.sync_push(params)?)
        }
        method if method == rpc_method::SyncPull::METHOD => {
            let params = parse_method_params::<rpc_method::SyncPull>(request, "sync pull params")?;
            to_method_value::<rpc_method::SyncPull>(provider.sync_pull(params)?)
        }
        method if method == rpc_method::SyncStatus::METHOD => {
            let params =
                parse_method_params::<rpc_method::SyncStatus>(request, "sync status params")?;
            to_method_value::<rpc_method::SyncStatus>(provider.sync_status(&params)?)
        }
        method if method == rpc_method::SyncList::METHOD => {
            let params = parse_method_params::<rpc_method::SyncList>(request, "sync list params")?;
            to_method_value::<rpc_method::SyncList>(provider.sync_list(&params)?)
        }
        method if method == rpc_method::SyncMkdir::METHOD => {
            let params =
                parse_method_params::<rpc_method::SyncMkdir>(request, "sync mkdir params")?;
            to_method_value::<rpc_method::SyncMkdir>(provider.sync_mkdir(&params)?)
        }
        method if method == rpc_method::AppInstall::METHOD => {
            let params =
                parse_method_params::<rpc_method::AppInstall>(request, "app install params")?;
            to_method_value::<rpc_method::AppInstall>(provider.app_install(params)?)
        }
        method if method == rpc_method::AppStart::METHOD => {
            let params = parse_method_params::<rpc_method::AppStart>(request, "app target params")?;
            to_method_value::<rpc_method::AppStart>(provider.app_start(&params)?)
        }
        method if method == rpc_method::AppStop::METHOD => {
            let params = parse_method_params::<rpc_method::AppStop>(request, "app target params")?;
            to_method_value::<rpc_method::AppStop>(provider.app_stop(&params)?)
        }
        method if method == rpc_method::AppRestart::METHOD => {
            let params =
                parse_method_params::<rpc_method::AppRestart>(request, "app target params")?;
            to_method_value::<rpc_method::AppRestart>(provider.app_restart(&params)?)
        }
        method if method == rpc_method::AppRollback::METHOD => {
            let params =
                parse_method_params::<rpc_method::AppRollback>(request, "app target params")?;
            to_method_value::<rpc_method::AppRollback>(provider.app_rollback(&params)?)
        }
        method if method == rpc_method::AppUninstall::METHOD => {
            let params =
                parse_method_params::<rpc_method::AppUninstall>(request, "app target params")?;
            to_method_value::<rpc_method::AppUninstall>(provider.app_uninstall(&params)?)
        }
        method if method == rpc_method::AppList::METHOD => {
            let params = parse_method_params::<rpc_method::AppList>(request, "serial params")?;
            to_method_value::<rpc_method::AppList>(provider.app_list(&params)?)
        }
        method if method == rpc_method::AppLog::METHOD => {
            let params = parse_method_params::<rpc_method::AppLog>(request, "app log params")?;
            to_method_value::<rpc_method::AppLog>(provider.app_log(&params)?)
        }
        method if method == rpc_method::ProcessList::METHOD => {
            let params = parse_method_params::<rpc_method::ProcessList>(request, "serial params")?;
            to_method_value::<rpc_method::ProcessList>(provider.process_list(&params)?)
        }
        method if method == rpc_method::ProcessSignal::METHOD => {
            let params =
                parse_method_params::<rpc_method::ProcessSignal>(request, "process signal params")?;
            to_method_value::<rpc_method::ProcessSignal>(provider.process_signal(&params)?)
        }
        method if method == rpc_method::LogTail::METHOD => {
            let params = parse_method_params::<rpc_method::LogTail>(request, "log tail params")?;
            to_method_value::<rpc_method::LogTail>(provider.log_tail(&params)?)
        }
        _ => Err(RpcError::method_not_found(&request.method)),
    }
}

fn parse_params<T: DeserializeOwned>(request: &RpcRequest, expected: &str) -> Result<T, RpcError> {
    let value = request
        .params
        .clone()
        .ok_or_else(|| RpcError::invalid_params("missing params object"))?;
    serde_json::from_value(value).map_err(|_| RpcError::invalid_params(expected))
}

fn parse_method_params<M: RpcMethod>(
    request: &RpcRequest,
    expected: &str,
) -> Result<M::Params, RpcError> {
    parse_params(request, expected)
}

fn to_value<T: Serialize>(value: T) -> Result<Value, RpcError> {
    serde_json::to_value(value).map_err(|_| RpcError::internal_error())
}

fn to_method_value<M: RpcMethod>(value: M::Result) -> Result<Value, RpcError> {
    to_value(value)
}

fn require_empty_method<M>(request: &RpcRequest) -> Result<M::Params, RpcError>
where
    M: RpcMethod<Params = EmptyParams>,
{
    require_empty_params(request.params.as_ref())?;
    Ok(EmptyParams {})
}

fn require_empty_params(params: Option<&Value>) -> Result<(), RpcError> {
    match params {
        None => Ok(()),
        Some(Value::Object(object)) if object.is_empty() => Ok(()),
        Some(Value::Array(array)) if array.is_empty() => Ok(()),
        Some(_) => Err(RpcError::invalid_params("method takes no params")),
    }
}

#[cfg(test)]
mod tests {
    use kindlebridge_schema::{methods, DeviceState, DeviceSummary, RequestId};
    use serde_json::json;

    use super::*;
    use crate::{DeviceRecord, MemoryDeviceProvider};

    fn provider() -> MemoryDeviceProvider {
        MemoryDeviceProvider::new(vec![DeviceRecord {
            summary: DeviceSummary {
                serial: "KT6-TEST".to_owned(),
                model: "KT6".to_owned(),
                state: DeviceState::Online,
                transport: "usb".to_owned(),
            },
            protocol_version: kindlebridge_schema::device_protocol::PROTOCOL_VERSION,
            features: vec!["sync.v1".to_owned(), "exec.v1".to_owned()],
        }])
    }

    fn call(method: &str, params: Option<Value>) -> RpcResponse {
        handle(
            RpcRequest::call(RequestId::Number(1), method, params),
            &provider(),
        )
        .expect("calls have responses")
    }

    #[test]
    fn malformed_typed_params_preserve_the_public_error() {
        let response = call(methods::EXEC_RUN, Some(json!({"serial": "KT6-TEST"})));

        assert_eq!(
            response.error,
            Some(RpcError::invalid_params(
                "expected serial, non-empty argv, cwd, environment, timeout_ms",
            ))
        );
    }

    #[test]
    fn unknown_method_preserves_method_not_found() {
        let response = call("v1.unknown", None);

        assert_eq!(
            response.error,
            Some(RpcError::method_not_found("v1.unknown"))
        );
    }

    #[test]
    fn notifications_execute_without_a_response() {
        reset_server_stop_requested();
        let response = handle(
            RpcRequest::notification(methods::SERVER_STOP, None),
            &provider(),
        );

        assert!(response.is_none());
        assert!(server_stop_requested());
        reset_server_stop_requested();
    }
}
