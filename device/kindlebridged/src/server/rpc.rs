//! Bounded Device RPC dispatch.

use kindlebridge_schema::device_protocol::{DeviceAppInstallParams, DeviceCall, DeviceReply};
use kindlebridge_schema::device_rpc::{self as rpc_method, RpcMethod};
use kindlebridge_schema::{
    error_codes, AppList, AppLogParams, AppLogSnapshot, AppSummary, AppTargetParams, ExecParams,
    ExecResult, LogSnapshot, LogTailParams, ProcessList, ProcessSignalParams, ProcessSummary,
    RpcError, SerialParams, SyncListParams, SyncListResult, SyncMkdirParams, SyncMkdirResult,
    SyncStatus, SyncStatusParams,
};
use serde::Serialize;

use crate::exec::{self, ExecError};
use crate::services;
use crate::sync::{StoreError, SyncStore};

use super::ServerConfig;

pub(super) fn dispatch(
    call: DeviceCall,
    config: &ServerConfig,
    sync_store: &SyncStore,
) -> DeviceReply {
    match call.method.as_str() {
        method if method == rpc_method::SyncStatus::METHOD => {
            dispatch_rpc::<rpc_method::SyncStatus>(
                call.params,
                "expected serial and transfer_id",
                |params| dispatch_sync_status(params, config, sync_store),
            )
        }
        method if method == rpc_method::SyncList::METHOD => dispatch_rpc::<rpc_method::SyncList>(
            call.params,
            "expected serial, remote_path, cursor, and limit",
            |params| dispatch_sync_list(params, config, sync_store),
        ),
        method if method == rpc_method::SyncMkdir::METHOD => dispatch_rpc::<rpc_method::SyncMkdir>(
            call.params,
            "expected serial and remote_path",
            |params| dispatch_sync_mkdir(params, config, sync_store),
        ),
        method if method == rpc_method::ExecRun::METHOD => dispatch_rpc::<rpc_method::ExecRun>(
            call.params,
            "expected serial, argv, cwd, environment, and timeout_ms",
            |params| dispatch_exec(params, config),
        ),
        method if method == rpc_method::AppList::METHOD => {
            dispatch_rpc::<rpc_method::AppList>(call.params, "expected serial", |params| {
                dispatch_app_list(params, config)
            })
        }
        method if method == rpc_method::ProcessList::METHOD => {
            dispatch_rpc::<rpc_method::ProcessList>(call.params, "expected serial", |params| {
                dispatch_process_list(params, config)
            })
        }
        method if method == rpc_method::LogTail::METHOD => dispatch_rpc::<rpc_method::LogTail>(
            call.params,
            "expected serial, cursor, and limit",
            |params| dispatch_log_tail(params, config),
        ),
        method if method == rpc_method::AppInstall::METHOD => {
            dispatch_rpc::<rpc_method::AppInstall>(
                call.params,
                "expected serial, remote_path, and file_hash",
                |params| dispatch_app_install(params, config, sync_store),
            )
        }
        method if method == rpc_method::AppStart::METHOD => dispatch_rpc::<rpc_method::AppStart>(
            call.params,
            "expected serial and app_id",
            |params| dispatch_app_start(params, config),
        ),
        method if method == rpc_method::AppLog::METHOD => dispatch_rpc::<rpc_method::AppLog>(
            call.params,
            "expected serial, app_id, run_id, cursors, and max_bytes",
            |params| dispatch_app_log(params, config),
        ),
        method if method == rpc_method::AppStop::METHOD => dispatch_rpc::<rpc_method::AppStop>(
            call.params,
            "expected serial and app_id",
            |params| dispatch_app_stop(params, config),
        ),
        method if method == rpc_method::AppRestart::METHOD => {
            dispatch_rpc::<rpc_method::AppRestart>(
                call.params,
                "expected serial and app_id",
                |params| dispatch_app_restart(params, config),
            )
        }
        method if method == rpc_method::AppRollback::METHOD => {
            dispatch_rpc::<rpc_method::AppRollback>(
                call.params,
                "expected serial and app_id",
                |params| dispatch_app_rollback(params, config),
            )
        }
        method if method == rpc_method::AppUninstall::METHOD => {
            dispatch_rpc::<rpc_method::AppUninstall>(
                call.params,
                "expected serial and app_id",
                |params| dispatch_app_uninstall(params, config),
            )
        }
        method if method == rpc_method::ProcessSignal::METHOD => {
            dispatch_rpc::<rpc_method::ProcessSignal>(
                call.params,
                "expected serial, pid, and signal",
                |params| dispatch_process_signal(params, config),
            )
        }
        _ => DeviceReply::failure(RpcError::method_not_found(&call.method)),
    }
}

fn dispatch_rpc<M: RpcMethod>(
    params: serde_json::Value,
    detail: &'static str,
    handler: impl FnOnce(M::Params) -> Result<M::Result, RpcError>,
) -> DeviceReply {
    reply(decode_params::<M::Params>(params, detail).and_then(handler))
}

fn reply<T: Serialize>(result: Result<T, RpcError>) -> DeviceReply {
    match result
        .and_then(|value| serde_json::to_value(value).map_err(|_| RpcError::internal_error()))
    {
        Ok(value) => DeviceReply::success(value),
        Err(error) => DeviceReply::failure(error),
    }
}

fn dispatch_sync_status(
    params: SyncStatusParams,
    config: &ServerConfig,
    sync_store: &SyncStore,
) -> Result<SyncStatus, RpcError> {
    require_serial(&params.serial, config)?;
    sync_store
        .status(&params.transfer_id)
        .map_err(StoreError::into_rpc)
}

fn dispatch_sync_list(
    params: SyncListParams,
    config: &ServerConfig,
    sync_store: &SyncStore,
) -> Result<SyncListResult, RpcError> {
    require_serial(&params.serial, config)?;
    sync_store
        .list_directory(&params.remote_path, params.cursor.as_deref(), params.limit)
        .map_err(StoreError::into_rpc)
}

fn dispatch_sync_mkdir(
    params: SyncMkdirParams,
    config: &ServerConfig,
    sync_store: &SyncStore,
) -> Result<SyncMkdirResult, RpcError> {
    require_serial(&params.serial, config)?;
    sync_store
        .create_directory(&params.remote_path)
        .map_err(StoreError::into_rpc)
}

fn dispatch_exec(params: ExecParams, config: &ServerConfig) -> Result<ExecResult, RpcError> {
    require_serial(&params.serial, config)?;
    match exec::run(&params) {
        Ok(result) => Ok(result),
        Err(ExecError::EmptyArgv | ExecError::InvalidTimeout) => {
            Err(RpcError::invalid_params("invalid exec bounds"))
        }
        Err(ExecError::Timeout(timeout)) => Err(RpcError::new(
            error_codes::EXEC_TIMEOUT,
            "Command timed out",
        )
        .with_data(serde_json::json!({ "timeout_ms": timeout }))),
        Err(ExecError::OutputLimit) => Err(RpcError::new(
            error_codes::EXEC_OUTPUT_LIMIT,
            "Command output exceeds the device limit",
        )),
        Err(_) => Err(RpcError::new(
            error_codes::EXEC_FAILED,
            "Command could not be executed",
        )),
    }
}

fn dispatch_app_list(params: SerialParams, config: &ServerConfig) -> Result<AppList, RpcError> {
    require_serial(&params.serial, config)?;
    config.applications.list()
}

fn dispatch_process_list(
    params: SerialParams,
    config: &ServerConfig,
) -> Result<ProcessList, RpcError> {
    require_serial(&params.serial, config)?;
    let mut list = services::process_list(&config.proc_root)?;
    config.applications.annotate_processes(&mut list)?;
    Ok(list)
}

fn dispatch_process_signal(
    params: ProcessSignalParams,
    config: &ServerConfig,
) -> Result<ProcessSummary, RpcError> {
    require_serial(&params.serial, config)?;
    config
        .applications
        .reject_managed_process_signal(params.pid)?;
    services::process_signal(&config.proc_root, params.pid, &params.signal)
}

fn dispatch_log_tail(
    params: LogTailParams,
    config: &ServerConfig,
) -> Result<LogSnapshot, RpcError> {
    require_serial(&params.serial, config)?;
    services::log_tail(&config.log_path, &params)
}

fn dispatch_app_install(
    params: DeviceAppInstallParams,
    config: &ServerConfig,
    sync_store: &SyncStore,
) -> Result<AppSummary, RpcError> {
    require_serial(&params.serial, config)?;
    let mut bundle = sync_store
        .open_committed(&params.remote_path)
        .map_err(StoreError::into_rpc)?;
    config.applications.install(&mut bundle, &params.file_hash)
}

fn dispatch_app_start(
    params: AppTargetParams,
    config: &ServerConfig,
) -> Result<AppSummary, RpcError> {
    require_serial(&params.serial, config)?;
    config.applications.start(&params.app_id)
}

fn dispatch_app_log(
    params: AppLogParams,
    config: &ServerConfig,
) -> Result<AppLogSnapshot, RpcError> {
    require_serial(&params.serial, config)?;
    config.applications.log(&params)
}

fn dispatch_app_stop(
    params: AppTargetParams,
    config: &ServerConfig,
) -> Result<AppSummary, RpcError> {
    require_serial(&params.serial, config)?;
    config.applications.stop(&params.app_id)
}

fn dispatch_app_restart(
    params: AppTargetParams,
    config: &ServerConfig,
) -> Result<AppSummary, RpcError> {
    require_serial(&params.serial, config)?;
    config.applications.restart(&params.app_id)
}

fn dispatch_app_rollback(
    params: AppTargetParams,
    config: &ServerConfig,
) -> Result<AppSummary, RpcError> {
    require_serial(&params.serial, config)?;
    config.applications.rollback(&params.app_id)
}

fn dispatch_app_uninstall(
    params: AppTargetParams,
    config: &ServerConfig,
) -> Result<AppSummary, RpcError> {
    require_serial(&params.serial, config)?;
    config.applications.uninstall(&params.app_id)
}

fn decode_params<T: serde::de::DeserializeOwned>(
    params: serde_json::Value,
    detail: &'static str,
) -> Result<T, RpcError> {
    serde_json::from_value(params).map_err(|_| RpcError::invalid_params(detail))
}

fn require_serial(serial: &str, config: &ServerConfig) -> Result<(), RpcError> {
    if serial == config.device.serial {
        Ok(())
    } else {
        Err(RpcError::device_not_found(serial))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use kindlebridge_schema::methods;

    use super::*;
    use crate::DeviceInfo;

    #[test]
    fn dispatch_exec_preserves_typed_result() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-LINK"));
        let executable = std::env::current_exe().unwrap();
        let params = ExecParams {
            serial: "KT6-LINK".to_owned(),
            argv: vec![
                executable.to_string_lossy().into_owned(),
                "--list".to_owned(),
            ],
            cwd: None,
            environment: BTreeMap::new(),
            timeout_ms: 10_000,
        };
        let reply = dispatch(
            DeviceCall {
                method: methods::EXEC_RUN.to_owned(),
                params: serde_json::to_value(params).unwrap(),
            },
            &config,
            &SyncStore::new(std::env::temp_dir().join("kindlebridge-dispatch-test")),
        );
        let result = reply.into_result().unwrap();
        assert_eq!(result["exit_code"], 0);
        assert!(result["stdout"]
            .as_str()
            .unwrap()
            .contains("dispatch_exec_preserves_typed_result"));
    }

    #[test]
    fn wrong_serial_is_a_stable_device_error() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-LINK"));
        let reply = dispatch(
            DeviceCall {
                method: methods::EXEC_RUN.to_owned(),
                params: serde_json::json!({
                    "serial": "OTHER",
                    "argv": ["unused"],
                    "environment": {},
                    "timeout_ms": 1
                }),
            },
            &config,
            &SyncStore::new(std::env::temp_dir().join("kindlebridge-dispatch-test")),
        );
        assert_eq!(
            reply.into_result().unwrap_err().code,
            error_codes::DEVICE_NOT_FOUND
        );
    }

    #[test]
    fn malformed_typed_params_keep_the_existing_detail() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-LINK"));
        let reply = dispatch(
            DeviceCall {
                method: methods::EXEC_RUN.to_owned(),
                params: serde_json::json!({"serial": "KT6-LINK"}),
            },
            &config,
            &SyncStore::new(std::env::temp_dir().join("kindlebridge-dispatch-test")),
        );

        assert_eq!(
            reply.into_result().unwrap_err(),
            RpcError::invalid_params("expected serial, argv, cwd, environment, and timeout_ms")
        );
    }

    #[test]
    fn unknown_methods_keep_the_existing_method_not_found_error() {
        let config = ServerConfig::new(DeviceInfo::kt6("KT6-LINK"));
        let reply = dispatch(
            DeviceCall {
                method: "v1.unknown".to_owned(),
                params: serde_json::Value::Null,
            },
            &config,
            &SyncStore::new(std::env::temp_dir().join("kindlebridge-dispatch-test")),
        );

        assert_eq!(
            reply.into_result().unwrap_err(),
            RpcError::method_not_found("v1.unknown")
        );
    }
}
