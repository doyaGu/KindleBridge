//! Typed definitions for unary RPC methods carried by `rpc.v1`.

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::device_protocol::{
    DeviceAppInstallParams, APP_INSTALL_FEATURE, APP_LIST_FEATURE, APP_LOG_FEATURE,
    APP_RESTART_FEATURE, APP_ROLLBACK_FEATURE, APP_START_FEATURE, APP_STOP_FEATURE,
    APP_UNINSTALL_FEATURE, EXEC_FEATURE, LOG_TAIL_FEATURE, PROCESS_LIST_FEATURE,
    PROCESS_SIGNAL_FEATURE, SYNC_FEATURE, SYNC_TREE_FEATURE,
};
use crate::{
    methods, AppList as AppListResult, AppLogParams, AppLogSnapshot, AppSummary, AppTargetParams,
    ExecParams, ExecResult, LogSnapshot, LogTailParams, ProcessList as ProcessListResult,
    ProcessSignalParams, ProcessSummary, SerialParams, SyncListParams, SyncListResult,
    SyncMkdirParams, SyncMkdirResult, SyncStatus as SyncStatusResult, SyncStatusParams,
};

pub trait RpcMethod {
    type Params: Serialize + DeserializeOwned;
    type Result: Serialize + DeserializeOwned;

    const METHOD: &'static str;
    const FEATURE: &'static str;
}

macro_rules! rpc_method {
    ($name:ident, $method:expr, $feature:expr, $params:ty, $result:ty) => {
        pub enum $name {}

        impl RpcMethod for $name {
            type Params = $params;
            type Result = $result;

            const METHOD: &'static str = $method;
            const FEATURE: &'static str = $feature;
        }
    };
}

rpc_method!(
    ExecRun,
    methods::EXEC_RUN,
    EXEC_FEATURE,
    ExecParams,
    ExecResult
);
rpc_method!(
    SyncStatus,
    methods::SYNC_STATUS,
    SYNC_FEATURE,
    SyncStatusParams,
    SyncStatusResult
);
rpc_method!(
    SyncList,
    methods::SYNC_LIST,
    SYNC_TREE_FEATURE,
    SyncListParams,
    SyncListResult
);
rpc_method!(
    SyncMkdir,
    methods::SYNC_MKDIR,
    SYNC_TREE_FEATURE,
    SyncMkdirParams,
    SyncMkdirResult
);
rpc_method!(
    AppInstall,
    methods::APP_INSTALL,
    APP_INSTALL_FEATURE,
    DeviceAppInstallParams,
    AppSummary
);
rpc_method!(
    AppStart,
    methods::APP_START,
    APP_START_FEATURE,
    AppTargetParams,
    AppSummary
);
rpc_method!(
    AppStop,
    methods::APP_STOP,
    APP_STOP_FEATURE,
    AppTargetParams,
    AppSummary
);
rpc_method!(
    AppRestart,
    methods::APP_RESTART,
    APP_RESTART_FEATURE,
    AppTargetParams,
    AppSummary
);
rpc_method!(
    AppRollback,
    methods::APP_ROLLBACK,
    APP_ROLLBACK_FEATURE,
    AppTargetParams,
    AppSummary
);
rpc_method!(
    AppUninstall,
    methods::APP_UNINSTALL,
    APP_UNINSTALL_FEATURE,
    AppTargetParams,
    AppSummary
);
rpc_method!(
    AppList,
    methods::APP_LIST,
    APP_LIST_FEATURE,
    SerialParams,
    AppListResult
);
rpc_method!(
    AppLog,
    methods::APP_LOG,
    APP_LOG_FEATURE,
    AppLogParams,
    AppLogSnapshot
);
rpc_method!(
    ProcessList,
    methods::PROCESS_LIST,
    PROCESS_LIST_FEATURE,
    SerialParams,
    ProcessListResult
);
rpc_method!(
    ProcessSignal,
    methods::PROCESS_SIGNAL,
    PROCESS_SIGNAL_FEATURE,
    ProcessSignalParams,
    ProcessSummary
);
rpc_method!(
    LogTail,
    methods::LOG_TAIL,
    LOG_TAIL_FEATURE,
    LogTailParams,
    LogSnapshot
);

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::device_protocol::DeviceCall;

    use super::*;

    #[test]
    fn app_start_definition_keeps_method_feature_and_types_together() {
        assert_eq!(AppStart::METHOD, methods::APP_START);
        assert_eq!(AppStart::FEATURE, APP_START_FEATURE);

        fn assert_types<M: RpcMethod<Params = AppTargetParams, Result = AppSummary>>() {}
        assert_types::<AppStart>();
    }

    #[test]
    fn unary_method_names_are_unique_and_app_log_keeps_its_v2_feature() {
        let definitions = [
            (ExecRun::METHOD, ExecRun::FEATURE),
            (SyncStatus::METHOD, SyncStatus::FEATURE),
            (SyncList::METHOD, SyncList::FEATURE),
            (SyncMkdir::METHOD, SyncMkdir::FEATURE),
            (AppInstall::METHOD, AppInstall::FEATURE),
            (AppStart::METHOD, AppStart::FEATURE),
            (AppStop::METHOD, AppStop::FEATURE),
            (AppRestart::METHOD, AppRestart::FEATURE),
            (AppRollback::METHOD, AppRollback::FEATURE),
            (AppUninstall::METHOD, AppUninstall::FEATURE),
            (AppList::METHOD, AppList::FEATURE),
            (AppLog::METHOD, AppLog::FEATURE),
            (ProcessList::METHOD, ProcessList::FEATURE),
            (ProcessSignal::METHOD, ProcessSignal::FEATURE),
            (LogTail::METHOD, LogTail::FEATURE),
        ];
        let methods: BTreeSet<_> = definitions.iter().map(|(method, _)| *method).collect();

        assert_eq!(methods.len(), definitions.len());
        assert_eq!(AppLog::METHOD, methods::APP_LOG);
        assert_eq!(AppLog::FEATURE, APP_LOG_FEATURE);
    }

    #[test]
    fn typed_definition_preserves_the_existing_device_call_shape() {
        let params = AppTargetParams {
            serial: "KT6-TEST".to_owned(),
            app_id: "org.example.reader".to_owned(),
        };
        let call = DeviceCall {
            method: AppStart::METHOD.to_owned(),
            params: serde_json::to_value(&params).unwrap(),
        };

        assert_eq!(
            serde_json::to_value(call).unwrap(),
            serde_json::json!({
                "method": "v1.app.start",
                "params": {
                    "serial": "KT6-TEST",
                    "app_id": "org.example.reader"
                }
            })
        );
    }
}
