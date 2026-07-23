//! Typed definitions for request-response methods on the host JSON-RPC link.

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::methods;

pub trait RpcMethod {
    type Params: Serialize + DeserializeOwned;
    type Result: Serialize + DeserializeOwned;

    const METHOD: &'static str;
}

macro_rules! rpc_method {
    ($name:ident, $method:expr, $params:ty, $result:ty) => {
        pub struct $name;

        impl RpcMethod for $name {
            type Params = $params;
            type Result = $result;

            const METHOD: &'static str = $method;
        }
    };
}

rpc_method!(
    ServerPing,
    methods::SERVER_PING,
    crate::EmptyParams,
    crate::PingResult
);
rpc_method!(
    ServerVersion,
    methods::SERVER_VERSION,
    crate::EmptyParams,
    crate::ServerVersion
);
rpc_method!(
    ServerStatus,
    methods::SERVER_STATUS,
    crate::EmptyParams,
    crate::ServerStatus
);
rpc_method!(
    ServerStop,
    methods::SERVER_STOP,
    crate::EmptyParams,
    crate::ServerStopResult
);
rpc_method!(
    DeviceList,
    methods::DEVICE_LIST,
    crate::EmptyParams,
    crate::DeviceList
);
rpc_method!(
    DeviceFeatures,
    methods::DEVICE_FEATURES,
    crate::DeviceFeaturesParams,
    crate::DeviceFeatures
);
rpc_method!(
    DevicePing,
    methods::DEVICE_PING,
    crate::SerialParams,
    crate::PingResult
);
rpc_method!(
    ExecRun,
    methods::EXEC_RUN,
    crate::ExecParams,
    crate::ExecResult
);
rpc_method!(
    SyncPush,
    methods::SYNC_PUSH,
    crate::SyncPushParams,
    crate::SyncPushResult
);
rpc_method!(
    SyncPull,
    methods::SYNC_PULL,
    crate::SyncPullParams,
    crate::SyncPullResult
);
rpc_method!(
    SyncStatus,
    methods::SYNC_STATUS,
    crate::SyncStatusParams,
    crate::SyncStatus
);
rpc_method!(
    SyncList,
    methods::SYNC_LIST,
    crate::SyncListParams,
    crate::SyncListResult
);
rpc_method!(
    SyncMkdir,
    methods::SYNC_MKDIR,
    crate::SyncMkdirParams,
    crate::SyncMkdirResult
);
rpc_method!(
    AppInstall,
    methods::APP_INSTALL,
    crate::AppInstallParams,
    crate::AppSummary
);
rpc_method!(
    AppStart,
    methods::APP_START,
    crate::AppTargetParams,
    crate::AppSummary
);
rpc_method!(
    AppStop,
    methods::APP_STOP,
    crate::AppTargetParams,
    crate::AppSummary
);
rpc_method!(
    AppRestart,
    methods::APP_RESTART,
    crate::AppTargetParams,
    crate::AppSummary
);
rpc_method!(
    AppRollback,
    methods::APP_ROLLBACK,
    crate::AppTargetParams,
    crate::AppSummary
);
rpc_method!(
    AppUninstall,
    methods::APP_UNINSTALL,
    crate::AppTargetParams,
    crate::AppSummary
);
rpc_method!(
    AppList,
    methods::APP_LIST,
    crate::SerialParams,
    crate::AppList
);
rpc_method!(
    AppLog,
    methods::APP_LOG,
    crate::AppLogParams,
    crate::AppLogSnapshot
);
rpc_method!(
    ProcessList,
    methods::PROCESS_LIST,
    crate::SerialParams,
    crate::ProcessList
);
rpc_method!(
    ProcessSignal,
    methods::PROCESS_SIGNAL,
    crate::ProcessSignalParams,
    crate::ProcessSummary
);
rpc_method!(
    LogTail,
    methods::LOG_TAIL,
    crate::LogTailParams,
    crate::LogSnapshot
);

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn request_response_method_names_are_unique() {
        let methods = [
            ServerPing::METHOD,
            ServerVersion::METHOD,
            ServerStatus::METHOD,
            ServerStop::METHOD,
            DeviceList::METHOD,
            DeviceFeatures::METHOD,
            DevicePing::METHOD,
            ExecRun::METHOD,
            SyncPush::METHOD,
            SyncPull::METHOD,
            SyncStatus::METHOD,
            SyncList::METHOD,
            SyncMkdir::METHOD,
            AppInstall::METHOD,
            AppStart::METHOD,
            AppStop::METHOD,
            AppRestart::METHOD,
            AppRollback::METHOD,
            AppUninstall::METHOD,
            AppList::METHOD,
            AppLog::METHOD,
            ProcessList::METHOD,
            ProcessSignal::METHOD,
            LogTail::METHOD,
        ];

        assert_eq!(
            methods.iter().copied().collect::<BTreeSet<_>>().len(),
            methods.len()
        );
    }

    #[test]
    fn exec_definition_keeps_method_params_and_result_together() {
        fn assert_types<M: RpcMethod<Params = crate::ExecParams, Result = crate::ExecResult>>() {}
        assert_types::<ExecRun>();
        assert_eq!(ExecRun::METHOD, methods::EXEC_RUN);
    }

    #[test]
    fn status_result_preserves_the_existing_json_shape() {
        assert_eq!(
            serde_json::to_value(crate::ServerStatus {
                running: true,
                pid: 42,
            })
            .unwrap(),
            serde_json::json!({"running": true, "pid": 42})
        );
    }
}
