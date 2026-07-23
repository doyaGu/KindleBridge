use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Args;
use kindlebridge_bundle::{build_project_bundle, read_project_manifest};
use kindlebridge_schema::{AppInstallParams, AppSummary, AppTargetParams};
use serde_json::json;

use super::{app, call_method, host_rpc, normalize_host_path, CliError, RpcCaller};

#[derive(Clone, Debug, Args)]
pub struct RunArgs {
    /// Stable device serial from `device list`.
    pub serial: String,
    /// Project KBB manifest containing a [development] section.
    #[arg(long, default_value = "kindlebridge.toml")]
    pub manifest: PathBuf,
    /// Rebuild and redeploy when configured watch paths change.
    #[arg(long)]
    pub watch: bool,
}

pub fn run_project_once<C: RpcCaller>(
    caller: &mut C,
    args: &RunArgs,
    json_output: bool,
) -> Result<String, CliError> {
    run_project(caller, args, json_output, true)
}

pub fn deploy_project_after_build<C: RpcCaller>(
    caller: &mut C,
    args: &RunArgs,
    json_output: bool,
) -> Result<String, CliError> {
    run_project(caller, args, json_output, false)
}

fn run_project<C: RpcCaller>(
    caller: &mut C,
    args: &RunArgs,
    json_output: bool,
    execute_build: bool,
) -> Result<String, CliError> {
    let manifest_path = absolute_path(&args.manifest)?;
    let project_root = manifest_path.parent().ok_or_else(|| {
        CliError::Project(format!(
            "manifest has no parent directory: {}",
            manifest_path.display()
        ))
    })?;
    let manifest = read_project_manifest(&manifest_path)
        .map_err(|error| CliError::Project(error.to_string()))?;
    let development = manifest.development.as_ref().ok_or_else(|| {
        CliError::Project(format!(
            "{} is missing [development]",
            manifest_path.display()
        ))
    })?;
    if execute_build {
        if let Some((program, arguments)) = development.build.split_first() {
            run_project_build(program, arguments, project_root, json_output)?;
        }
    }

    let input = resolve_project_path(project_root, &development.input);
    let signing_key = resolve_project_path(project_root, &development.signing_key);
    let development_root = project_root.join(".kindlebridge");
    let output = development_root.join("run.kbb");
    let release = next_development_release(&development_root, manifest.release)?;
    let built = build_project_bundle(&manifest_path, &input, &signing_key, &output, Some(release))
        .map_err(|error| CliError::Project(error.to_string()))?;

    let bundle_path = normalize_host_path(output.to_string_lossy().as_ref())?;
    let (_, installed): (_, AppSummary) = call_method::<_, host_rpc::AppInstall>(
        caller,
        &AppInstallParams {
            serial: args.serial.clone(),
            bundle_path,
        },
        "run install",
    )?;
    let (started_value, started): (_, AppSummary) = call_method::<_, host_rpc::AppStart>(
        caller,
        &AppTargetParams {
            serial: args.serial.clone(),
            app_id: built.id.clone(),
        },
        "run start",
    )?;
    if json_output {
        Ok(json!({
            "bundle": {
                "path": output,
                "bytes": built.bytes,
                "id": built.id,
                "version": built.version,
                "release": built.release,
                "bundle_root": format!("{:?}", built.bundle_root),
            },
            "installed": installed,
            "app": started,
        })
        .to_string())
    } else {
        Ok(format!(
            "built {} {} ({} bytes)\n{}",
            built.id,
            built.version,
            built.bytes,
            app::format_result(started_value, &started, false)?
        ))
    }
}

fn run_project_build(
    program: &str,
    arguments: &[String],
    project_root: &Path,
    json_output: bool,
) -> Result<(), CliError> {
    let mut command = Command::new(program);
    command.args(arguments).current_dir(project_root);
    if json_output {
        let output = command.output().map_err(|error| {
            CliError::Project(format!("could not start build command {program}: {error}"))
        })?;
        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let combined = format!("{stdout}{stderr}").trim().to_owned();
            return Err(CliError::BuildFailed {
                exit_code: output.status.code().unwrap_or(1),
                detail: if combined.is_empty() {
                    String::new()
                } else {
                    format!(": {combined}")
                },
            });
        }
    } else {
        let status = command.status().map_err(|error| {
            CliError::Project(format!("could not start build command {program}: {error}"))
        })?;
        if !status.success() {
            return Err(CliError::BuildFailed {
                exit_code: status.code().unwrap_or(1),
                detail: String::new(),
            });
        }
    }
    Ok(())
}

fn next_development_release(root: &Path, manifest_release: u64) -> Result<u64, CliError> {
    fs::create_dir_all(root).map_err(|error| {
        CliError::Project(format!(
            "could not create development state {}: {error}",
            root.display()
        ))
    })?;
    let state = root.join("run-release");
    let previous = match fs::read_to_string(&state) {
        Ok(value) => value.trim().parse::<u64>().map_err(|_| {
            CliError::Project(format!(
                "development release state is invalid: {}",
                state.display()
            ))
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            return Err(CliError::Project(format!(
                "could not read development release state {}: {error}",
                state.display()
            )));
        }
    };
    let clock: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CliError::Project(format!("system clock is before Unix epoch: {error}")))?
        .as_millis()
        .try_into()
        .map_err(|_| CliError::Project("system time does not fit a KBB release".to_owned()))?;
    let release = clock.max(manifest_release).max(previous.saturating_add(1));
    fs::write(&state, format!("{release}\n")).map_err(|error| {
        CliError::Project(format!(
            "could not update development release state {}: {error}",
            state.display()
        ))
    })?;
    Ok(release)
}

fn absolute_path(path: &Path) -> Result<PathBuf, CliError> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .map_err(CliError::CurrentDirectory)
    }
}

fn resolve_project_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        root.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_paths_are_resolved_from_the_manifest_directory() {
        let root = Path::new("project");
        assert_eq!(
            resolve_project_path(root, Path::new("build/app")),
            root.join("build/app")
        );
        let absolute = std::env::temp_dir().join("kindlebridge-absolute-input");
        assert_eq!(resolve_project_path(root, &absolute), absolute);
    }

    #[test]
    fn development_release_is_monotonic_and_never_below_the_manifest() {
        let root = std::env::temp_dir().join(format!(
            "kindlebridge-development-release-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let first = next_development_release(&root, 10_000).unwrap();
        let second = next_development_release(&root, 10_000).unwrap();
        assert!(first >= 10_000);
        assert!(second > first);
        fs::remove_dir_all(root).unwrap();
    }
}
