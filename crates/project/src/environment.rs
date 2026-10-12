use anyhow::{Context as _, anyhow, bail};
use futures::{
    FutureExt, StreamExt as _,
    channel::mpsc,
    future::{BoxFuture, Shared},
};
use language::Buffer;
use parking_lot::Mutex;
use remote::RemoteClient;
use rpc::proto::{self, REMOTE_SERVER_PROJECT_ID};
use std::{collections::VecDeque, path::Path, sync::Arc};
use task::{Shell, shell_to_proto};
use util::{ResultExt, command::new_command, rel_path::RelPath};
use worktree::{PathChange, Worktree};

use collections::{HashMap, HashSet};
use gpui::{App, AppContext as _, Context, Entity, EventEmitter, Subscription, Task, WeakEntity};
use settings::Settings as _;

use crate::{
    project_settings::{DevenvSettings, DirenvSettings, ProjectSettings},
    worktree_store::{WorktreeStore, WorktreeStoreEvent},
};

type DevenvLoader = Arc<
    dyn Fn(
            HashMap<String, String>,
            Arc<Path>,
        ) -> BoxFuture<'static, anyhow::Result<DevenvEnvironment>>
        + Send
        + Sync,
>;

struct CachedDevenvEnvironment {
    outcome: Shared<Task<DevenvEnvironment>>,
    environment: Shared<Task<Option<HashMap<String, String>>>>,
}

pub struct ProjectEnvironment {
    cli_environment: Option<HashMap<String, String>>,
    local_environments: HashMap<(Shell, Arc<Path>), Shared<Task<Option<HashMap<String, String>>>>>,
    remote_environments: HashMap<(Shell, Arc<Path>), Shared<Task<Option<HashMap<String, String>>>>>,
    devenv_environments: HashMap<(Shell, Arc<Path>), CachedDevenvEnvironment>,
    devenv_loader: DevenvLoader,
    /// Untrusted devenv projects are probed again on every request, so their
    /// hint is only reported once until the environments are reloaded.
    reported_devenv_messages: Arc<Mutex<HashSet<String>>>,
    integration_settings: Option<(DirenvSettings, DevenvSettings)>,
    environment_error_messages: VecDeque<String>,
    environment_error_messages_tx: mpsc::UnboundedSender<String>,
    worktree_store: WeakEntity<WorktreeStore>,
    remote_client: Option<WeakEntity<RemoteClient>>,
    is_remote_project: bool,
    _tasks: Vec<Task<()>>,
    _subscriptions: Vec<Subscription>,
}

pub enum ProjectEnvironmentEvent {
    ErrorsUpdated,
}

impl EventEmitter<ProjectEnvironmentEvent> for ProjectEnvironment {}

impl ProjectEnvironment {
    pub fn new(
        cli_environment: Option<HashMap<String, String>>,
        worktree_store: WeakEntity<WorktreeStore>,
        remote_client: Option<WeakEntity<RemoteClient>>,
        is_remote_project: bool,
        cx: &mut Context<Self>,
    ) -> Self {
        let (tx, mut rx) = mpsc::unbounded();
        let task = cx.spawn(async move |this, cx| {
            while let Some(message) = rx.next().await {
                this.update(cx, |this, cx| {
                    this.environment_error_messages.push_back(message);
                    cx.emit(ProjectEnvironmentEvent::ErrorsUpdated);
                })
                .ok();
            }
        });
        let mut _subscriptions = Vec::new();
        if let Some(worktree_store) = worktree_store.upgrade() {
            _subscriptions.push(cx.subscribe(
                &worktree_store,
                |this, worktree_store, event, cx| {
                    this.handle_worktree_store_event(&worktree_store, event, cx);
                },
            ));
        }
        Self {
            cli_environment,
            local_environments: Default::default(),
            remote_environments: Default::default(),
            devenv_environments: Default::default(),
            devenv_loader: default_devenv_loader(),
            reported_devenv_messages: Default::default(),
            integration_settings: None,
            environment_error_messages: Default::default(),
            environment_error_messages_tx: tx,
            worktree_store,
            remote_client,
            is_remote_project,
            _tasks: vec![task],
            _subscriptions,
        }
    }

    /// Returns the inherited CLI environment, if this project was opened from the Zed CLI.
    pub(crate) fn get_cli_environment(&self) -> Option<HashMap<String, String>> {
        if cfg!(any(test, feature = "test-support")) {
            return Some(HashMap::default());
        }
        if let Some(mut env) = self.cli_environment.clone() {
            set_origin_marker(&mut env, EnvironmentOrigin::Cli);
            Some(env)
        } else {
            None
        }
    }

    pub fn buffer_environment(
        &mut self,
        buffer: &Entity<Buffer>,
        worktree_store: &Entity<WorktreeStore>,
        cx: &mut Context<Self>,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        let Some(worktree) = buffer
            .read(cx)
            .file()
            .map(|f| f.worktree_id(cx))
            .and_then(|worktree_id| worktree_store.read(cx).worktree_for_id(worktree_id, cx))
        else {
            if let Some(cli_environment) = self.get_cli_environment() {
                log::debug!("using project environment variables from CLI");
                return Task::ready(Some(cli_environment)).shared();
            }
            return Task::ready(None).shared();
        };
        self.worktree_environment(worktree, cx)
    }

    pub fn worktree_environment(
        &mut self,
        worktree: Entity<Worktree>,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        let worktree = worktree.read(cx);
        let mut abs_path = worktree.abs_path();
        if worktree.is_single_file() {
            let Some(parent) = abs_path.parent() else {
                return Task::ready(None).shared();
            };
            abs_path = parent.into();
        }

        let remote_client = self.remote_client.as_ref().and_then(|it| it.upgrade());
        match remote_client {
            Some(remote_client) => remote_client.clone().read(cx).shell().map(|shell| {
                self.remote_directory_environment(
                    &Shell::Program(shell),
                    abs_path,
                    remote_client,
                    cx,
                )
            }),
            None if self.is_remote_project => {
                Some(self.local_directory_environment(&Shell::System, abs_path, cx))
            }
            None => Some(self.local_directory_environment(&Shell::System, abs_path, cx)),
        }
        .unwrap_or_else(|| Task::ready(None).shared())
    }

    pub fn directory_environment(
        &mut self,
        abs_path: Arc<Path>,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        let remote_client = self.remote_client.as_ref().and_then(|it| it.upgrade());
        match remote_client {
            Some(remote_client) => remote_client.clone().read(cx).shell().map(|shell| {
                self.remote_directory_environment(
                    &Shell::Program(shell),
                    abs_path,
                    remote_client,
                    cx,
                )
            }),
            None if self.is_remote_project => {
                Some(self.local_directory_environment(&Shell::System, abs_path, cx))
            }
            None => self
                .worktree_store
                .read_with(cx, |worktree_store, cx| {
                    worktree_store.find_worktree(&abs_path, cx)
                })
                .ok()
                .map(|_| self.local_directory_environment(&Shell::System, abs_path, cx)),
        }
        .unwrap_or_else(|| Task::ready(None).shared())
    }

    /// Returns the project environment using the default worktree path.
    /// This ensures that project-specific environment variables (e.g. from `.envrc`)
    /// are loaded from the project directory rather than the home directory.
    pub fn default_environment(
        &mut self,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        let abs_path = self
            .worktree_store
            .read_with(cx, |worktree_store, cx| {
                crate::Project::default_visible_worktree_paths(worktree_store, cx)
                    .into_iter()
                    .next()
            })
            .ok()
            .flatten()
            .map(|path| Arc::<Path>::from(path))
            .unwrap_or_else(|| paths::home_dir().as_path().into());
        self.local_directory_environment(&Shell::System, abs_path, cx)
    }

    /// Returns the project environment, if possible.
    /// If the project was opened from the CLI, then the inherited CLI environment is returned.
    /// If it wasn't opened from the CLI, and an absolute path is given, then a shell is spawned in
    /// that directory, to get environment variables as if the user has `cd`'d there.
    /// In both cases, the environment of a trusted devenv project is applied on top.
    pub fn local_directory_environment(
        &mut self,
        shell: &Shell,
        abs_path: Arc<Path>,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        let project_settings = ProjectSettings::get_global(cx);
        let integration_settings = (
            project_settings.load_direnv.clone(),
            project_settings.load_devenv.clone(),
        );
        if self.integration_settings.as_ref() != Some(&integration_settings) {
            self.local_environments.clear();
            self.devenv_environments.clear();
            self.reported_devenv_messages.lock().clear();
            self.integration_settings = Some(integration_settings.clone());
        }
        let (load_direnv, load_devenv) = integration_settings;

        let cli_environment = self.get_cli_environment();
        let origin = if cli_environment.is_some() {
            EnvironmentOrigin::Cli
        } else {
            EnvironmentOrigin::WorktreeShell
        };
        let base_environment = if let Some(cli_environment) = cli_environment {
            log::debug!("using project environment variables from CLI");
            Task::ready(Some(cli_environment)).shared()
        } else {
            self.local_environments
                .entry((shell.clone(), abs_path.clone()))
                .or_insert_with(|| {
                    let shell = shell.clone();
                    let abs_path = abs_path.clone();
                    let tx = self.environment_error_messages_tx.clone();
                    cx.spawn(async move |cx| {
                        let mut shell_env = match cx
                            .background_spawn(load_directory_shell_environment(
                                shell,
                                abs_path.clone(),
                                load_direnv,
                                tx,
                            ))
                            .await
                        {
                            Ok(shell_env) => Some(shell_env),
                            Err(e) => {
                                log::error!(
                                    "Failed to load shell environment for directory {abs_path:?}: {e:#}"
                                );
                                None
                            }
                        };

                        if let Some(shell_env) = shell_env.as_mut() {
                            let path = shell_env
                                .get("PATH")
                                .map(|path| path.as_str())
                                .unwrap_or_default();
                            log::debug!(
                                "using project environment variables shell launched in {:?}. PATH={:?}",
                                abs_path,
                                path
                            );

                            set_origin_marker(shell_env, EnvironmentOrigin::WorktreeShell);
                        }

                        shell_env
                    })
                    .shared()
                })
                .clone()
        };

        if cfg!(target_os = "windows") || load_devenv == DevenvSettings::Disabled {
            return base_environment;
        }

        let key = (shell.clone(), abs_path.clone());
        // Probing an untrusted project does not evaluate its configuration, so
        // it is cheap to repeat, and lets `devenv allow` take effect without
        // reopening the project.
        if self.devenv_environments.get(&key).is_some_and(|cached| {
            matches!(
                cached.outcome.peek(),
                Some(DevenvEnvironment::Untrusted { .. })
            )
        }) {
            self.devenv_environments.remove(&key);
        }

        let devenv_loader = self.devenv_loader.clone();
        let reported_messages = self.reported_devenv_messages.clone();
        let tx = self.environment_error_messages_tx.clone();
        self.devenv_environments
            .entry(key)
            .or_insert_with(|| {
                let outcome = cx
                    .spawn({
                        let base_environment = base_environment.clone();
                        async move |cx| {
                            let Some(environment) = base_environment.await else {
                                return DevenvEnvironment::NothingToDo;
                            };
                            let outcome = cx
                                .background_spawn(devenv_loader(environment, abs_path.clone()))
                                .await
                                .unwrap_or_else(|error| {
                                    log::error!(
                                        "Failed to load devenv environment for directory {abs_path:?}: {error:#}"
                                    );
                                    DevenvEnvironment::Failed {
                                        message: "Failed to load devenv environment".into(),
                                    }
                                });
                            if let DevenvEnvironment::Untrusted { message }
                            | DevenvEnvironment::Failed { message } = &outcome
                                && reported_messages.lock().insert(message.clone())
                            {
                                tx.unbounded_send(message.clone()).ok();
                            }
                            outcome
                        }
                    })
                    .shared();
                let environment = cx
                    .spawn({
                        let outcome = outcome.clone();
                        async move |_| {
                            let mut environment = base_environment.await?;
                            if let DevenvEnvironment::Vars(devenv_vars) = outcome.await {
                                apply_devenv_environment(&mut environment, devenv_vars);
                                set_origin_marker(&mut environment, origin);
                            }
                            Some(environment)
                        }
                    })
                    .shared();
                CachedDevenvEnvironment {
                    outcome,
                    environment,
                }
            })
            .environment
            .clone()
    }

    pub fn remote_directory_environment(
        &mut self,
        shell: &Shell,
        abs_path: Arc<Path>,
        remote_client: Entity<RemoteClient>,
        cx: &mut App,
    ) -> Shared<Task<Option<HashMap<String, String>>>> {
        if cfg!(any(test, feature = "test-support")) {
            return Task::ready(Some(HashMap::default())).shared();
        }

        self.remote_environments
            .entry((shell.clone(), abs_path.clone()))
            .or_insert_with(|| {
                let response =
                    remote_client
                        .read(cx)
                        .proto_client()
                        .request(proto::GetDirectoryEnvironment {
                            project_id: REMOTE_SERVER_PROJECT_ID,
                            shell: Some(shell_to_proto(shell.clone())),
                            directory: abs_path.to_string_lossy().to_string(),
                        });
                cx.background_spawn(async move {
                    let environment = response.await.log_err()?;
                    Some(environment.environment.into_iter().collect())
                })
                .shared()
            })
            .clone()
    }

    pub fn peek_environment_error(&self) -> Option<&String> {
        self.environment_error_messages.front()
    }

    pub fn pop_environment_error(&mut self) -> Option<String> {
        self.environment_error_messages.pop_front()
    }

    fn handle_worktree_store_event(
        &mut self,
        worktree_store: &Entity<WorktreeStore>,
        event: &WorktreeStoreEvent,
        cx: &mut Context<Self>,
    ) {
        let WorktreeStoreEvent::WorktreeUpdatedEntries(worktree_id, entries) = event else {
            return;
        };
        // Scanning a worktree reports its existing entries as `Loaded`, which
        // must not reload environments that were loaded from those files.
        if !entries.iter().any(|(path, _, change)| {
            *change != PathChange::Loaded && is_watched_environment_file(path)
        }) {
            return;
        }
        let Some(worktree) = worktree_store.read(cx).worktree_for_id(*worktree_id, cx) else {
            return;
        };
        let root = worktree.read(cx).abs_path();
        self.local_environments
            .retain(|(_, path), _| !path.starts_with(&root));
        self.remote_environments
            .retain(|(_, path), _| !path.starts_with(&root));
        self.devenv_environments
            .retain(|(_, path), _| !path.starts_with(&root));
        self.reported_devenv_messages.lock().clear();
    }
}

fn set_origin_marker(env: &mut HashMap<String, String>, origin: EnvironmentOrigin) {
    env.insert(ZED_ENVIRONMENT_ORIGIN_MARKER.to_string(), origin.into());
}

const ZED_ENVIRONMENT_ORIGIN_MARKER: &str = "ZED_ENVIRONMENT";

#[derive(Clone, Copy)]
enum EnvironmentOrigin {
    Cli,
    WorktreeShell,
}

impl From<EnvironmentOrigin> for String {
    fn from(val: EnvironmentOrigin) -> Self {
        match val {
            EnvironmentOrigin::Cli => "cli".into(),
            EnvironmentOrigin::WorktreeShell => "worktree-shell".into(),
        }
    }
}

async fn load_directory_shell_environment(
    shell: Shell,
    abs_path: Arc<Path>,
    load_direnv: DirenvSettings,
    tx: mpsc::UnboundedSender<String>,
) -> anyhow::Result<HashMap<String, String>> {
    if let DirenvSettings::Disabled = load_direnv {
        return Ok(HashMap::default());
    }

    let meta = smol::fs::metadata(&abs_path).await.with_context(|| {
        tx.unbounded_send(format!("Failed to open {}", abs_path.display()))
            .ok();
        format!("stat {abs_path:?}")
    })?;

    let dir = if meta.is_dir() {
        abs_path.clone()
    } else {
        abs_path
            .parent()
            .with_context(|| {
                tx.unbounded_send(format!("Failed to open {}", abs_path.display()))
                    .ok();
                format!("getting parent of {abs_path:?}")
            })?
            .into()
    };

    let (shell, args) = shell.program_and_args();
    let mut envs = util::shell_env::capture(shell.clone(), args, abs_path)
        .await
        .with_context(|| {
            tx.unbounded_send("Failed to load environment variables".into())
                .ok();
            format!("capturing shell environment with {shell:?}")
        })?;

    if cfg!(target_os = "windows")
        && let Some(path) = envs.remove("Path")
    {
        // windows env vars are case-insensitive, so normalize the path var
        // so we can just assume `PATH` in other places
        envs.insert("PATH".into(), path);
    }
    // If the user selects `Direct` for direnv, it would set an environment
    // variable that later uses to know that it should not run the hook.
    // We would include in `.envs` call so it is okay to run the hook
    // even if direnv direct mode is enabled.
    let direnv_environment = match load_direnv {
        DirenvSettings::ShellHook => None,
        DirenvSettings::Disabled => bail!("direnv integration is disabled"),
        // Note: direnv is not available on Windows, so we skip direnv processing
        // and just return the shell environment
        DirenvSettings::Direct if cfg!(target_os = "windows") => None,
        DirenvSettings::Direct => load_direnv_environment(&envs, &dir)
            .await
            .with_context(|| {
                tx.unbounded_send("Failed to load direnv environment".into())
                    .ok();
                "load direnv environment"
            })
            .log_err(),
    };
    if let Some(direnv_environment) = direnv_environment {
        for (key, value) in direnv_environment {
            if let Some(value) = value {
                envs.insert(key, value);
            } else {
                envs.remove(&key);
            }
        }
    }

    Ok(envs)
}

async fn load_direnv_environment(
    env: &HashMap<String, String>,
    dir: &Path,
) -> anyhow::Result<HashMap<String, Option<String>>> {
    let Some(direnv_path) = which::which("direnv").ok() else {
        return Ok(HashMap::default());
    };

    let args = &["export", "json"];
    let direnv_output = new_command(&direnv_path)
        .args(args)
        .envs(env)
        .env("TERM", "dumb")
        .current_dir(dir)
        .output()
        .await
        .context("running direnv")?;

    if !direnv_output.status.success() {
        bail!(
            "Loading direnv environment failed ({}), stderr: {}",
            direnv_output.status,
            String::from_utf8_lossy(&direnv_output.stderr)
        );
    }

    let output = String::from_utf8_lossy(&direnv_output.stdout);
    if output.is_empty() {
        // direnv outputs nothing when it has no changes to apply to environment variables
        return Ok(HashMap::default());
    }

    serde_json::from_str(&output).context("parsing direnv json")
}

/// devenv and direnv look for their project by walking up from the worktree
/// root, so only changes to these files at the root reload its environment.
/// Files devenv lists in `.devenv/input-paths.txt` are not tracked.
const WATCHED_ENVIRONMENT_FILES: &[&str] = &[
    "devenv.nix",
    "devenv.yaml",
    "devenv.lock",
    "devenv.local.nix",
    "devenv.local.yaml",
    ".envrc",
];

fn is_watched_environment_file(path: &RelPath) -> bool {
    path.parent().is_none_or(RelPath::is_empty)
        && path
            .file_name()
            .is_some_and(|name| WATCHED_ENVIRONMENT_FILES.contains(&name))
}

#[derive(Clone)]
enum DevenvEnvironment {
    Vars(HashMap<String, String>),
    /// devenv is not installed, there is no devenv project, or its
    /// environment is already active.
    NothingToDo,
    Untrusted {
        message: String,
    },
    Failed {
        message: String,
    },
}

enum DevenvActivation {
    Activate,
    NoProject,
    NotAllowed,
    Unsupported,
}

/// Classifies the result of `devenv hook-should-activate`, the check devenv's
/// own shell hook runs on every prompt.
fn classify_devenv_hook_output(
    exit_code: i32,
    stdout: &[u8],
    stderr: &str,
) -> anyhow::Result<DevenvActivation> {
    match exit_code {
        0 if stdout.iter().all(u8::is_ascii_whitespace) => Ok(DevenvActivation::NoProject),
        0 => Ok(DevenvActivation::Activate),
        2 if stderr.contains("is not allowed") => Ok(DevenvActivation::NotAllowed),
        // Usage errors also exit with code 2, which is what devenv versions
        // that predate `hook-should-activate` report for it.
        2 => Ok(DevenvActivation::Unsupported),
        exit_code => Err(anyhow!(
            "devenv hook-should-activate failed with exit code {exit_code}: {stderr}"
        )),
    }
}

async fn environment_directory(abs_path: Arc<Path>) -> Arc<Path> {
    let is_dir = smol::fs::metadata(&abs_path)
        .await
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false);
    if is_dir {
        abs_path
    } else {
        abs_path.parent().map(Arc::from).unwrap_or(abs_path)
    }
}

fn apply_devenv_environment(
    environment: &mut HashMap<String, String>,
    devenv_vars: HashMap<String, String>,
) {
    for (key, value) in devenv_vars {
        // devenv's values of these only contain nix store paths. Like
        // `nix develop`, prepend them instead of replacing the existing value,
        // so the user's own tools remain reachable. Without a shell environment
        // (direnv disabled), the existing value is the one of the Zed process.
        let value = match key.as_str() {
            "PATH" | "XDG_DATA_DIRS" => {
                let existing = environment
                    .get(&key)
                    .cloned()
                    .or_else(|| std::env::var(&key).ok())
                    .unwrap_or_default();
                prepend_path_list(&value, &existing)
            }
            _ => value,
        };
        environment.insert(key, value);
    }
}

fn prepend_path_list(new: &str, existing: &str) -> String {
    [new, existing]
        .into_iter()
        .filter(|list| !list.is_empty())
        .collect::<Vec<_>>()
        .join(":")
}

fn default_devenv_loader() -> DevenvLoader {
    if cfg!(any(test, feature = "test-support")) {
        // Keep tests independent of the host's devenv installation.
        Arc::new(|_, _| async { Ok(DevenvEnvironment::NothingToDo) }.boxed())
    } else {
        Arc::new(|environment, directory| load_devenv_environment(environment, directory).boxed())
    }
}

async fn load_devenv_environment(
    environment: HashMap<String, String>,
    abs_path: Arc<Path>,
) -> anyhow::Result<DevenvEnvironment> {
    let directory = environment_directory(abs_path).await;
    // `devenv hook-should-activate` does not detect an environment that is
    // already active (e.g. loaded by direnv's `use devenv`), and would report
    // an untrusted project even though its environment was loaded.
    if environment
        .get("DEVENV_ROOT")
        .is_some_and(|root| directory.starts_with(root))
    {
        return Ok(DevenvEnvironment::NothingToDo);
    }

    let search_path = environment
        .get("PATH")
        .cloned()
        .or_else(|| std::env::var("PATH").ok());
    let Ok(devenv_path) = which::which_in("devenv", search_path, &directory) else {
        return Ok(DevenvEnvironment::NothingToDo);
    };

    // Evaluating the devenv configuration runs arbitrary Nix code, so, like
    // direnv's `.envrc` blocking, this is only done for directories the user
    // has explicitly trusted.
    let hook_output = new_command(&devenv_path)
        .arg("hook-should-activate")
        .envs(&environment)
        .env("TERM", "dumb")
        .current_dir(&directory)
        .output()
        .await
        .context("running devenv hook-should-activate")?;
    let stderr = String::from_utf8_lossy(&hook_output.stderr)
        .trim()
        .to_string();

    match classify_devenv_hook_output(
        hook_output.status.code().unwrap_or(-1),
        &hook_output.stdout,
        &stderr,
    )? {
        DevenvActivation::Activate => {}
        DevenvActivation::NoProject => return Ok(DevenvEnvironment::NothingToDo),
        DevenvActivation::NotAllowed => {
            log::info!("not loading devenv environment: {stderr}");
            return Ok(DevenvEnvironment::Untrusted { message: stderr });
        }
        DevenvActivation::Unsupported => {
            log::warn!("not loading devenv environment, devenv 2.1 or newer is required: {stderr}");
            return Ok(DevenvEnvironment::NothingToDo);
        }
    }

    let devenv_output = new_command(&devenv_path)
        .args(["print-dev-env", "--json"])
        .envs(&environment)
        .env("TERM", "dumb")
        .current_dir(&directory)
        .output()
        .await
        .context("running devenv")?;

    if !devenv_output.status.success() {
        bail!(
            "Loading devenv environment failed ({}), stderr: {}",
            devenv_output.status,
            String::from_utf8_lossy(&devenv_output.stderr)
        );
    }

    let output = String::from_utf8_lossy(&devenv_output.stdout);
    if output.trim().is_empty() {
        return Ok(DevenvEnvironment::NothingToDo);
    }

    Ok(DevenvEnvironment::Vars(parse_devenv_json(&output)?))
}

/// `devenv print-dev-env --json` emits the output of `nix print-dev-env --json`,
/// which is the raw build environment of the shell derivation rather than a
/// ready-to-use shell environment.
#[derive(serde::Deserialize)]
struct DevenvDevEnv {
    variables: HashMap<String, DevenvVariable>,
}

#[derive(serde::Deserialize)]
struct DevenvVariable {
    r#type: String,
    #[serde(default)]
    value: serde_json::Value,
}

/// Variables that `nix develop` strips from the derivation environment before
/// activating it, since their build-sandbox values (`HOME=/homeless-shelter`,
/// `TMPDIR=/build`, ...) would break the resulting shell.
const NIX_IGNORED_VARS: &[&str] = &[
    "BASHOPTS",
    "HOME",
    "NIX_BUILD_TOP",
    "NIX_ENFORCE_PURITY",
    "NIX_LOG_FD",
    "NIX_REMOTE",
    "PPID",
    "SHELLOPTS",
    "SSL_CERT_FILE",
    "TEMP",
    "TEMPDIR",
    "TERM",
    "TMP",
    "TMPDIR",
    "TZ",
    "UID",
];

fn parse_devenv_json(output: &str) -> anyhow::Result<HashMap<String, String>> {
    let dev_env: DevenvDevEnv = serde_json::from_str(output).context("parsing devenv json")?;
    Ok(dev_env
        .variables
        .into_iter()
        .filter_map(|(key, variable)| {
            // Only `exported` entries are environment variables; `var`, `array`
            // and `associative` entries are bash-internal state of the derivation.
            if variable.r#type != "exported" || NIX_IGNORED_VARS.contains(&key.as_str()) {
                return None;
            }
            match variable.value {
                serde_json::Value::String(value) => Some((key, value)),
                _ => None,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs::FakeFs;
    use gpui::{TestAppContext, UpdateGlobal as _};
    use serde_json::json;
    use settings::SettingsStore;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use util::path;

    // Trimmed from real `devenv print-dev-env --json` output.
    const DEVENV_OUTPUT: &str = r#"{
        "bashFunctions": { "_activatePkgs": "    local pkg\n" },
        "variables": {
            "PATH": { "type": "exported", "value": "/nix/store/abc-hello-2.12.3/bin:/nix/store/def-coreutils-9.11/bin" },
            "XDG_DATA_DIRS": { "type": "exported", "value": "/nix/store/abc-hello-2.12.3/share" },
            "DEVENV_ROOT": { "type": "exported", "value": "/path/to/project" },
            "MY_TEST_VAR": { "type": "exported", "value": "hello-from-devenv" },
            "MY_PORT": { "type": "exported", "value": "3000" },
            "HOME": { "type": "exported", "value": "/homeless-shelter" },
            "TMPDIR": { "type": "exported", "value": "/build" },
            "TERM": { "type": "exported", "value": "xterm-256color" },
            "NIX_ENFORCE_PURITY": { "type": "exported", "value": "1" },
            "builder": { "type": "exported", "value": "/nix/store/xyz-bash-5.3p15/bin/bash" },
            "IFS": { "type": "var", "value": " \t\n" },
            "prefix": { "type": "var", "value": "/nix/store/xyz-devenv-shell-env" },
            "pkgsHostTarget": { "type": "array", "value": ["/nix/store/abc-hello-2.12.3"] },
            "envHostHostHooks": { "type": "associative", "value": { "0": "ccWrapper_addCVars" } },
            "BASHOPTS": { "type": "unknown" },
            "SRANDOM": { "type": "unknown" }
        }
    }"#;

    #[test]
    fn test_parse_devenv_json_keeps_only_exported_variables() {
        let parsed = parse_devenv_json(DEVENV_OUTPUT).expect("should parse successfully");

        assert_eq!(
            parsed.get("PATH").map(String::as_str),
            Some("/nix/store/abc-hello-2.12.3/bin:/nix/store/def-coreutils-9.11/bin")
        );
        assert_eq!(
            parsed.get("DEVENV_ROOT").map(String::as_str),
            Some("/path/to/project")
        );
        assert_eq!(
            parsed.get("MY_TEST_VAR").map(String::as_str),
            Some("hello-from-devenv")
        );
        assert_eq!(parsed.get("MY_PORT").map(String::as_str), Some("3000"));
        // Exported derivation variables that are not in nix's ignore list are
        // passed through, matching what `nix develop` does.
        assert!(parsed.contains_key("builder"));

        for ignored in ["HOME", "TMPDIR", "TERM", "NIX_ENFORCE_PURITY"] {
            assert!(!parsed.contains_key(ignored), "{ignored} should be ignored");
        }
        for non_exported in ["IFS", "prefix", "pkgsHostTarget", "envHostHostHooks"] {
            assert!(
                !parsed.contains_key(non_exported),
                "{non_exported} should not be exported"
            );
        }
        for unknown in ["BASHOPTS", "SRANDOM"] {
            assert!(!parsed.contains_key(unknown), "{unknown} should be dropped");
        }
    }

    #[test]
    fn test_parse_devenv_json_rejects_output_without_variables() {
        assert!(parse_devenv_json(r#"{"PATH": "/nix/store/bin"}"#).is_err());
        assert!(parse_devenv_json("not json").is_err());
    }

    #[test]
    fn test_prepend_path_list() {
        assert_eq!(
            prepend_path_list("/nix/store/a/bin", "/usr/bin:/"),
            "/nix/store/a/bin:/usr/bin:/"
        );
        assert_eq!(
            prepend_path_list("/nix/store/a/bin", ""),
            "/nix/store/a/bin"
        );
        assert_eq!(prepend_path_list("", "/usr/bin"), "/usr/bin");
    }

    #[test]
    fn test_classify_devenv_hook_output() {
        assert!(matches!(
            classify_devenv_hook_output(0, b"/path/to/project", ""),
            Ok(DevenvActivation::Activate)
        ));
        assert!(matches!(
            classify_devenv_hook_output(0, b" \n\t ", ""),
            Ok(DevenvActivation::NoProject)
        ));
        assert!(matches!(
            classify_devenv_hook_output(
                2,
                b"",
                "devenv: /path/to/project is not allowed. Run 'devenv allow' to trust this directory."
            ),
            Ok(DevenvActivation::NotAllowed)
        ));
        assert!(matches!(
            classify_devenv_hook_output(
                2,
                b"",
                "error: unrecognized subcommand 'hook-should-activate'"
            ),
            Ok(DevenvActivation::Unsupported)
        ));
        assert!(classify_devenv_hook_output(1, b"", "").is_err());
    }

    #[test]
    fn test_apply_devenv_environment() {
        let mut environment = HashMap::from_iter([
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("XDG_DATA_DIRS".to_string(), "/usr/share".to_string()),
            ("EXISTING".to_string(), "old".to_string()),
        ]);
        apply_devenv_environment(
            &mut environment,
            HashMap::from_iter([
                ("PATH".to_string(), "/nix/store/a/bin".to_string()),
                (
                    "XDG_DATA_DIRS".to_string(),
                    "/nix/store/a/share".to_string(),
                ),
                ("EXISTING".to_string(), "new".to_string()),
                ("NEW".to_string(), "value".to_string()),
            ]),
        );

        assert_eq!(
            environment.get("PATH").map(String::as_str),
            Some("/nix/store/a/bin:/usr/bin")
        );
        assert_eq!(
            environment.get("XDG_DATA_DIRS").map(String::as_str),
            Some("/nix/store/a/share:/usr/share")
        );
        assert_eq!(environment.get("EXISTING").map(String::as_str), Some("new"));
        assert_eq!(environment.get("NEW").map(String::as_str), Some("value"));
    }

    #[test]
    fn test_is_watched_environment_file() {
        for name in [
            "devenv.nix",
            "devenv.yaml",
            "devenv.lock",
            "devenv.local.nix",
            "devenv.local.yaml",
            ".envrc",
        ] {
            assert!(
                is_watched_environment_file(&RelPath::new_test(name)),
                "{name} should be watched"
            );
        }
        for name in ["sub/devenv.nix", "devenv.nix.bak", "README.md", "envrc"] {
            assert!(
                !is_watched_environment_file(&RelPath::new_test(name)),
                "{name} should not be watched"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_load_devenv_environment_with_devenv_binary() {
        use std::os::unix::fs::PermissionsExt as _;

        let bin_directory = tempfile::tempdir().expect("should create temp dir");
        let project_directory = tempfile::tempdir().expect("should create temp dir");
        let project_path: Arc<Path> = project_directory.path().into();

        let write_devenv = |script: &str| {
            let devenv_path = bin_directory.path().join("devenv");
            std::fs::write(&devenv_path, format!("#!/bin/sh\n{script}"))
                .expect("should write devenv script");
            std::fs::set_permissions(&devenv_path, std::fs::Permissions::from_mode(0o755))
                .expect("should make devenv script executable");
        };
        let load = |extra: &[(&str, &str)]| {
            let mut environment = HashMap::from_iter([(
                "PATH".to_string(),
                format!("{}:/usr/bin:/bin", bin_directory.path().display()),
            )]);
            for (key, value) in extra {
                environment.insert(key.to_string(), value.to_string());
            }
            smol::block_on(load_devenv_environment(environment, project_path.clone()))
        };

        write_devenv(
            r#"case "$1" in
  hook-should-activate) pwd ;;
  print-dev-env) echo '{"variables":{"FROM_DEVENV":{"type":"exported","value":"yes"}}}' ;;
  *) exit 1 ;;
esac"#,
        );
        let Ok(DevenvEnvironment::Vars(vars)) = load(&[]) else {
            panic!("a trusted project should load its environment");
        };
        assert_eq!(vars.get("FROM_DEVENV").map(String::as_str), Some("yes"));

        let active_root = project_directory.path().display().to_string();
        assert!(matches!(
            load(&[("DEVENV_ROOT", &active_root)]),
            Ok(DevenvEnvironment::NothingToDo)
        ));

        write_devenv(
            r#"echo "devenv: $PWD is not allowed. Run 'devenv allow' to trust this directory." >&2
exit 2"#,
        );
        let Ok(DevenvEnvironment::Untrusted { message }) = load(&[]) else {
            panic!("an untrusted project should not be loaded");
        };
        assert!(message.contains("devenv allow"));

        write_devenv(
            r#"echo "error: unrecognized subcommand '$1'" >&2
exit 2"#,
        );
        assert!(matches!(load(&[]), Ok(DevenvEnvironment::NothingToDo)));

        write_devenv(
            r#"case "$1" in
  hook-should-activate) pwd ;;
  *) echo "error: evaluation failed" >&2; exit 1 ;;
esac"#,
        );
        assert!(load(&[]).is_err());

        std::fs::remove_file(bin_directory.path().join("devenv"))
            .expect("should remove devenv script");
        assert!(matches!(load(&[]), Ok(DevenvEnvironment::NothingToDo)));
    }

    fn init_test(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);
        });
    }

    async fn load_environment(
        environment: &Entity<ProjectEnvironment>,
        directory: &Arc<Path>,
        cx: &mut TestAppContext,
    ) -> HashMap<String, String> {
        environment
            .update(cx, |environment, cx| {
                environment.local_directory_environment(&Shell::System, directory.clone(), cx)
            })
            .await
            .expect("environment should load")
    }

    #[gpui::test]
    async fn test_devenv_environment_caching(cx: &mut TestAppContext) {
        init_test(cx);
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            path!("/project"),
            json!({ "devenv.nix": "", "src": { "main.rs": "" } }),
        )
        .await;
        fs.insert_tree(path!("/other"), json!({ ".envrc": "" }))
            .await;
        let project = crate::Project::test(fs.clone(), [path!("/project").as_ref()], cx).await;
        let environment = project.read_with(cx, |project, _| project.environment().clone());

        let load_count = Arc::new(AtomicUsize::new(0));
        let trusted = Arc::new(AtomicBool::new(false));
        environment.update(cx, |environment, _| {
            environment.devenv_loader = Arc::new({
                let load_count = load_count.clone();
                let trusted = trusted.clone();
                move |_, _| {
                    load_count.fetch_add(1, Ordering::SeqCst);
                    let outcome = if trusted.load(Ordering::SeqCst) {
                        DevenvEnvironment::Vars(HashMap::from_iter([(
                            "FROM_DEVENV".to_string(),
                            "yes".to_string(),
                        )]))
                    } else {
                        DevenvEnvironment::Untrusted {
                            message: "devenv: /project is not allowed".to_string(),
                        }
                    };
                    async move { Ok(outcome) }.boxed()
                }
            });
            environment.devenv_environments.clear();
        });
        let project_directory: Arc<Path> = Path::new(path!("/project")).into();
        let other_directory: Arc<Path> = Path::new(path!("/other")).into();

        // Untrusted projects are probed again on every request, but the hint
        // is only reported once.
        let loaded = load_environment(&environment, &project_directory, cx).await;
        assert!(!loaded.contains_key("FROM_DEVENV"));
        load_environment(&environment, &project_directory, cx).await;
        assert_eq!(load_count.load(Ordering::SeqCst), 2);
        cx.run_until_parked();
        environment.update(cx, |environment, _| {
            assert_eq!(
                environment.pop_environment_error().as_deref(),
                Some("devenv: /project is not allowed")
            );
            assert_eq!(environment.pop_environment_error(), None);
        });

        // Once trusted, the environment is loaded and cached.
        trusted.store(true, Ordering::SeqCst);
        let loaded = load_environment(&environment, &project_directory, cx).await;
        assert_eq!(loaded.get("FROM_DEVENV").map(String::as_str), Some("yes"));
        load_environment(&environment, &project_directory, cx).await;
        assert_eq!(load_count.load(Ordering::SeqCst), 3);

        // Unrelated changes keep the cached environment.
        fs.insert_file(path!("/project/src/main.rs"), b"fn main() {}".to_vec())
            .await;
        cx.run_until_parked();
        load_environment(&environment, &project_directory, cx).await;
        assert_eq!(load_count.load(Ordering::SeqCst), 3);

        // Changing the devenv configuration reloads the environment.
        fs.insert_file(path!("/project/devenv.nix"), b"{ }".to_vec())
            .await;
        cx.run_until_parked();
        load_environment(&environment, &project_directory, cx).await;
        assert_eq!(load_count.load(Ordering::SeqCst), 4);

        // Scanning a newly added worktree does not reload environments that
        // were loaded before.
        load_environment(&environment, &other_directory, cx).await;
        assert_eq!(load_count.load(Ordering::SeqCst), 5);
        project
            .update(cx, |project, cx| {
                project.find_or_create_worktree(path!("/other"), true, cx)
            })
            .await
            .expect("should add worktree");
        cx.run_until_parked();
        load_environment(&environment, &other_directory, cx).await;
        assert_eq!(load_count.load(Ordering::SeqCst), 5);

        // Disabling the integration stops applying the devenv environment.
        cx.update(|cx| {
            SettingsStore::update_global(cx, |store, cx| {
                store.update_user_settings(cx, |settings| {
                    settings.project.load_devenv = Some(DevenvSettings::Disabled);
                });
            });
        });
        let loaded = load_environment(&environment, &project_directory, cx).await;
        assert!(!loaded.contains_key("FROM_DEVENV"));
        assert_eq!(load_count.load(Ordering::SeqCst), 5);
    }
}
