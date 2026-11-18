use anyhow::Context as _;
use heart_client::v2::{
    ClaimScope, DispatchConfig, ExternalDispatcher, ExternalHandler, ExternalJobsClient,
    HandlerFailure, JobContext,
};
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    io::{self, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

#[derive(Deserialize)]
struct Input {
    account_id: String,
}

#[derive(Deserialize, Serialize)]
struct Decision {
    approved: bool,
    account_id: String,
}

struct Pilot {
    directory: PathBuf,
    pause_after_effect: Duration,
}

#[async_trait::async_trait]
impl ExternalHandler for Pilot {
    type Input = Input;
    type Output = Decision;

    async fn handle(&self, input: Input, context: JobContext) -> Result<Decision, HandlerFailure> {
        let directory = self.directory.clone();
        let job_id = context.job_id;
        if input.account_id.is_empty() {
            return Err(HandlerFailure::permanent("invalid_account_id"));
        }
        let decision = Decision {
            approved: input.account_id != "blocked",
            account_id: input.account_id,
        };
        let (decision, created) =
            tokio::task::spawn_blocking(move || persist_effect(&directory, job_id, decision))
                .await
                .map_err(|_| HandlerFailure::retryable("pilot_effect_task"))?
                .map_err(|_| HandlerFailure::retryable("pilot_effect_store"))?;
        if created {
            tokio::time::sleep(self.pause_after_effect).await;
        }
        Ok(decision)
    }
}

fn persist_effect(
    directory: &Path,
    job_id: Uuid,
    decision: Decision,
) -> io::Result<(Decision, bool)> {
    let target = directory.join(format!("{job_id}.json"));
    let stage = directory.join(format!(".{}.tmp", Uuid::new_v4()));
    let mut file = fs::File::create_new(&stage)?;
    let publish = (|| {
        serde_json::to_writer(&mut file, &decision)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        // Publishing the synced file under the stable ID is the pilot's side
        // effect. A competing attempt can only recover the existing outcome.
        fs::hard_link(&stage, &target)
    })();
    let _ = fs::remove_file(stage);
    match publish {
        Ok(()) => {
            #[cfg(unix)]
            fs::File::open(directory)?.sync_all()?;
            Ok((decision, true))
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let recovered: Decision = serde_json::from_reader(fs::File::open(target)?)?;
            if recovered.account_id != decision.account_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "effect input changed",
                ));
            }
            Ok((recovered, false))
        }
        Err(error) => Err(error),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = env::var("HEART_API_URL").context("set HEART_API_URL")?;
    let token = env::var("HEART_WORKER_TOKEN").context("set HEART_WORKER_TOKEN")?;
    let namespace = env::var("HEART_WORKER_NAMESPACE").context("set HEART_WORKER_NAMESPACE")?;
    let task_type = env::var("HEART_WORKER_TASK_TYPE").unwrap_or_else(|_| "account-check".into());
    let directory =
        PathBuf::from(env::var_os("HEART_PILOT_EFFECT_DIR").context("set HEART_PILOT_EFFECT_DIR")?);
    fs::create_dir_all(&directory)?;
    let pause_ms = env::var("HEART_PILOT_PAUSE_AFTER_EFFECT_MS")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .context("invalid HEART_PILOT_PAUSE_AFTER_EFFECT_MS")?;
    let capacity = env::var("HEART_PILOT_CAPACITY")
        .unwrap_or_else(|_| "1".into())
        .parse::<NonZeroUsize>()
        .context("invalid HEART_PILOT_CAPACITY")?;
    let client = ExternalJobsClient::new(&url, &token)?;
    ExternalDispatcher::new(client, DispatchConfig::default())?
        .register(
            ClaimScope::new(namespace, task_type, 1),
            capacity,
            Pilot {
                directory,
                pause_after_effect: Duration::from_millis(pause_ms),
            },
        )?
        .run(shutdown())
        .await?;
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_replicas_publish_one_effect() {
        let directory = env::temp_dir().join(format!("heart-external-pilot-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let job_id = Uuid::new_v4();
        let recorded = std::thread::scope(|scope| {
            let workers: Vec<_> = [true, false]
                .into_iter()
                .map(|approved| {
                    let directory = &directory;
                    scope.spawn(move || {
                        persist_effect(
                            directory,
                            job_id,
                            Decision {
                                approved,
                                account_id: "same-account".into(),
                            },
                        )
                        .unwrap()
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(recorded.iter().filter(|(_, created)| *created).count(), 1);
        assert_eq!(recorded[0].0.approved, recorded[1].0.approved);
        let path = directory.join(format!("{job_id}.json"));
        fs::remove_file(path).unwrap();
        fs::remove_dir(directory).unwrap();
    }
}
