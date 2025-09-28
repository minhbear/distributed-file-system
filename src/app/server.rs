use std::sync::Arc;

use async_trait::async_trait;
use log::info;
use thiserror::Error;
use tokio::{
  sync::Mutex,
  task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::app::{
  config::P2pServiceConfig,
  service::{P2pNetworkError, P2pService},
};

const LOG_TARGET: &str = "app::server";

#[derive(Debug, Error)]
pub enum ServerError {
  #[error("Task join error: {0}")]
  TaskJoin(#[from] JoinError),
  #[error("P2P network error: {0}")]
  P2pNetwork(#[from] P2pNetworkError),
}

pub type ServerResult<T> = Result<T, ServerError>;

pub struct Server {
  cancel_token: CancellationToken,
  subtasks: Arc<Mutex<JoinSet<Result<(), ServerError>>>>,
}

#[async_trait]
pub trait Service: Send + Sync + 'static {
  async fn start(&self, cancel_token: CancellationToken) -> Result<(), ServerError>;
}

impl Server {
  pub fn new() -> Self {
    Self {
      cancel_token: CancellationToken::new(),
      subtasks: Arc::new(Mutex::new(JoinSet::new())),
    }
  }

  pub async fn start(&self) -> ServerResult<()> {
    let p2p_service = P2pService::new(
      P2pServiceConfig::builder()
        .with_keypair_file("./keys.keypair")
        .build(),
    );
    self.spawn_task(p2p_service).await?;

    Ok(())
  }

  /// Stops the server.
  pub async fn stop(&self) -> ServerResult<()> {
    info!(target: LOG_TARGET, "Shutting down...");
    self.cancel_token.cancel();
    let mut tasks = self.subtasks.lock().await;
    while let Some(res) = tasks.join_next().await {
      res?;
    }
    Ok(())
  }

  pub async fn spawn_task<S: Service>(&self, service: S) -> ServerResult<()> {
    let mut join_set = self.subtasks.lock().await;
    let cancel_token = self.cancel_token.clone();
    join_set.spawn(async move {
      service.start(cancel_token).await?;

      Ok(())
    });

    Ok(())
  }
}
