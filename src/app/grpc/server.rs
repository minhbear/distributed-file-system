use std::net::AddrParseError;

use log::{error, info};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server};

use crate::{
  app::{
    ServerError, Service,
    publish::{
      self,
      publish_server::{Publish, PublishServer},
    },
  },
  file_processor::{self, FileProcessResult},
};
use async_trait::async_trait;

const LOG_TARGET: &str = "app::grpc::server";

#[derive(Debug, Error)]
pub enum GrpcServerError {
  #[error("Failed to parse address: {0}")]
  AddressParse(#[from] AddrParseError),
}

#[derive(Debug)]
pub struct PublishService {
  file_publish_tx: mpsc::Sender<FileProcessResult>,
}

impl PublishService {
  pub fn new(file_publish_tx: mpsc::Sender<FileProcessResult>) -> Self {
    Self { file_publish_tx }
  }
}

#[tonic::async_trait]
impl Publish for PublishService {
  async fn publish_file(
    &self,
    request: tonic::Request<publish::PublishFileRequest>,
  ) -> std::result::Result<tonic::Response<publish::PublishFileResponse>, tonic::Status> {
    let request = request.into_inner();
    info!(target: LOG_TARGET, "We got a new publish file request: {request:?}");

    // file processing
    let file_processor = file_processor::Processor::new();
    let file_process_result = file_processor.process_file(&request).await?;

    // TODO: start providing files on DHT
    // TODO: start broadcasting of this file on gossipsub periodically if it's public

    self
      .file_publish_tx
      .send(file_process_result)
      .await
      .map_err(|_error| {
        tonic::Status::internal("Failed to send processed file details internally!")
      })?;

    let response = publish::PublishFileResponse {
      success: true,
      error: "".to_string(),
    };
    Ok(tonic::Response::new(response))
  }
}

pub struct GrpcService {
  port: u16,
  file_publish_tx: mpsc::Sender<FileProcessResult>,
}

impl GrpcService {
  pub fn new(port: u16, file_publish_tx: mpsc::Sender<FileProcessResult>) -> Self {
    Self {
      port,
      file_publish_tx,
    }
  }
}

#[async_trait]
impl Service for GrpcService {
  async fn start(&mut self, cancel_token: CancellationToken) -> Result<(), ServerError> {
    let grpc_address = format!("127.0.0.1:{}", self.port)
      .as_str()
      .parse()
      .map_err(|error| GrpcServerError::AddressParse(error))?;
    info!(target: LOG_TARGET, "Grpc server is starting at {grpc_address}!");
    if let Err(error) = Server::builder()
      .add_service(PublishServer::new(PublishService::new(
        self.file_publish_tx.clone(),
      )))
      .serve_with_shutdown(grpc_address, cancel_token.cancelled())
      .await
    {
      error!(target: LOG_TARGET, "Error during Grpc server run: {error:?}");
    }

    info!(target: LOG_TARGET, "Shutting down Grpc server...");

    Ok(())
  }
}
