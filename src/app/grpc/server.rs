use std::net::AddrParseError;

use log::{error, info};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::app::{
  ServerError, Service,
  grpc::server::publish::publish_server::{Publish, PublishServer},
};
use async_trait::async_trait;

pub mod publish {
  tonic::include_proto!("publish");
}

const LOG_TARGET: &str = "app::grpc::server";

#[derive(Debug, Error)]
pub enum GrpcServerError {
  #[error("Failed to parse address: {0}")]
  AddressParse(#[from] AddrParseError),
}

#[derive(Debug)]
pub struct PublishService {}

impl PublishService {
  pub fn new() -> Self {
    Self {}
  }
}

#[tonic::async_trait]
impl Publish for PublishService {
  async fn publish_file(
    &self,
    request: tonic::Request<publish::PublishFileRequest>,
  ) -> std::result::Result<tonic::Response<publish::PublishFileResponse>, tonic::Status> {
    info!(target: LOG_TARGET, "We got a new publish file request: {request:?}");

    let response = publish::PublishFileResponse {
      success: true,
      error: "".to_string(),
    };
    Ok(tonic::Response::new(response))
  }
}

pub struct GrpcService {
  port: u16,
}

impl GrpcService {
  pub fn new(port: u16) -> Self {
    Self { port }
  }
}

#[async_trait]
impl Service for GrpcService {
  async fn start(&self, cancel_token: CancellationToken) -> Result<(), ServerError> {
    let grpc_address = format!("127.0.0.1:{}", self.port)
      .as_str()
      .parse()
      .map_err(|error| GrpcServerError::AddressParse(error))?;
    info!(target: LOG_TARGET, "Grpc server is starting at {grpc_address}!");
    if let Err(error) = Server::builder()
      .add_service(PublishServer::new(PublishService::new()))
      .serve_with_shutdown(grpc_address, cancel_token.cancelled())
      .await
    {
      error!(target: LOG_TARGET, "Error during Grpc server run: {error:?}");
    }

    info!(target: LOG_TARGET, "Shutting down Grpc server...");

    Ok(())
  }
}
