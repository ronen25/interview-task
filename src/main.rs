mod message;
mod msgqueue;

use std::io;
use std::borrow::BorrowMut;
use std::sync::Mutex;
use std::error;

use message::Message;
use msgqueue::QueueManager;

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder, HttpRequest, Result};
use serde::Deserialize;
use crate::msgqueue::MessageQueue;

#[derive(Deserialize)]
struct TimeoutParam {
    timeout: i64,
}

#[post("/api/{queue_name}")]
async fn post_message(data: web::Data<QueueManager>, web::Path(queue_name): web::Path<String>,
    mut payload: web::Payload)
    -> Result<HttpResponse> {
    const MAX_MESSAGE_SIZE: usize = 512_000;

    // Check if queue exists
    if let Ok(exists) = data.queue_exists(queue_name.as_str()) {
        if !exists {
            data.lock().create_queue(queue_name.as_str())?;
        }
    }

    // Serialize message
    // NOTE: If I had more time I would've moved this into a separate function,
    // this DOES NOT belong here.
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_MESSAGE_SIZE {
            return Err(actix_web::error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }

    // Put message in queue
    let msg = serde_json::from_slice::<Message>(&body)?;
    data.lock().post_message(msg)?;

    Ok(HttpResponse::ok().finish())
}

#[get("/api/{queue_name}")]
async fn get_message(data: web::Data<QueueManager>, info: web::Query<TimeoutParam>,
    web::Path(queue_name): web::Path<String>) -> Result<HttpResponse> {
    if let Ok(exists) = data.queue_exists(queue_name.as_str()) {
        if !exists {
            return Ok(HttpResponse::BadRequest().finish())
        }
    }

    todo!("NOT FINISHED")
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    let queue = web::Data::new(Mutex::new(QueueManager::new()));

    HttpServer::new(move || {
        App::new()
            .service(post_message)
            .service(get_message)
            .app_data(queue.clone())
    })
    .bind("127.0.0.1:600")?
    .run()
    .await
}
