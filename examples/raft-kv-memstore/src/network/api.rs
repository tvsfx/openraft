use core::time::Duration;

use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::error::CheckIsLeaderError;
use openraft::error::Fatal;
use openraft::error::Infallible;
use openraft::error::RaftError;
use openraft::BasicNode;
use openraft::RaftState;
use tokio::sync::oneshot;
use web::Json;

use crate::app::App;
use crate::store::Request;
use crate::NodeId;

/**
 * Application API
 *
 * This is where you place your application, you can use the example below to create your
 * API. The current implementation:
 *
 *  - `POST - /write` saves a value in a key and sync the nodes.
 *  - `POST - /read` attempt to find a value from a given key.
 */
#[post("/write")]
pub async fn write(app: Data<App>, req: Json<Request>) -> actix_web::Result<impl Responder> {
    let response = app.raft.client_write(req.0).await;
    Ok(Json(response))
}

#[post("/read")]
pub async fn read(app: Data<App>, req: Json<String>) -> actix_web::Result<impl Responder> {
    let state_machine = app.store.state_machine.read().await;
    let key = req.0;
    let value = state_machine.data.get(&key).cloned();

    let res: Result<String, Infallible> = Ok(value.unwrap_or_default());
    Ok(Json(res))
}

#[post("/consistent_read")]
pub async fn consistent_read(app: Data<App>, req: Json<String>) -> actix_web::Result<impl Responder> {
    let ret = app.raft.is_leader().await;

    match ret {
        Ok(_) => {
            // Retrieve the `read_index` at which we are allowed to serve the read.
            // Note that the index we receive might be higher than it was at the time of the `is_leader`
            // request, but this is no issue, as a higher read index is still linearizable.
            // TODO: this extra call can be avoided by having `is_leader` return the `read_index` immediately,
            // but that requires an API change.
            let (tx, rx) = oneshot::channel();
            let fn_read_index = |raft_state: &RaftState<_, _, _>| {
                let _ = tx.send(raft_state.committed.map(|log_id| log_id.index));
            };
            app.raft.external_request(fn_read_index);
            let res = rx.await;
            let read_index = match res {
                Ok(x) => x, //Note that `x` should never be `None`
                Err(recv_err) => {
                    tracing::error!("error awaiting raft core: {}", recv_err);
                    let res: Result<String, RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>> =
                        Err(RaftError::Fatal(Fatal::Stopped)); //TODO: can this only be caused by a shutdown?
                    return Ok(Json(res));
                }
            };

            // Wait until `read_index` is applied; we can see this by subscribing to `RaftMetrics`
            // 3 seconds is the client-side timeout as well
            let res = app
                .raft
                .wait(Some(Duration::from_millis(3_000)))
                .log_at_least(read_index, "apply logs until read_index")
                .await;
            match res {
                Ok(_) => (), //No need for metrics
                Err(wait_err) =>
                //TODO: create more general `ReadError` type for timeout case
                {
                    tracing::error!("error awaiting metrics for `read_index`: {}", wait_err);
                    let res: Result<String, RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>> =
                        Err(RaftError::Fatal(Fatal::Stopped));
                    return Ok(Json(res));
                }
            };

            // Now we can safely read the value in the state machine
            let state_machine = app.store.state_machine.read().await;
            let key = req.0;
            let value = state_machine.data.get(&key).cloned();

            let res: Result<String, RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>> =
                Ok(value.unwrap_or_default());
            Ok(Json(res))
        }
        Err(e) => Ok(Json(Err(e))),
    }
}
