//! Implementation of the HTTP service

use crate::config::Config;
use crate::controller::Controller;
use crate::node::{NodeManager, StopMessage};

use actix::SystemService;
use actix_web::{error, web, App, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;

use tracing::error;

/// Create Subscriber/Publisher WebSocket actors.
async fn ws(
    path: web::Path<String>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, actix_web::Error> {
    match path.as_str() {
        "control" => {
            let controller = Controller::new(&req.connection_info()).map_err(|err| {
                error!("Failed to create controller: {}", err);
                error::ErrorInternalServerError(err)
            })?;

            ws::start(controller, &req, stream)
        }
        _ => Ok(HttpResponse::NotFound().finish()),
    }
}

/// Start the server based on the passed `Config`.
pub async fn run(cfg: Config) -> Result<(), anyhow::Error> {
    let server = HttpServer::new(move || {
        App::new()
            .wrap(actix_web::middleware::Logger::default())
            .wrap(tracing_actix_web::TracingLogger::default())
            .route("/ws/{mode:(control)}", web::get().to(ws))
    });

    let server = if cfg.use_tls {
        use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
        builder.set_private_key_file(
            cfg.key_file.as_ref().expect("No key file given"),
            SslFiletype::PEM,
        )?;
        builder.set_certificate_chain_file(
            cfg.certificate_file
                .as_ref()
                .expect("No certificate file given"),
        )?;

        server.bind_openssl(format!("0.0.0.0:{}", cfg.port), builder)?
    } else {
        server.bind(format!("0.0.0.0:{}", cfg.port))?
    };

    server.run().await?;

    let _ = NodeManager::from_registry().send(StopMessage).await;

    Ok(())
}
