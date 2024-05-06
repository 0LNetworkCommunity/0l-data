mod app_state;
mod controllers;

use std::sync::Arc;

use app_state::AppState;
use axum::{http::Method, routing::get, Extension, Router};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

const V0_TIMESTAMP: u64 = 1712696400;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app_state = AppState {
        clickhouse_host: std::env::var("CLICKHOUSE_HOST").unwrap_or("http://127.0.0.1:8123".into()),
        clickhouse_username: std::env::var("CLICKHOUSE_USERNAME").unwrap_or("default".into()),
        clickhouse_password: std::env::var("CLICKHOUSE_PASSWORD").unwrap_or("default".into()),
        clickhouse_database: std::env::var("CLICKHOUSE_DATABASE").unwrap_or("olfyi".into()),
    };

    let port = std::env::var("PORT").unwrap_or("4000".into());

    // build our application with a single route
    let app = Router::new()
        .route("/total-supply", get(controllers::total_supply::get))
        .route("/locked-coins", get(controllers::locked_coins::get))
        .route(
            "/historical-balance/:address",
            get(controllers::historical_balance::get),
        )
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET]),
        )
        .layer(Extension(Arc::new(app_state)));

    // run our app with hyper, listening globally on port 3000
    let interface = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&interface).await?;

    println!("Listenning on {interface}");

    axum::serve(listener, app).await?;

    Ok(())
}
