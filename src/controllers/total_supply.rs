use std::sync::Arc;

use arrow_array::{Float64Array, UInt64Array};
use axum::{http::StatusCode, response::IntoResponse, Extension};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::app_state::AppState;

pub async fn get(Extension(app_state): Extension<Arc<AppState>>) -> impl IntoResponse {
    let params = &[("database", app_state.clickhouse_database.clone())];
    let qs = serde_urlencoded::to_string(params).unwrap();

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{}/?{}", app_state.clickhouse_host.clone(), qs,))
        .header("X-ClickHouse-User", app_state.clickhouse_username.clone())
        .header("X-ClickHouse-Key", app_state.clickhouse_password.clone())
        .body(
            r#"
                SELECT
                    tupleElement("entry", 2) / 1e6 AS "value",
                    tupleElement("entry", 3) AS "version"
                FROM (
                    SELECT
                    arrayElement(
                        arraySort(
                            (x) -> tupleElement(x, 1),
                            groupArray(
                                tuple(
                                    "change_index",
                                    "amount",
                                    "version"
                                )
                            )
                        ),
                        -1
                    ) AS "entry"
                    FROM "total_supply"
                    GROUP BY "version"
                    ORDER BY "version" ASC
                )
                FORMAT Parquet
            "#,
        )
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(res)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();

    let mut total_supply: Vec<(u64, f64)> = Vec::new();

    for batch in parquet_reader {
        if let Ok(batch) = batch {
            total_supply.reserve(batch.num_rows());
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Failed to downcast")
                .values()
                .to_vec();

            let versions = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast")
                .values()
                .to_vec();

            for i in 0..values.len() {
                total_supply.push((versions[i], values[i]));
            }
        }
    }

    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&&total_supply).unwrap())
        .unwrap()
}
