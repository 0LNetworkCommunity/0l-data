use std::sync::Arc;

use arrow_array::{Float64Array, UInt64Array};
use axum::{http::StatusCode, response::IntoResponse, Extension};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{app_state::AppState, V0_TIMESTAMP};

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
                    tupleElement("entry", 3) AS "time"
                FROM (
                    SELECT
                    arrayElement(
                        arraySort(
                            (x) -> tupleElement(x, 1),
                            groupArray(
                                tuple(
                                    "change_index",
                                    "amount",
                                    toInt64(ceil("timestamp" / 1e6))
                                )
                            )
                        ),
                        -1
                    ) AS "entry"
                    FROM "total_supply"
                    GROUP BY ceil("timestamp" / 1e6)
                    ORDER BY ceil("timestamp" / 1e6) ASC
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

            let timestamps = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast")
                .values()
                .to_vec();

            for i in 0..values.len() {
                let timestamp = if timestamps[i] == 0 {
                    V0_TIMESTAMP
                } else {
                    timestamps[i]
                };
                total_supply.push((timestamp, values[i]));
            }
        }
    }

    let last_timestamp = total_supply.last().unwrap().0;
    let last_timestamp = last_timestamp + (last_timestamp % 3600);

    let total_supply = ol_data::resample(V0_TIMESTAMP, last_timestamp, 3600, &total_supply);

    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&&total_supply).unwrap())
        .unwrap()
}
