use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use axum::{extract::Path, http::StatusCode, response::IntoResponse, Extension};
use datafusion::{
    datasource::MemTable,
    execution::context::SessionContext,
    logical_expr::{coalesce, col, JoinType},
    sql::TableReference,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;

#[derive(Serialize, Deserialize)]
struct Response {
    version: Vec<u64>,
    unlocked: Vec<u64>,
    balance: Vec<u64>,
    locked: Vec<u64>,
    timestamp: Vec<u64>,
}

async fn get_transactions_timestamps(
    app_state: &AppState,
    versions_in: &Vec<u64>,
) -> (Vec<u64>, Vec<u64>) {
    let params = &[
        ("database", app_state.clickhouse_database.clone()),
    ];

    let qs = serde_urlencoded::to_string(params).unwrap();

    let form = reqwest::multipart::Form::new()
        .text(
            "query",
            r#"
                SELECT "version", "timestamp"
                FROM "block_metadata_transaction"
                WHERE "version" IN {versions:Array(UInt64)}

                UNION ALL

                SELECT "version", "timestamp"
                FROM "state_checkpoint_transaction"
                WHERE "version" IN {versions:Array(UInt64)}

                UNION ALL

                SELECT "version", "timestamp"
                FROM "user_transaction"
                WHERE "version" IN {versions:Array(UInt64)}

                FORMAT Parquet
            "#
        )
        .text("param_versions", serde_json::to_string(versions_in).unwrap());

    let client = reqwest::Client::new();
    let res = client
        .post(
            format!(
                "{}/?{}",
                app_state.clickhouse_host.clone(),
                qs,
            )
        )
        .header("X-ClickHouse-User", app_state.clickhouse_username.clone())
        .header("X-ClickHouse-Key", app_state.clickhouse_password.clone())
        .multipart(form)
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

    let batches = parquet_reader
        .into_iter()
        .map(|it| it.unwrap())
        .collect::<Vec<RecordBatch>>();

    let mut version = Vec::new();
    let mut timestamp = Vec::new();

    for batch in batches {
        version.append(
            &mut batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast unlocked")
                .values()
                .to_vec(),
        );

        timestamp.append(
            &mut batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast unlocked")
                .values()
                .to_vec(),
        );
    }

    (version, timestamp)
}

async fn get_genesis_transactions(
    app_state: &AppState,
    versions: &Vec<u64>,
) -> (Vec<u64>, Vec<u64>) {
    if versions.len() == 0 {
        return (vec![], vec![]);
    }

    let params = &[
        ("database", app_state.clickhouse_database.clone()),
        ("param_start", versions.first().unwrap().to_string()),
        ("param_end", versions.last().unwrap().to_string()),
    ];

    let qs = serde_urlencoded::to_string(params).unwrap();

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{}/?{}", app_state.clickhouse_host.clone(), qs))
        .header("X-ClickHouse-User", app_state.clickhouse_username.clone())
        .header("X-ClickHouse-Key", app_state.clickhouse_password.clone())
        .body(
            r#"
                SELECT "version"
                FROM "genesis_transaction"
                WHERE "version" BETWEEN {start:UInt64} AND {end:UInt64}
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

    let batches = parquet_reader
        .into_iter()
        .map(|it| it.unwrap())
        .collect::<Vec<RecordBatch>>();

    let mut version = Vec::new();

    for batch in batches {
        version.append(
            &mut batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast unlocked")
                .values()
                .to_vec(),
        );
    }

    if version.len() == 0 {
        return (vec![], vec![]);
    }

    let next_versions = version.iter().map(|value| value + 1).collect::<Vec<_>>();

    let params = &[
        ("database", app_state.clickhouse_database.clone()),
        ("param_versions", serde_json::to_string(&next_versions).unwrap()),
    ];

    let qs = serde_urlencoded::to_string(params).unwrap();

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{}/?{}", app_state.clickhouse_host.clone(), qs))
        .header("X-ClickHouse-User", app_state.clickhouse_username.clone())
        .header("X-ClickHouse-Key", app_state.clickhouse_password.clone())
        .body(
            r#"
                SELECT "version", "timestamp"
                FROM "block_metadata_transaction"
                WHERE "version" IN {versions:Array(UInt64)}

                UNION ALL

                SELECT "version", "timestamp"
                FROM "state_checkpoint_transaction"
                WHERE "version" IN {versions:Array(UInt64)}

                UNION ALL

                SELECT "version", "timestamp"
                FROM "user_transaction"
                WHERE "version" IN {versions:Array(UInt64)}

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

    let batches = parquet_reader
        .into_iter()
        .map(|it| it.unwrap())
        .collect::<Vec<RecordBatch>>();

    let mut version = Vec::new();
    let mut timestamp = Vec::new();

    for batch in batches {
        version.append(
            &mut batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast unlocked")
                .values()
                .to_vec()
                .iter()
                .map(|value| value - 1)
                .collect::<Vec<_>>(),
        );

        timestamp.append(
            &mut batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast unlocked")
                .values()
                .to_vec()
                .iter()
                .map(|value| value - 1)
                .collect::<Vec<_>>(),
        );
    }

    return (version, timestamp);
}

async fn get_timestamps(app_state: &AppState, versions: &Vec<u64>) -> MemTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("version", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt64, false),
    ]));

    let (version, timestamp) = get_genesis_transactions(app_state, versions).await;

    let mut batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(version)),
                Arc::new(UInt64Array::from(timestamp)),
            ],
        ).unwrap()
    ];

    let (version, timestamp) = get_transactions_timestamps(app_state, versions).await;

    batches.push(
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(version)),
                Arc::new(UInt64Array::from(timestamp)),
            ],
        ).unwrap()
    );

    MemTable::try_new(schema.clone(), vec![batches]).unwrap()
}

async fn get_slow_wallet(app_state: &AppState, address: &str) -> MemTable {
    let params = &[
        ("database", app_state.clickhouse_database.clone()),
        ("param_address", address.to_owned()),
    ];

    let qs = serde_urlencoded::to_string(params).unwrap();

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{}/?{}", app_state.clickhouse_host.clone(), qs))
        .header("X-ClickHouse-User", app_state.clickhouse_username.clone())
        .header("X-ClickHouse-Key", app_state.clickhouse_password.clone())
        .body(
            r#"
                SELECT
                    toUInt64(tupleElement("entry", 2)) AS "unlocked",
                    tupleElement("entry", 3) AS "version"
                FROM (
                    SELECT
                        arrayElement(
                            arraySort(
                                (x) -> tupleElement(x, 1),
                                groupArray(
                                    tuple(
                                        "change_index",
                                        "unlocked",
                                        "version"
                                    )
                                )
                            ),
                            -1
                        ) AS "entry"
                    FROM "slow_wallet"
                    WHERE
                        "address" = reinterpretAsUInt256(reverse(unhex({address:String})))
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

    let batches = parquet_reader
        .into_iter()
        .map(|it| it.unwrap())
        .collect::<Vec<RecordBatch>>();

    let schema = Schema::new(vec![
        Field::new("unlocked", DataType::UInt64, false),
        Field::new("version", DataType::UInt64, false),
    ]);

    MemTable::try_new(Arc::new(schema), vec![batches]).unwrap()
}

async fn get_historical_balance(app_state: &AppState, address: &str) -> MemTable {
    let params = &[
        ("database", app_state.clickhouse_database.clone()),
        ("param_address", address.to_owned()),
    ];

    let qs = serde_urlencoded::to_string(params).unwrap();

    let client = reqwest::Client::new();

    let res = client
        .post(format!("{}/?{}", app_state.clickhouse_host.clone(), qs))
        .header("X-ClickHouse-User", app_state.clickhouse_username.clone())
        .header("X-ClickHouse-Key", app_state.clickhouse_password.clone())
        .body(
            r#"
                SELECT
                    toUInt64(tupleElement("entry", 2)) AS "value",
                    tupleElement("entry", 3) AS "version"
                FROM (
                    SELECT
                        arrayElement(
                            arraySort(
                                (x) -> tupleElement(x, 1),
                                groupArray(
                                    tuple(
                                        "change_index",
                                        "balance",
                                        "version"
                                    )
                                )
                            ),
                            -1
                        ) AS "entry"
                    FROM "coin_balance"
                    WHERE
                        "address" = reinterpretAsUInt256(reverse(unhex({address:String})))
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

    let batches = parquet_reader
        .into_iter()
        .map(|it| it.unwrap())
        .collect::<Vec<RecordBatch>>();

    let schema = Schema::new(vec![
        Field::new("value", DataType::UInt64, false),
        Field::new("version", DataType::UInt64, false),
    ]);

    MemTable::try_new(Arc::new(schema), vec![batches]).unwrap()
}

pub async fn get(
    Extension(state): Extension<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    let historical_balance = get_historical_balance(&state, &address).await;
    let slow_wallet = get_slow_wallet(&state, &address).await;

    let ctx = SessionContext::new();

    ctx.register_table(
        TableReference::Bare {
            table: "historical_balance".into(),
        },
        Arc::new(historical_balance),
    )
    .unwrap();

    ctx.register_table(
        TableReference::Bare {
            table: "slow_wallet".into(),
        },
        Arc::new(slow_wallet),
    )
    .unwrap();

    let slow_wallet = ctx.table("slow_wallet".to_owned()).await.unwrap();
    let historical_balance = ctx.table("historical_balance".to_owned()).await.unwrap();

    let df = slow_wallet
        .join(
            historical_balance,
            JoinType::Full,
            &["version"],
            &["version"],
            None,
        )
        .unwrap()
        .select(vec![
            coalesce(vec![
                col("historical_balance.version"),
                col("slow_wallet.version"),
            ])
            .alias("version"),
            col("unlocked"),
            col("historical_balance.value").alias("balance"),
        ])
        .unwrap()
        .sort(vec![col("version").sort(true, true)])
        .unwrap();

    let mut unlocked_mem: Option<u64> = None;
    let mut balance_mem = 0;

    let schema = Schema::new(vec![
        Field::new("version", DataType::UInt64, false),
        Field::new("unlocked", DataType::UInt64, true),
        Field::new("balance", DataType::UInt64, true),
    ]);

    let batches = df
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|batch| {
            let version = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast time")
                .values()
                .to_vec();

            let unlocked_in = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast unlocked")
                .into_iter()
                .collect::<Vec<Option<u64>>>();

            let balance_in = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast balance")
                .into_iter()
                .collect::<Vec<Option<u64>>>();

            let num_rows = batch.num_rows();

            let mut unlocked_out = Vec::with_capacity(num_rows);
            let mut balance_out = Vec::with_capacity(num_rows);

            for i in 0..batch.num_rows() {
                let balance = balance_in[i].unwrap_or(balance_mem);
                balance_mem = balance;

                let unlocked: u64;
                if let Some(unlocked_value) = unlocked_in[i] {
                    unlocked = unlocked_value;
                    unlocked_mem = Some(unlocked_value);
                } else {
                    if let Some(unlocked_mem) = unlocked_mem {
                        unlocked = unlocked_mem;
                    } else {
                        unlocked = balance_mem;
                    }
                }

                unlocked_out.push(unlocked);
                balance_out.push(balance);
            }

            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(UInt64Array::from(version)),
                    Arc::new(UInt64Array::from(unlocked_out)),
                    Arc::new(UInt64Array::from(balance_out)),
                ],
            )
            .unwrap();

            return batch;
        })
        .collect::<Vec<RecordBatch>>();

    let table = MemTable::try_new(
        Arc::new(schema),
        vec![batches]
    ).unwrap();

    let ctx = SessionContext::new();

    ctx.register_table(
        TableReference::Bare {
            table: "history".into(),
        },
        Arc::new(table),
    )
    .unwrap();

    let df = ctx
        .sql(
            r#"
                SELECT
                    (
                        CASE
                            WHEN "unlocked" > "balance" THEN "balance"
                            ELSE "unlocked"
                        END
                    ) as "unlocked",
                    "balance",
                    "balance" - (
                        CASE
                            WHEN "unlocked" > "balance" THEN "balance"
                            ELSE "unlocked"
                        END
                    ) as "locked",
                    "version"
                FROM "history"
                ORDER BY "version" ASC
            "#,
        )
        .await
        .unwrap();

    let mut unlocked = Vec::new();
    let mut locked = Vec::new();
    let mut version = Vec::new();
    let mut balance = Vec::new();
    let mut timestamp = Vec::new();

    for batch in df.collect().await.unwrap() {
        unlocked.append(
            &mut batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast unlocked")
                .values()
                .to_vec(),
        );

        balance.append(
            &mut batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast balance")
                .values()
                .to_vec(),
        );

        locked.append(
            &mut batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast balance")
                .values()
                .to_vec(),
        );

        version.append(
            &mut batch
                .column(3)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast balance")
                .values()
                .to_vec(),
        );
    }

    let timestamp_table = get_timestamps(&state, &version).await;

    ctx.register_table(
        TableReference::Bare {
            table: "timestamp".into(),
        },
        Arc::new(timestamp_table),
    )
    .unwrap();

    let df = ctx
        .sql(
            r#"
                SELECT
                    "history"."version",
                    "timestamp"."timestamp"
                FROM "history"
                INNER JOIN "timestamp" ON "timestamp"."version" = "history"."version"
                ORDER BY "history"."version" ASC
            "#,
        )
        .await
        .unwrap();

    for batch in df.collect().await.unwrap() {
        timestamp.append(
            &mut batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast balance")
                .values()
                .to_vec(),
        );
    }

    assert_eq!(version.len(), timestamp.len());

    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::to_string(&Response {
                unlocked,
                balance,
                locked,
                version,
                timestamp,
            })
            .unwrap(),
        )
        .unwrap()
}
