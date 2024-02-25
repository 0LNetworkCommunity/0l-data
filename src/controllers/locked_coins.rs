use std::{
    sync::Arc,
    vec,
};
use arrow_array::{
     FixedSizeBinaryArray, RecordBatch, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use axum::{http::StatusCode, response::IntoResponse, Extension};
use datafusion::{
    dataframe::DataFrame,
    datasource::MemTable,
    execution::context::SessionContext,
    logical_expr::{coalesce, col, lit, JoinType},
    scalar::ScalarValue,
    sql::TableReference,
};
use ethereum_types::U256;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;

use crate::{app_state::AppState, V0_TIMESTAMP};

async fn get_slow_wallet_balances(app_state: &AppState) -> MemTable {
    let params = &[("database", app_state.clickhouse_database.clone())];

    let qs = serde_urlencoded::to_string(params).unwrap();

    let client = reqwest::Client::new();
    let res = client
        .post(format!("{}/?{}", app_state.clickhouse_host.clone(), qs))
        .header("X-ClickHouse-User", app_state.clickhouse_username.clone())
        .header("X-ClickHouse-Key", app_state.clickhouse_password.clone())
        .body(
            r#"
                SELECT
                    tupleElement("entry", 2) AS "time",
                    toUInt64(tupleElement("entry", 3)) AS "value",
                    tupleElement("entry", 4) AS "address"
                FROM (
                    SELECT
                        arrayElement(
                            arraySort(
                                (x) -> tupleElement(x, 1),
                                groupArray(
                                    tuple(
                                        "change_index",
                                        toUInt64(ceil("timestamp" / 1e6)),
                                        "balance",
                                        "address"
                                    )
                                )
                            ),
                            -1
                        ) AS "entry"
                    FROM
                        "coin_balance"
                    WHERE
                        "address" IN (
                            SELECT DISTINCT "address"
                            FROM "slow_wallet"
                        )
                    GROUP BY
                        "address",
                        ceil("timestamp" / 1e6)
                    ORDER BY
                        "address",
                        ceil("timestamp" / 1e6) ASC
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
        Field::new("time", DataType::UInt64, false),
        Field::new("value", DataType::UInt64, false),
        Field::new("address", DataType::FixedSizeBinary(32), false),
    ]);

    MemTable::try_new(Arc::new(schema), vec![batches]).unwrap()
}

async fn get_slow_wallet(app_state: &AppState) -> MemTable {
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
                    toUInt64(tupleElement("entry", 2)) AS "unlocked",
                    tupleElement("entry", 3) AS "time",
                    tupleElement("entry", 4) AS "address"
                FROM (
                    SELECT
                        arrayElement(
                            arraySort(
                                (x) -> tupleElement(x, 1),
                                groupArray(
                                    tuple(
                                        "change_index",
                                        "unlocked",
                                        toUInt64(ceil("timestamp" / 1e6)),
                                        "address"
                                    )
                                )
                            ),
                            -1
                        ) AS "entry"
                    FROM "slow_wallet"
                    GROUP BY
                      "address",
                      ceil("timestamp" / 1e6)
                    ORDER BY
                      "address",
                      ceil("timestamp" / 1e6) ASC
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
        Field::new("time", DataType::UInt64, false),
        Field::new("address", DataType::FixedSizeBinary(32), false),
    ]);

    MemTable::try_new(Arc::new(schema), vec![batches]).unwrap()
}

async fn get_locked_hist(hist: DataFrame) -> Vec<(u64, u64)> {
    let mut unlocked_mem: Option<u64> = None;
    let mut balance_mem = 0;

    hist.collect().await.unwrap().iter().map(|batch| {
        let timestamp = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("Failed to downcast time")
            .values()
            .to_vec();

        let balance_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("Failed to downcast unlocked")
            .into_iter()
            .collect::<Vec<Option<u64>>>();

        let unlocked_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("Failed to downcast unlocked")
            .into_iter()
            .collect::<Vec<Option<u64>>>();

        balance_col.iter().enumerate().map(|(index, balance)| {
            let balance = balance.unwrap_or(balance_mem);
            balance_mem = balance;

            let unlocked = if let Some(unlocked_value) = unlocked_col[index] {
                unlocked_mem = Some(unlocked_value);
                unlocked_value
            } else if let Some(unlocked_mem) = unlocked_mem {
                unlocked_mem
            } else {
                balance_mem
            };

            if unlocked <= balance {
                (timestamp[index], balance - unlocked)
            } else {
                (timestamp[index], 0)
            }
        }).collect::<Vec<(u64, u64)>>()
    }).flatten().collect()
}

#[derive(Serialize)]
struct Res {
    timestamps: Vec<u64>,
    locked: Vec<f64>,
}

pub async fn get(Extension(app_state): Extension<Arc<AppState>>) -> impl IntoResponse {
    let balance = get_slow_wallet_balances(&app_state).await;
    let slow_wallet = get_slow_wallet(&app_state).await;

    let ctx = SessionContext::new();

    ctx.register_table(
        TableReference::Bare {
            table: "slow_wallet".into(),
        },
        Arc::new(slow_wallet),
    )
    .unwrap();

    ctx.register_table(
        TableReference::Bare {
            table: "balance".into(),
        },
        Arc::new(balance),
    )
    .unwrap();

    let balance = ctx.table("balance").await.unwrap();
    let slow_wallet = ctx.table("slow_wallet").await.unwrap();

    let timestamps = slow_wallet
        .join(balance, JoinType::Full, &["time"], &["time"], None)
        .unwrap()
        .select(vec![coalesce(vec![
            col("slow_wallet.time"),
            col("balance.time"),
        ])
        .alias("timestamp")])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(vec![col("timestamp").sort(true, true)])
        .unwrap();

    let schema = Schema::new(vec![Field::new("timestamp", DataType::UInt64, true)]);
    let timestamps = MemTable::try_new(
        Arc::new(schema),
        timestamps.collect_partitioned().await.unwrap(),
    )
    .unwrap();

    ctx.register_table(
        TableReference::Bare {
            table: "timestamps".into(),
        },
        Arc::new(timestamps),
    )
    .unwrap();

    let mut timestamps: Vec<u64> = ctx.table("timestamps").await.unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Failed to downcast time")
                .values()
                .to_vec()
        })
        .flatten()
        .collect();

    let len = timestamps.len();
    let mut total = vec![0f64; len];

    let df = ctx
        .sql(
            r#"
              select distinct address from slow_wallet
            "#,
        )
        .await
        .unwrap();

    let addresses = df
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|batch| {
            let addresses = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("Failed to downcast time")
                .values()
                .to_vec();
            let (_, addresses, _) = unsafe { addresses.align_to::<U256>() };
            addresses
                .iter()
                .map(|addr| addr.clone())
                .collect::<Vec<U256>>()
        })
        .flatten()
        .collect::<Vec<U256>>();

    for address in addresses.into_iter() {
        let addr = Into::<[u8; 32]>::into(address);
        let mut addr = Vec::from(addr);
        addr.reverse();
        let addr = ScalarValue::FixedSizeBinary(32, Some(addr));

        let balance = ctx
            .table("balance")
            .await
            .unwrap()
            .filter(col("address").eq(lit(addr.clone())))
            .unwrap();

        let slow_wallet = ctx
            .table("slow_wallet")
            .await
            .unwrap()
            .filter(col("slow_wallet.address").eq(lit(addr.clone())))
            .unwrap();

        let hist = slow_wallet
            .join(balance, JoinType::Full, &["time"], &["time"], None)
            .unwrap()
            .select(vec![
                coalesce(vec![col("slow_wallet.time"), col("balance.time")]).alias("time"),
                col("balance.value").alias("balance"),
                col("slow_wallet.unlocked"),
            ])
            .unwrap()
            .sort(vec![col("time").sort(true, true)])
            .unwrap();

        let mut i = 0;
        let mut prev = 0f64;

        let locked_hist = get_locked_hist(hist).await;
        for (timestamp, locked) in locked_hist {
            while timestamps[i] < timestamp {
                total[i] += prev;
                i += 1;
            }

            if timestamps[i] == timestamp {
                prev = (locked as f64) / 1e6;
                total[i] += prev;
                i += 1;
            }
        }

        while i < timestamps.len() {
            total[i] += prev;
            i += 1;
        }
    }

    for i in 0..timestamps.len() {
        if timestamps[i] == 0 {
            timestamps[i] = V0_TIMESTAMP;
        } else {
            break;
        }
    }

    let mut locked_coins = Vec::with_capacity(timestamps.len());
    for i in 0..timestamps.len() {
        locked_coins.push((timestamps[i], total[i]));
    }

    let last_timestamp = timestamps.last().unwrap_or(&V0_TIMESTAMP);

    let locked_coins = ol_data::resample(V0_TIMESTAMP, *last_timestamp, 3600, &locked_coins);

    axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::to_string(&&locked_coins)
            .unwrap(),
        )
        .unwrap()
}
