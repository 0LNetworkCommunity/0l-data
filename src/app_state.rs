#[derive(Clone)]
pub struct AppState {
    pub clickhouse_username: String,
    pub clickhouse_password: String,
    pub clickhouse_host: String,
    pub clickhouse_database: String,
}
