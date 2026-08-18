//! Тонкая обёртка над `PgPool` — единственный источник истины по-прежнему
//! `policy.policy_template` (мигрировано в Ф2, `V024__policy_template_sender_id.sql`),
//! эта таблица не дублируется. Каждый эндпоинт-модуль (list/search, preview,
//! bulk import/export) пишет свои запросы напрямую против `Store::pool`, не
//! через общие методы здесь — так параллельные фичи не конкурируют за один
//! файл.

use sqlx::postgres::{PgPool, PgPoolOptions};

#[derive(Clone)]
pub struct Store {
    pub pool: PgPool,
}

impl Store {
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new().max_connections(10).connect(database_url).await?;
        Ok(Self { pool })
    }

    /// readyz dependency check — тот же паттерн, что Go-сервисы
    /// (`internal/health/health.go`: readyz реально пингует зависимости,
    /// не просто bool).
    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}
