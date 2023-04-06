use crate::{
    portfolio::{
        error::PortfolioError,
        position::{determine_instrument_id, InstrumentId, Position},
        repository::{
            determine_exited_positions_id, error::RepositoryError, BalanceHandler, PositionHandler,
            StatisticHandler,
        },
        Balance,
    },
    statistic::summary::PositionSummariser,
};
use barter_integration::model::{Market, MarketId};
use r2d2::{Pool, PooledConnection};
use r2d2_redis::{
    redis::{Commands, ErrorKind},
    RedisConnectionManager,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fmt::{Debug, Formatter},
    marker::PhantomData,
};
use uuid::Uuid;

/// Configuration for constructing a [`RedisRepository`] via the new() constructor method.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    pub uri: String,
}

/// Redis persisted repository that implements [`PositionHandler`], [`BalanceHandler`],
/// & [`PositionSummariser`]. Used by a Portfolio implementation to persist the Portfolio state,
/// including total equity, available cash & Positions.
pub struct RedisRepository<Statistic>
where
    Statistic: PositionSummariser + Serialize + DeserializeOwned,
{
    pool: Pool<RedisConnectionManager>,
    _statistic_marker: PhantomData<Statistic>,
}

impl<Statistic> PositionHandler for RedisRepository<Statistic>
where
    Statistic: PositionSummariser + Serialize + DeserializeOwned,
{
    fn set_open_position(&mut self, position: Position) -> Result<(), RepositoryError> {
        let position_string = serde_json::to_string(&position)?;

        let mut conn = self.pool.get().unwrap();
        conn.set(
            format!("{}_{}", position.instrument_id, position.signal_id),
            position_string,
        )
        .map_err(|_| RepositoryError::WriteError)
    }

    fn get_open_instrument_positions(
        &self,
        instrument_id: &InstrumentId,
    ) -> Result<Vec<Position>, RepositoryError> {
        let mut conn = self.conn();
        let mut positions = vec![];
        let keys: Vec<String> = conn
            .keys(instrument_id)
            .map_err(|_| RepositoryError::ReadError)?;
        for k in keys {
            let position_value: String = conn.get(k).map_err(|_| RepositoryError::ReadError)?;
            let p = serde_json::from_str::<Position>(&position_value)?;
            positions.push(p);
        }

        Ok(positions)
    }

    fn get_open_markets_positions<'a, Markets: Iterator<Item = &'a Market>>(
        &self,
        engine_id: Uuid,
        markets: Markets,
    ) -> Result<Vec<Position>, RepositoryError> {
        let mut positions = vec![];
        for market in markets {
            let mut p = self.get_open_instrument_positions(&determine_instrument_id(
                engine_id,
                &market.exchange,
                &market.instrument,
            ))?;
            positions.append(&mut p);
        }
        Ok(positions)
    }

    fn get_all_open_positions(&self) -> Result<Vec<Position>, RepositoryError> {
        let mut conn = self.conn();
        let mut positions = vec![];
        let keys: Vec<String> = conn
            .keys("instrument_")
            .map_err(|_| RepositoryError::ReadError)?;
        for k in keys {
            let position_value: String = conn.get(k).map_err(|_| RepositoryError::ReadError)?;
            let p = serde_json::from_str::<Position>(&position_value)?;
            positions.push(p);
        }

        Ok(positions)
    }

    fn remove_positions(
        &mut self,
        instrument_id: &String,
    ) -> Result<Vec<Position>, RepositoryError> {
        let position = self.get_open_instrument_positions(instrument_id)?;

        let mut conn = self.conn();
        conn.del(instrument_id)
            .map_err(|_| RepositoryError::DeleteError)?;

        Ok(position)
    }

    fn set_exited_position(
        &mut self,
        engine_id: Uuid,
        position: Position,
    ) -> Result<(), RepositoryError> {
        let mut conn = self.conn();
        conn.lpush(
            determine_exited_positions_id(engine_id),
            serde_json::to_string(&position)?,
        )
        .map_err(|_| RepositoryError::WriteError)
    }

    fn get_exited_positions(&self, engine_id: Uuid) -> Result<Vec<Position>, RepositoryError> {
        let mut conn = self.conn();
        conn.get(determine_exited_positions_id(engine_id))
            .or_else(|err| match err.kind() {
                ErrorKind::TypeError => Ok(Vec::<String>::new()),
                _ => Err(RepositoryError::ReadError),
            })?
            .iter()
            .map(|position| serde_json::from_str::<Position>(position))
            .collect::<Result<Vec<Position>, serde_json::Error>>()
            .map_err(RepositoryError::JsonSerDeError)
    }
}

impl<Statistic> BalanceHandler for RedisRepository<Statistic>
where
    Statistic: PositionSummariser + Serialize + DeserializeOwned,
{
    fn set_balance(&mut self, engine_id: Uuid, balance: Balance) -> Result<(), RepositoryError> {
        let balance_string = serde_json::to_string(&balance)?;

        let mut conn = self.conn();
        conn.set(Balance::balance_id(engine_id), balance_string)
            .map_err(|_| RepositoryError::WriteError)
    }

    fn get_balance(&self, engine_id: Uuid) -> Result<Balance, RepositoryError> {
        let mut conn = self.conn();
        let balance_value: String = conn
            .get(Balance::balance_id(engine_id))
            .map_err(|_| RepositoryError::ReadError)?;

        Ok(serde_json::from_str::<Balance>(&balance_value)?)
    }
}

impl<Statistic> StatisticHandler<Statistic> for RedisRepository<Statistic>
where
    Statistic: PositionSummariser + Serialize + DeserializeOwned,
{
    fn set_statistics(
        &mut self,
        market_id: MarketId,
        statistic: Statistic,
    ) -> Result<(), RepositoryError> {
        let mut conn = self.conn();
        conn.set(market_id.0, serde_json::to_string(&statistic)?)
            .map_err(|_| RepositoryError::WriteError)
    }

    fn get_statistics(&self, market_id: &MarketId) -> Result<Statistic, RepositoryError> {
        let mut conn = self.conn();
        let statistics: String = conn
            .get(&market_id.0)
            .map_err(|_| RepositoryError::ReadError)?;

        serde_json::from_str(&statistics).map_err(RepositoryError::JsonSerDeError)
    }
}

impl<Statistic: PositionSummariser> Debug for RedisRepository<Statistic>
where
    Statistic: PositionSummariser + Serialize + DeserializeOwned,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisRepository").finish()
    }
}

impl<Statistic: PositionSummariser> RedisRepository<Statistic>
where
    Statistic: PositionSummariser + Serialize + DeserializeOwned,
{
    /// Constructs a new [`RedisRepository`] component using the provided Redis connection struct.
    pub fn new(pool: Pool<RedisConnectionManager>) -> Self {
        Self {
            pool,
            _statistic_marker: PhantomData::<Statistic>::default(),
        }
    }

    pub fn conn(&self) -> PooledConnection<RedisConnectionManager> {
        self.pool.get().expect("Failed get a connection from pool")
    }

    /// Returns a [`RedisRepositoryBuilder`] instance.
    pub fn builder() -> RedisRepositoryBuilder<Statistic> {
        RedisRepositoryBuilder::new()
    }

    /// Establish & return a Redis connection.
    pub fn setup_redis_connection(cfg: Config) -> Pool<RedisConnectionManager> {
        let manager = RedisConnectionManager::new(cfg.uri).expect("Failed to create Redis client");
        Pool::builder()
            .build(manager)
            .expect("Failed to create Redis client pool")
    }
}

/// Builder to construct [`RedisRepository`] instances.
#[derive(Default)]
pub struct RedisRepositoryBuilder<Statistic>
where
    Statistic: PositionSummariser + Serialize + DeserializeOwned,
{
    conn: Option<Pool<RedisConnectionManager>>,
    _statistic_marker: PhantomData<Statistic>,
}

impl<Statistic: PositionSummariser> RedisRepositoryBuilder<Statistic>
where
    Statistic: PositionSummariser + Serialize + DeserializeOwned,
{
    pub fn new() -> Self {
        Self {
            conn: None,
            _statistic_marker: PhantomData::<Statistic>::default(),
        }
    }

    pub fn conn(self, value: Pool<RedisConnectionManager>) -> Self {
        Self {
            conn: Some(value),
            ..self
        }
    }

    pub fn build(self) -> Result<RedisRepository<Statistic>, PortfolioError> {
        Ok(RedisRepository {
            pool: self.conn.ok_or(PortfolioError::BuilderIncomplete("conn"))?,
            _statistic_marker: PhantomData::<Statistic>::default(),
        })
    }
}

impl<Statistic> Debug for RedisRepositoryBuilder<Statistic>
where
    Statistic: PositionSummariser + Serialize + DeserializeOwned,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisRepositoryBuilder")
            .field("conn", &"Option<redis::Connection>")
            .field("_statistic_market", &self._statistic_marker)
            .finish()
    }
}
