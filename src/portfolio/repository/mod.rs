use crate::portfolio::{
    position::{InstrumentId, Position},
    repository::error::RepositoryError,
    Balance,
};
use barter_integration::model::{Market, MarketId};
use uuid::Uuid;

/// Barter repository module specific errors.
pub mod error;

/// In-Memory repository for convenient state keeping. No fault tolerant guarantees.
pub mod in_memory;

/// Redis repository for state keeping.
pub mod redis;

/// Handles the reading & writing of a [`Position`] to/from the persistence layer.
pub trait PositionHandler {
    /// Upsert the open [`Position`] using it's [`InstrumentId`].
    fn set_open_position(&mut self, position: Position) -> Result<(), RepositoryError>;

    /// Get all open [`Position`] using the [`InstrumentId`] provided.
    fn get_open_instrument_positions(
        &self,
        instrument_id: &InstrumentId,
    ) -> Result<Vec<Position>, RepositoryError>;

    /// Get open [`Position`]s associated with some markets.
    fn get_open_markets_positions<'a, Markets: Iterator<Item = &'a Market>>(
        &self,
        engine_id: Uuid,
        markets: Markets,
    ) -> Result<Vec<Position>, RepositoryError>;

    /// Get all open [`Position`]s associated with a Portfolio.
    fn get_all_open_positions(&self) -> Result<Vec<Position>, RepositoryError>;

    /// Get an open [`Position`] by the [`InstrumentId`] and signal_id.
    fn get_open_position(
        &self,
        instrument_id: &InstrumentId,
        signal_id: &Uuid,
    ) -> Result<Option<Position>, RepositoryError>;

    /// Remove specific [`Position`] by [`InstrumentId`] and signal_id.
    fn remove_position(
        &mut self,
        instrument_id: &InstrumentId,
        signal_id: &Uuid,
    ) -> Result<Option<Position>, RepositoryError>;

    /// Append an exited [`Position`] to the Portfolio's exited position list.
    fn set_exited_position(
        &mut self,
        engine_id: Uuid,
        position: Position,
    ) -> Result<(), RepositoryError>;

    /// Get every exited [`Position`] associated with the engine_id.
    fn get_exited_positions(&self, engine_id: Uuid) -> Result<Vec<Position>, RepositoryError>;
}

/// Handles the reading & writing of a Portfolio's current balance to/from the persistence layer.
pub trait BalanceHandler {
    /// Upsert the Portfolio [`Balance`] at the engine_id.
    fn set_balance(&mut self, engine_id: Uuid, balance: Balance) -> Result<(), RepositoryError>;
    /// Get the Portfolio [`Balance`] using the engine_id provided.
    fn get_balance(&self, engine_id: Uuid) -> Result<Balance, RepositoryError>;
}

/// Handles the reading & writing of a Portfolio's statistics for each of it's
/// markets, where each market is represented by a [`MarketId`].
pub trait StatisticHandler<Statistic> {
    /// Upsert the market statistics at the [`MarketId`] provided.
    fn set_statistics(
        &mut self,
        market_id: MarketId,
        statistic: Statistic,
    ) -> Result<(), RepositoryError>;
    /// Get the market statistics using the [`MarketId`] provided.
    fn get_statistics(&self, market_id: &MarketId) -> Result<Statistic, RepositoryError>;
}

/// Communicates a String represents a unique identifier for all a Portfolio's exited [`Position`]s.
/// Used to append new exited [`Position`]s to the entry in the [`PositionHandler`].
pub type ExitedPositionsId = String;

/// Returns the unique identifier for a Portfolio's exited [`Position`]s, given an engine_id.
pub fn determine_exited_positions_id(engine_id: Uuid) -> ExitedPositionsId {
    format!("positions_exited_{}", engine_id)
}
