use crate::{
    portfolio::{
        position::{determine_instrument_id, InstrumentId, Position},
        repository::{
            determine_exited_positions_id, error::RepositoryError, BalanceHandler, PositionHandler,
            StatisticHandler,
        },
        Balance, BalanceId,
    },
    statistic::summary::PositionSummariser,
};
use barter_integration::model::{Market, MarketId};
use std::collections::HashMap;
use uuid::Uuid;

/// In-Memory repository for Proof Of Concepts. Implements [`PositionHandler`], [`BalanceHandler`]
/// & [`StatisticHandler`]. Used by a Proof Of Concept Portfolio implementation to
/// save the current equity, available cash, Positions, and market pair statistics.
/// **Careful in production - no fault tolerant guarantees!**
#[derive(Debug, Default)]
pub struct InMemoryRepository<Statistic: PositionSummariser> {
    open_positions: HashMap<InstrumentId, HashMap<Uuid, Position>>,
    closed_positions: HashMap<String, Vec<Position>>,
    current_balances: HashMap<BalanceId, Balance>,
    statistics: HashMap<MarketId, Statistic>,
}

impl<Statistic: PositionSummariser> PositionHandler for InMemoryRepository<Statistic> {
    fn set_open_position(&mut self, position: Position) -> Result<(), RepositoryError> {
        let instrument_id = position.instrument_id.clone();
        let signal_id = position.signal_id;

        if let Some(positions) = self.open_positions.get_mut(&instrument_id) {
            positions.insert(signal_id, position);
        } else {
            let mut positions = HashMap::new();
            positions.insert(signal_id, position);
            self.open_positions.insert(instrument_id, positions);
        }
        Ok(())
    }

    fn get_open_instrument_positions(
        &mut self,
        instrument_id: &InstrumentId,
    ) -> Result<Vec<Position>, RepositoryError> {
        Ok(self
            .open_positions
            .get(instrument_id)
            .map(|p| p.values().map(Position::clone).collect())
            .unwrap_or(vec![]))
    }

    fn get_open_markets_positions<'a, Markets: Iterator<Item = &'a Market>>(
        &mut self,
        engine_id: Uuid,
        markets: Markets,
    ) -> Result<Vec<Position>, RepositoryError> {
        let mut positions = vec![];
        for market in markets {
            let position_id =
                determine_instrument_id(engine_id, &market.exchange, &market.instrument);
            if let Some(p) = self.open_positions.get(&position_id) {
                positions.append(&mut p.values().map(Position::clone).collect());
            }
        }
        Ok(positions)
    }

    fn remove_positions(
        &mut self,
        instrument_id: &String,
    ) -> Result<Vec<Position>, RepositoryError> {
        let positions = self
            .open_positions
            .remove(instrument_id)
            .ok_or(RepositoryError::DeleteError)?;
        Ok(positions.values().map(Position::clone).collect())
    }

    fn set_exited_position(
        &mut self,
        engine_id: Uuid,
        position: Position,
    ) -> Result<(), RepositoryError> {
        let exited_positions_key = determine_exited_positions_id(engine_id);

        match self.closed_positions.get_mut(&exited_positions_key) {
            None => {
                self.closed_positions
                    .insert(exited_positions_key, vec![position]);
            }
            Some(closed_positions) => closed_positions.push(position),
        }
        Ok(())
    }

    fn get_exited_positions(&mut self, engine_id: Uuid) -> Result<Vec<Position>, RepositoryError> {
        Ok(self
            .closed_positions
            .get(&determine_exited_positions_id(engine_id))
            .map(Vec::clone)
            .unwrap_or_else(Vec::new))
    }
}

impl<Statistic: PositionSummariser> BalanceHandler for InMemoryRepository<Statistic> {
    fn set_balance(&mut self, engine_id: Uuid, balance: Balance) -> Result<(), RepositoryError> {
        self.current_balances
            .insert(Balance::balance_id(engine_id), balance);
        Ok(())
    }

    fn get_balance(&mut self, engine_id: Uuid) -> Result<Balance, RepositoryError> {
        self.current_balances
            .get(&Balance::balance_id(engine_id))
            .copied()
            .ok_or(RepositoryError::ExpectedDataNotPresentError)
    }
}

impl<Statistic: PositionSummariser> StatisticHandler<Statistic> for InMemoryRepository<Statistic> {
    fn set_statistics(
        &mut self,
        market_id: MarketId,
        statistic: Statistic,
    ) -> Result<(), RepositoryError> {
        self.statistics.insert(market_id, statistic);
        Ok(())
    }

    fn get_statistics(&mut self, market_id: &MarketId) -> Result<Statistic, RepositoryError> {
        self.statistics
            .get(market_id)
            .copied()
            .ok_or(RepositoryError::ExpectedDataNotPresentError)
    }
}

impl<Statistic: PositionSummariser> InMemoryRepository<Statistic> {
    /// Constructs a new [`InMemoryRepository`] component.
    pub fn new() -> Self {
        Self {
            open_positions: HashMap::new(),
            closed_positions: HashMap::new(),
            current_balances: HashMap::new(),
            statistics: HashMap::new(),
        }
    }
}
