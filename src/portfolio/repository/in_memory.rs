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
        &self,
        instrument_id: &InstrumentId,
    ) -> Result<Vec<Position>, RepositoryError> {
        Ok(self
            .open_positions
            .get(instrument_id)
            .map(|p| p.values().map(Position::clone).collect())
            .unwrap_or(vec![]))
    }

    fn get_open_markets_positions<'a, Markets: Iterator<Item = &'a Market>>(
        &self,
        engine_id: Uuid,
        markets: Markets,
    ) -> Result<Vec<Position>, RepositoryError> {
        let mut positions = vec![];
        for market in markets {
            let instrument_id =
                determine_instrument_id(engine_id, &market.exchange, &market.instrument);
            if let Some(p) = self.open_positions.get(&instrument_id) {
                positions.append(&mut p.values().map(Position::clone).collect());
            }
        }
        Ok(positions)
    }

    fn get_all_open_positions(&self) -> Result<Vec<Position>, RepositoryError> {
        let mut positions = vec![];
        for p in self.open_positions.values() {
            positions.append(&mut p.values().map(Position::clone).collect());
        }

        Ok(positions)
    }

    fn get_open_position(
        &self,
        instrument_id: &InstrumentId,
        signal_id: &Uuid,
    ) -> Result<Option<Position>, RepositoryError> {
        Ok(self
            .open_positions
            .get(instrument_id)
            .and_then(|instrument_position| instrument_position.get(signal_id))
            .cloned())
    }

    fn remove_position(
        &mut self,
        instrument_id: &InstrumentId,
        signal_id: &Uuid,
    ) -> Result<Option<Position>, RepositoryError> {
        let p = self
            .open_positions
            .get_mut(instrument_id)
            .and_then(|instrument_positions| instrument_positions.remove(signal_id))
            .ok_or(RepositoryError::DeleteError)?;

        Ok(Some(p))
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

    fn get_exited_positions(&self, engine_id: Uuid) -> Result<Vec<Position>, RepositoryError> {
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

    fn get_balance(&self, engine_id: Uuid) -> Result<Balance, RepositoryError> {
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

    fn get_statistics(&self, market_id: &MarketId) -> Result<Statistic, RepositoryError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::Fees;
    use crate::portfolio::position::PositionEnterer;
    use crate::statistic::summary::trading::TradingSummary;
    use crate::strategy::Decision;
    use crate::test_util::fill_event;
    use barter_integration::model::{Exchange, Instrument, InstrumentKind};
    use std::collections::HashSet;

    fn positions(engine_id: Uuid) -> (Position, Position, Position) {
        let mut input_fill = fill_event();
        input_fill.decision = Decision::Long;
        input_fill.quantity = 1.0;
        input_fill.fill_value_gross = 100.0;
        input_fill.fees = Fees {
            exchange: 1.0,
            slippage: 1.0,
            network: 1.0,
        };
        input_fill.exchange = Exchange::from("binance");
        input_fill.instrument = Instrument::from(("btc", "usdt", InstrumentKind::Spot));
        let p1 = Position::enter(engine_id, &input_fill).unwrap();

        input_fill.exchange = Exchange::from("binance");
        input_fill.instrument = Instrument::from(("btc", "usdt", InstrumentKind::Spot));
        input_fill.quantity = 2.0;
        input_fill.fill_value_gross = 150.0;
        input_fill.signal_id = Uuid::new_v4();
        let p2 = Position::enter(engine_id, &input_fill).unwrap();

        input_fill.exchange = Exchange::from("binance");
        input_fill.instrument = Instrument::from(("eth", "usdt", InstrumentKind::Spot));
        input_fill.quantity = 1.0;
        input_fill.fill_value_gross = 10.0;
        input_fill.signal_id = Uuid::new_v4();
        let p3 = Position::enter(engine_id, &input_fill).unwrap();

        (p1, p2, p3)
    }

    #[test]
    fn set_open_position() {
        let engine_id = Uuid::new_v4();
        let mut repo: InMemoryRepository<TradingSummary> = InMemoryRepository::new();
        let (btc1, btc2, _eth1) = positions(engine_id);
        assert!(repo.set_open_position(btc1).is_ok());
        assert!(repo.set_open_position(btc2).is_ok());
    }

    #[test]
    fn get() {
        let engine_id = Uuid::new_v4();
        let mut repo: InMemoryRepository<TradingSummary> = InMemoryRepository::new();
        let (btc1, btc2, eth1) = positions(engine_id);
        let signal_set = HashSet::from([btc1.signal_id, btc2.signal_id, eth1.signal_id]);
        let exchange = Exchange::from("binance");
        let btc_instrument = Instrument::from(("btc", "usdt", InstrumentKind::Spot));
        let eth_instrument = Instrument::from(("eth", "usdt", InstrumentKind::Spot));
        let instrument_id = determine_instrument_id(engine_id, &exchange, &btc_instrument);
        let markets = vec![
            Market::new(exchange.clone(), btc_instrument.clone()),
            Market::new(exchange.clone(), eth_instrument.clone()),
        ];

        repo.set_open_position(btc1.clone()).unwrap();
        repo.set_open_position(btc2.clone()).unwrap();
        repo.set_open_position(eth1.clone()).unwrap();

        let positions = repo.get_open_instrument_positions(&instrument_id);
        assert!(positions.is_ok());
        let positions = positions.unwrap();
        assert_eq!(positions.len(), 2);
        assert!(signal_set.contains(&positions[0].signal_id));
        assert!(signal_set.contains(&positions[1].signal_id));

        let positions = repo.get_open_markets_positions(engine_id, markets.iter());
        assert!(positions.is_ok());
        let positions = positions.unwrap();
        assert_eq!(positions.len(), 3);
        assert!(signal_set.contains(&positions[0].signal_id));
        assert!(signal_set.contains(&positions[1].signal_id));
        assert!(signal_set.contains(&positions[2].signal_id));

        let positions = repo.get_all_open_positions();
        assert!(positions.is_ok());
        let positions = positions.unwrap();
        assert_eq!(positions.len(), 3);
        assert!(signal_set.contains(&positions[0].signal_id));
        assert!(signal_set.contains(&positions[1].signal_id));
        assert!(signal_set.contains(&positions[2].signal_id));

        let position = repo.get_open_position(&instrument_id, &btc2.signal_id);
        assert!(position.is_ok());
        let position = position.unwrap();
        assert!(position.is_some());
        let position = position.unwrap();
        assert_eq!(position.signal_id, btc2.signal_id)
    }

    #[test]
    fn remove() {
        let engine_id = Uuid::new_v4();
        let mut repo: InMemoryRepository<TradingSummary> = InMemoryRepository::new();
        let (btc1, btc2, eth1) = positions(engine_id);
        let exchange = Exchange::from("binance");
        let btc_instrument = Instrument::from(("btc", "usdt", InstrumentKind::Spot));
        let instrument_id = determine_instrument_id(engine_id, &exchange, &btc_instrument);

        repo.set_open_position(btc1.clone()).unwrap();
        repo.set_open_position(btc2.clone()).unwrap();
        repo.set_open_position(eth1.clone()).unwrap();

        let position = repo.remove_position(&instrument_id, &btc2.signal_id);
        assert!(position.is_ok());
        let position = position.unwrap();
        assert!(position.is_some());
        let position = position.unwrap();
        assert_eq!(position.signal_id, btc2.signal_id)
    }
}
