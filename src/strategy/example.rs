use super::{Decision, Signal, SignalGenerator};
use crate::data::MarketMeta;
use crate::strategy::{SignalExtra, Suggest};
use barter_data::event::{DataKind, MarketEvent};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use ta::{indicators::RelativeStrengthIndex, Next};
use uuid::Uuid;

/// Configuration for constructing a [`RSIStrategy`] via the new() constructor method.
#[derive(Copy, Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Config {
    pub rsi_period: usize,
}

#[derive(Clone, Debug)]
/// Example RSI based strategy that implements [`SignalGenerator`].
pub struct RSIStrategy {
    rsi: RelativeStrengthIndex,
}

impl SignalGenerator for RSIStrategy {
    fn generate_signal(&mut self, market: &MarketEvent<DataKind>) -> Option<Signal> {
        // Check if it's a MarketEvent with a candle
        let candle_close = match &market.kind {
            DataKind::Candle(candle) => candle.close,
            _ => return None,
        };

        // Calculate the next RSI value using the new MarketEvent Candle data
        let rsi = self.rsi.next(candle_close);

        // Generate advisory signals map
        // If signals is None, return no SignalEvent
        let suggest = RSIStrategy::generate_signals_map(rsi)?;

        Some(Signal {
            signal_id: Uuid::new_v4(),
            time: Utc::now(),
            exchange: market.exchange.clone(),
            instrument: market.instrument.clone(),
            market_meta: MarketMeta {
                close: candle_close,
                time: market.exchange_time,
            },
            suggest,
            extra: SignalExtra::default(),
        })
    }
}

impl RSIStrategy {
    /// Constructs a new [`RSIStrategy`] component using the provided configuration struct.
    pub fn new(config: Config) -> Self {
        let rsi_indicator = RelativeStrengthIndex::new(config.rsi_period)
            .expect("Failed to construct RSI indicator");

        Self { rsi: rsi_indicator }
    }

    /// Given the latest RSI value for a symbol, generates a map containing the [`SuggestInfo`] for
    /// [`Decision`] under consideration.
    fn generate_signals_map(rsi: f64) -> Option<Suggest> {
        if rsi < 40.0 {
            Some(Suggest::new(
                Decision::Short,
                RSIStrategy::calculate_signal_strength(),
                None,
                None,
                true,
                false,
            ))
        } else if rsi > 60.0 {
            Some(Suggest::new(
                Decision::Long,
                RSIStrategy::calculate_signal_strength(),
                None,
                None,
                true,
                false,
            ))
        } else {
            None
        }
    }

    /// Calculates the [`SuggestInfo`] of a particular [`Decision`].
    fn calculate_signal_strength() -> f64 {
        1.0
    }
}
