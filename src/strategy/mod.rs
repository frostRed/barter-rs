use crate::data::MarketMeta;
use barter_data::event::{DataKind, MarketEvent};
use barter_integration::model::{Exchange, Instrument, Market};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Barter example RSI strategy [`SignalGenerator`] implementation.
pub mod example;

/// May generate an advisory [`Signal`] as a result of analysing an input [`MarketEvent`].
pub trait SignalGenerator {
    /// Optionally return a [`Signal`] given input [`MarketEvent`].
    fn generate_signal(&mut self, market: &MarketEvent<DataKind>) -> Option<Signal>;
}

/// Advisory [`Signal`] for a [`Market`] detailing the [`SuggestInfo`] associated with each
/// possible [`Decision`]. Interpreted by an [`OrderGenerator`](crate::portfolio::OrderGenerator).
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct Signal {
    pub signal_id: Uuid,
    pub time: DateTime<Utc>,
    pub exchange: Exchange,
    pub instrument: Instrument,
    pub suggest: Suggest,
    /// Metadata propagated from the [`MarketEvent`] that yielded this [`Signal`].
    pub market_meta: MarketMeta,
}

/// Describes the type of advisory signal the strategy is endorsing.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub enum Decision {
    Long,
    CloseLong,
    Short,
    CloseShort,
}

impl Default for Decision {
    fn default() -> Self {
        Self::Long
    }
}

impl Decision {
    /// Determines if a [`Decision`] is Long.
    pub fn is_long(&self) -> bool {
        matches!(self, Decision::Long)
    }

    /// Determines if a [`Decision`] is Short.
    pub fn is_short(&self) -> bool {
        matches!(self, Decision::Short)
    }

    /// Determines if a [`Decision`] is an entry (long or short).
    pub fn is_entry(&self) -> bool {
        matches!(self, Decision::Short | Decision::Long)
    }

    /// Determines if a [`Decision`] is an exit (close_long or close_short).
    pub fn is_exit(&self) -> bool {
        matches!(self, Decision::CloseLong | Decision::CloseShort)
    }
}

/// Only two possibilities of a [`Signal`]
#[derive(Copy, Clone, PartialEq, Debug, Deserialize, Serialize)]
pub enum Suggest {
    SuggestLong(SuggestInfo),
    SuggestShort(SuggestInfo),
}

impl Suggest {
    pub fn new(
        decision: Decision,
        strength: f64,
        fail_price: Option<f64>,
        target_price: Option<f64>,
        only_close_opposite: bool,
        re_enter: bool,
    ) -> Self {
        let suggest = SuggestInfo::new(
            strength,
            fail_price,
            target_price,
            only_close_opposite,
            re_enter,
        );
        match decision {
            Decision::Long | Decision::CloseShort => Suggest::SuggestLong(suggest),
            Decision::Short | Decision::CloseLong => Suggest::SuggestShort(suggest),
        }
    }

    pub fn new_long(suggest: SuggestInfo) -> Self {
        Suggest::SuggestLong(suggest)
    }

    pub fn new_short(suggest: SuggestInfo) -> Self {
        Suggest::SuggestShort(suggest)
    }
}

impl Default for Suggest {
    fn default() -> Self {
        Self::new(Decision::Long, 1.0, None, None, true, false)
    }
}

/// Suggestion information of an advisory [`Signal`] decision produced by [`SignalGenerator`] strategy.
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct SuggestInfo {
    pub strength: f64,
    pub fail_price: Option<f64>,
    pub target_price: Option<f64>,
    // If true, the existing opposite position will be exited,
    // and will not create this direction position
    pub only_close_opposite: bool,
    // If true, even if there is already a position in the same direction,
    // another new position will be established
    pub re_enter: bool,
}

impl SuggestInfo {
    pub fn new(
        strength: f64,
        fail_price: Option<f64>,
        target_price: Option<f64>,
        only_close_opposite: bool,
        re_enter: bool,
    ) -> Self {
        Self {
            strength,
            fail_price,
            target_price,
            only_close_opposite,
            re_enter,
        }
    }
    pub fn new_only_strength(strength: f64) -> Self {
        Self {
            strength,
            fail_price: None,
            target_price: None,
            only_close_opposite: true,
            re_enter: false,
        }
    }
}

/// use this Signal to Exit all positions of a instrument.
/// generate by OrderGenerator with a strategy Signal
#[derive(Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct SignalInstrumentPositionsExit {
    pub signal_id: Uuid,
    pub force_exit: SignalForceExit,
}

impl SignalInstrumentPositionsExit {
    /// Constructs a new [`Self`] using the configuration provided.
    pub fn new<E, I>(signal_id: Uuid, exchange: E, instrument: I) -> Self
    where
        E: Into<Exchange>,
        I: Into<Instrument>,
    {
        Self {
            signal_id,
            force_exit: SignalForceExit::new(exchange, instrument),
        }
    }
}

impl From<SignalForceExit> for SignalInstrumentPositionsExit {
    fn from(value: SignalForceExit) -> Self {
        Self {
            signal_id: Uuid::new_v4(),
            force_exit: value,
        }
    }
}

/// Force exit Signal produced after an [`Engine`](crate::engine::Engine) receives a
/// [`Command::ExitPosition`](crate::engine::Command) from an external source.
#[derive(Clone, Eq, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct SignalForceExit {
    pub time: DateTime<Utc>,
    pub exchange: Exchange,
    pub instrument: Instrument,
}

impl<M> From<M> for SignalForceExit
where
    M: Into<Market>,
{
    fn from(market: M) -> Self {
        let market = market.into();
        Self::new(market.exchange, market.instrument)
    }
}

impl SignalForceExit {
    pub const FORCED_EXIT_SIGNAL: &'static str = "SignalForcedExit";

    /// Constructs a new [`Self`] using the configuration provided.
    pub fn new<E, I>(exchange: E, instrument: I) -> Self
    where
        E: Into<Exchange>,
        I: Into<Instrument>,
    {
        Self {
            time: Utc::now(),
            exchange: exchange.into(),
            instrument: instrument.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_decision_is_long() {
        let decision = Decision::Long;
        assert_eq!(decision.is_long(), true)
    }

    #[test]
    fn should_return_decision_is_not_long() {
        let decision = Decision::Short;
        assert_eq!(decision.is_long(), false)
    }

    #[test]
    fn should_return_decision_is_short() {
        let decision = Decision::Short;
        assert_eq!(decision.is_short(), true)
    }

    #[test]
    fn should_return_decision_is_not_short() {
        let decision = Decision::Long;
        assert_eq!(decision.is_short(), false)
    }

    #[test]
    fn should_return_decision_is_entry() {
        let decision = Decision::Long;
        assert_eq!(decision.is_entry(), true)
    }

    #[test]
    fn should_return_decision_is_not_entry() {
        let decision = Decision::CloseLong;
        assert_eq!(decision.is_entry(), false)
    }

    #[test]
    fn should_return_decision_is_exit() {
        let decision = Decision::CloseShort;
        assert_eq!(decision.is_exit(), true)
    }

    #[test]
    fn should_return_decision_is_not_exit() {
        let decision = Decision::Long;
        assert_eq!(decision.is_exit(), false)
    }
}
