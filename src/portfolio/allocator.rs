use crate::{
    portfolio::{
        position::Position,
        repository::{BalanceHandler, PositionHandler},
        OrderEvent,
    },
    strategy::{Decision, SignalStrength},
};
use serde::{Deserialize, Serialize};

/// Allocates an appropriate [`OrderEvent`] quantity.
pub trait OrderAllocator<Repository>
where
    Repository: PositionHandler + BalanceHandler,
{
    /// Returns an [`OrderEvent`] with a calculated order quantity based on the input order,
    /// [`SignalStrength`] and potential all existing [`Position`]s.
    fn allocate_order<'a, Positions: Iterator<Item = &'a Position>>(
        &self,
        repository: &Repository,
        order: &mut OrderEvent,
        instrument_positions: Positions,
        signal_strength: SignalStrength,
    );
}

/// Default allocation manager that implements [`OrderAllocator`]. Order size is calculated by
/// using the default_order_value, symbol close value, and [`SignalStrength`].
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug, Default, Deserialize, Serialize)]
pub struct DefaultAllocator {
    pub default_order_value: f64,
}

impl<Repository> OrderAllocator<Repository> for DefaultAllocator
where
    Repository: PositionHandler + BalanceHandler,
{
    fn allocate_order<'a, Positions: Iterator<Item = &'a Position>>(
        &self,
        _repository: &Repository,
        order: &mut OrderEvent,
        instrument_positions: Positions,
        signal_strength: SignalStrength,
    ) {
        // Calculate exact order_size, then round it to a more appropriate decimal place
        let default_order_size = self.default_order_value / order.market_meta.close;
        let default_order_size = (default_order_size * 10000.0).floor() / 10000.0;

        match order.decision {
            // Entry
            Decision::Long => order.quantity = default_order_size * signal_strength.strength,

            // Entry
            Decision::Short => order.quantity = -default_order_size * signal_strength.strength,

            // Exit
            _ => {
                order.quantity = 0.0
                    - instrument_positions
                        .into_iter()
                        .map(|p| p.quantity)
                        .sum::<f64>()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portfolio::repository::in_memory::InMemoryRepository;
    use crate::statistic::summary::trading::TradingSummary;
    use crate::test_util::{order_event, position};

    fn repository() -> InMemoryRepository<TradingSummary> {
        InMemoryRepository::new()
    }

    #[test]
    fn should_allocate_order_to_exit_open_long_position() {
        let allocator = DefaultAllocator {
            default_order_value: 1000.0,
        };

        let mut input_order = order_event();
        input_order.decision = Decision::CloseLong;

        let mut input_position = position();
        input_position.quantity = 100.0;

        let input_signal_strength = SignalStrength::new_with_strength(0.0);

        allocator.allocate_order(
            &repository(),
            &mut input_order,
            vec![input_position.clone()].iter(),
            input_signal_strength,
        );

        let actual_result = input_order.quantity;
        let expected_result = 0.0 - input_position.quantity;

        assert_eq!(actual_result, expected_result)
    }

    #[test]
    fn should_allocate_order_to_exit_open_short_position() {
        let allocator = DefaultAllocator {
            default_order_value: 1000.0,
        };

        let mut input_order = order_event();
        input_order.decision = Decision::CloseShort;

        let mut input_position = position();
        input_position.quantity = -100.0;

        let input_signal_strength = SignalStrength::new_with_strength(0.0);

        allocator.allocate_order(
            &repository(),
            &mut input_order,
            vec![input_position.clone()].iter(),
            input_signal_strength,
        );

        let actual_result = input_order.quantity;
        let expected_result = 0.0 - input_position.quantity;

        assert_eq!(actual_result, expected_result)
    }

    #[test]
    fn should_allocate_order_to_enter_long_position_with_correct_quantity() {
        let default_order_value = 1000.0;
        let allocator = DefaultAllocator {
            default_order_value,
        };

        let order_close = 10.0;
        let mut input_order = order_event();
        input_order.market_meta.close = order_close;
        input_order.decision = Decision::Long;

        let input_signal_strength = SignalStrength::new_with_strength(1.0);

        allocator.allocate_order(
            &repository(),
            &mut input_order,
            vec![].iter(),
            input_signal_strength,
        );

        let actual_result = input_order.quantity;
        let expected_result =
            (default_order_value / order_close) * input_signal_strength.strength as f64;

        assert_eq!(actual_result, expected_result)
    }

    #[test]
    fn should_allocate_order_to_enter_long_position_with_non_zero_quantity() {
        let default_order_value = 200.0;
        let allocator = DefaultAllocator {
            default_order_value,
        };

        let order_close = 226.753403;
        let mut input_order = order_event();
        input_order.market_meta.close = order_close;
        input_order.decision = Decision::Long;

        let input_signal_strength = SignalStrength::new_with_strength(1.0);

        allocator.allocate_order(
            &repository(),
            &mut input_order,
            vec![].iter(),
            input_signal_strength,
        );

        let actual_result = input_order.quantity;
        let expected_order_size = ((default_order_value / order_close) * 10000.0).floor() / 10000.0;
        let expected_result = expected_order_size * input_signal_strength.strength as f64;

        assert_ne!(actual_result, 0.0);
        assert_eq!(actual_result, expected_result)
    }

    #[test]
    fn should_allocate_order_to_enter_short_position_with_correct_quantity() {
        let default_order_value = 1000.0;
        let allocator = DefaultAllocator {
            default_order_value,
        };

        let order_close = 10.0;
        let mut input_order = order_event();
        input_order.market_meta.close = order_close;
        input_order.decision = Decision::Short;

        let input_signal_strength = SignalStrength::new_with_strength(1.0);

        allocator.allocate_order(
            &repository(),
            &mut input_order,
            vec![].iter(),
            input_signal_strength,
        );

        let actual_result = input_order.quantity;
        let expected_result =
            -(default_order_value / order_close) * input_signal_strength.strength as f64;

        assert_eq!(actual_result, expected_result)
    }

    #[test]
    fn should_allocate_order_to_enter_short_position_with_with_non_zero_quantity() {
        let default_order_value = 200.0;
        let allocator = DefaultAllocator {
            default_order_value,
        };

        let order_close = 226.753403;
        let mut input_order = order_event();
        input_order.market_meta.close = order_close;
        input_order.decision = Decision::Short;

        let input_signal_strength = SignalStrength::new_with_strength(1.0);

        allocator.allocate_order(
            &repository(),
            &mut input_order,
            vec![].iter(),
            input_signal_strength,
        );

        let actual_result = input_order.quantity;
        let expected_order_size = ((default_order_value / order_close) * 10000.0).floor() / 10000.0;
        let expected_result = -expected_order_size * input_signal_strength.strength;

        assert_ne!(actual_result, 0.0);
        assert_eq!(actual_result, expected_result)
    }
}
