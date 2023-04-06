use serde::{Deserialize, Serialize};

use crate::portfolio::{
    repository::{BalanceHandler, PositionHandler},
    OrderEvent, OrderType,
};

/// Evaluates the risk associated with an [`OrderEvent`] to determine if it should be actioned. It
/// can also amend the order (eg/ [`OrderType`]) to better fit the risk strategy required for
/// profitability.
pub trait OrderEvaluator<Repository>
where
    Repository: PositionHandler + BalanceHandler,
{
    const DEFAULT_ORDER_TYPE: OrderType;

    /// May return an amended [`OrderEvent`] if the associated risk is appropriate. Returns `None`
    /// if the risk is too high.
    fn evaluate_order(&self, repository: &Repository, order: OrderEvent) -> Option<OrderEvent>;
}

/// Default risk manager that implements [`OrderEvaluator`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct DefaultRisk {}

impl<Repository> OrderEvaluator<Repository> for DefaultRisk
where
    Repository: PositionHandler + BalanceHandler,
{
    const DEFAULT_ORDER_TYPE: OrderType = OrderType::Market;

    fn evaluate_order(
        &self,
        _repository: &Repository,
        mut order: OrderEvent,
    ) -> Option<OrderEvent> {
        if self.risk_too_high(&order) {
            return None;
        }
        order.order_type = <Self as OrderEvaluator<Repository>>::DEFAULT_ORDER_TYPE;
        Some(order)
    }
}

impl DefaultRisk {
    fn risk_too_high(&self, _: &OrderEvent) -> bool {
        false
    }
}
