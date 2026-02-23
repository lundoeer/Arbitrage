Implement sizing based on mainly on current best_size but maybe also book
Implement minimum sizing so both sides of the trade is above a configurable threshold.
Implement selling positions based on bids - this should also check size
Implement order monitoring and cancellations - this is hopefully not super relevant with the current order types, but i need to know if i have any orders in place.
Implement hedging based on positions and a check for unfilled orders. Integrated with drift_tolerance
Implement states for what is open: {buying, selling, hedging}, remember only one order per market at a time. orders must be resolved before and resulting positions updated before a order is placed. Maybe the current FSM is already handeling this otherwise consider if this is new or a change to FSM where does this belong? Maybe it is logic to go from Awaiting_result to cooldown currently it just auto moves as far as i can see.
Implement max exposture per market -> if over all buying blocked on that market.
