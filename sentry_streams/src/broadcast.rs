use crate::routes::RoutedValue;
use crate::utils::clone_committable;
use sentry_arroyo::{processing::strategies::SubmitError, types::Message};

pub fn broadcast(
    downstream_branches: Vec<String>,
    message: Message<RoutedValue>,
) -> Result<Vec<Message<RoutedValue>>, SubmitError<RoutedValue>> {
    let mut res = Vec::new();
    let branches = downstream_branches.clone();
    for branch in branches {
        let clone = message.payload().clone();
        let routed_clone = clone.add_waypoint(branch);
        let committable = clone_committable(&message);
        let routed_message = Message::new_any_message(routed_clone, committable);
        res.push(routed_message);
    }
    Ok(res)
}
