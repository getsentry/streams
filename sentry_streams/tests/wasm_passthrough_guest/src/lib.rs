//! Minimal passthrough guest for host unit tests (no embedded Python).

wit_bindgen::generate!({
    world: "plugin",
    path: "../../wit",
});

use std::cell::RefCell;

struct Component;

thread_local! {
    static PENDING: RefCell<Vec<exports::sentry_streams::processor::processor::Output>> =
        RefCell::new(Vec::new());
}

impl exports::sentry_streams::processor::processor::Guest for Component {
    fn submit(msg: exports::sentry_streams::processor::processor::Message) {
        PENDING.with(|p| {
            p.borrow_mut()
                .push(exports::sentry_streams::processor::processor::Output::Msg(msg));
        });
    }

    fn submit_watermark(wm: exports::sentry_streams::processor::processor::Watermark) {
        PENDING.with(|p| {
            p.borrow_mut()
                .push(exports::sentry_streams::processor::processor::Output::Wm(wm));
        });
    }

    fn poll() -> Option<Vec<exports::sentry_streams::processor::processor::Output>> {
        PENDING.with(|p| {
            let mut pending = p.borrow_mut();
            if pending.is_empty() {
                None
            } else {
                Some(std::mem::take(&mut *pending))
            }
        })
    }
}

export!(Component);
