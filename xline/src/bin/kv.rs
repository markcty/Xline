mod common;

use std::{error::Error, time::Duration};

use tracing::debug;
use tracing_subscriber::{fmt, EnvFilter};
use xline::client::kv_types::PutRequest;

use crate::common::Cluster;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn Error>> {
    console_subscriber::init();
    // Configure a custom event formatter
    // let format = fmt::format()
    //     .with_thread_ids(true) // include the thread ID of the current thread
    //     .with_line_number(true)
    //     .without_time()
    //     .compact(); // use the `Compact` formatting style.

    // // Create a `fmt` subscriber that uses our custom event format, and set it
    // // as the default.
    // tracing_subscriber::fmt()
    //     .event_format(format)
    //     .with_env_filter(EnvFilter::from_default_env())
    //     .init();

    struct TestCase {
        req: PutRequest,
        want_err: bool,
    }

    let tests = [
        // TestCase {
        //     req: PutRequest::new("foo", "").with_ignore_value(true),
        //     want_err: true,
        // },
        TestCase {
            req: PutRequest::new("foo", "bar"),
            want_err: false,
        },
        TestCase {
            req: PutRequest::new("foo", "").with_ignore_value(true),
            want_err: false,
        },
    ];

    debug!("hi");

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    for test in tests {
        let res = client.put(test.req).await;
        // tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(res.is_err(), test.want_err);
    }

    Ok(())
}
