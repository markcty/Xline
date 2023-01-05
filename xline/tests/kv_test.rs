mod common;

use std::{error::Error, fs::File};

use tracing_subscriber::EnvFilter;
use xline::client::kv_types::{
    DeleteRangeRequest, PutRequest, RangeRequest, SortOrder, SortTarget,
};

use crate::common::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_kv_put() -> Result<(), Box<dyn Error>> {
    struct TestCase {
        req: PutRequest,
        want_err: bool,
    }

    let tests = [
        TestCase {
            req: PutRequest::new("foo", "").with_ignore_value(true),
            want_err: true,
        },
        TestCase {
            req: PutRequest::new("foo", "bar"),
            want_err: false,
        },
        TestCase {
            req: PutRequest::new("foo", "").with_ignore_value(true),
            want_err: false,
        },
    ];

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    for test in tests {
        let res = client.put(test.req).await;
        assert_eq!(res.is_err(), test.want_err);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_kv_get() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        req: RangeRequest,
        want_kvs: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    let kvs = ["a", "b", "c", "c", "c", "foo", "foo/abc", "fop"];
    let want_kvs = ["a", "b", "c", "foo", "foo/abc", "fop"];
    let kvs_by_version = ["a", "b", "foo", "foo/abc", "fop", "c"];
    let reversed_kvs = ["fop", "foo/abc", "foo", "c", "b", "a"];

    let tests = [
        TestCase {
            req: RangeRequest::new("a"),
            want_kvs: &want_kvs[..1],
        },
        // TestCase {
        //     req: RangeRequest::new("a").with_serializable(true),
        //     want_kvs: &want_kvs[..1],
        // },
        TestCase {
            req: RangeRequest::new("a").with_range_end("c"),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            req: RangeRequest::new("").with_prefix(),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("").with_from_key(),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("a").with_range_end("x"),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("").with_prefix().with_revision(4),
            want_kvs: &want_kvs[..3],
        },
        TestCase {
            req: RangeRequest::new("a").with_count_only(true),
            want_kvs: &[],
        },
        TestCase {
            req: RangeRequest::new("foo").with_prefix(),
            want_kvs: &["foo", "foo/abc"],
        },
        TestCase {
            req: RangeRequest::new("foo").with_from_key(),
            want_kvs: &["foo", "foo/abc", "fop"],
        },
        TestCase {
            req: RangeRequest::new("").with_prefix().with_limit(2),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Mod)
                .with_sort_order(SortOrder::Ascend),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Version)
                .with_sort_order(SortOrder::Ascend),
            want_kvs: &kvs_by_version[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Create)
                .with_sort_order(SortOrder::None),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Create)
                .with_sort_order(SortOrder::Descend),
            want_kvs: &reversed_kvs[..],
        },
        TestCase {
            req: RangeRequest::new("")
                .with_prefix()
                .with_sort_target(SortTarget::Key)
                .with_sort_order(SortOrder::Descend),
            want_kvs: &reversed_kvs[..],
        },
    ];

    for key in kvs {
        client.put(PutRequest::new(key, "bar")).await?;
    }

    for test in tests {
        let res = client.range(test.req).await?;
        assert_eq!(res.kvs.len(), test.want_kvs.len());
        let is_identical = res
            .kvs
            .iter()
            .zip(test.want_kvs.iter())
            .all(|(kv, want)| kv.key == want.as_bytes());
        assert!(is_identical);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_kv_delete() -> Result<(), Box<dyn Error>> {
    let path = format!("../scripts/logs/d-{}.log", std::env::var("ID").unwrap());
    let file = File::create(path).unwrap();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(false)
        .with_writer(file)
        .init();
    struct TestCase<'a> {
        req: DeleteRangeRequest,
        want_deleted: i64,
        want_keys: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    let keys = ["a", "b", "c", "c/abc", "d"];

    let tests = [
        TestCase {
            req: DeleteRangeRequest::new("").with_prefix(),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            req: DeleteRangeRequest::new("").with_from_key(),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            req: DeleteRangeRequest::new("a").with_range_end("c"),
            want_deleted: 2,
            want_keys: &["c", "c/abc", "d"],
        },
        TestCase {
            req: DeleteRangeRequest::new("c"),
            want_deleted: 1,
            want_keys: &["a", "b", "c/abc", "d"],
        },
        TestCase {
            req: DeleteRangeRequest::new("c").with_prefix(),
            want_deleted: 2,
            want_keys: &["a", "b", "d"],
        },
        TestCase {
            req: DeleteRangeRequest::new("c").with_from_key(),
            want_deleted: 3,
            want_keys: &["a", "b"],
        },
        TestCase {
            req: DeleteRangeRequest::new("e"),
            want_deleted: 0,
            want_keys: &keys,
        },
    ];

    for (i, test) in tests.into_iter().enumerate() {
        tracing::debug!("test {i} begins");
        for key in keys {
            client.put(PutRequest::new(key, "bar")).await?;
        }

        let res = client.delete(test.req).await?;
        assert_eq!(res.deleted, test.want_deleted);

        let res = client.range(RangeRequest::new("").with_prefix()).await?;
        for (j, (kv, want)) in res.kvs.iter().zip(test.want_keys.iter()).enumerate() {
            assert_eq!(kv.key, want.as_bytes(), "fail {j} in test {i}");
        }
        tracing::debug!("test {i} ends");
    }

    Ok(())
}
