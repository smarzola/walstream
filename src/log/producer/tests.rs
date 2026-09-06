use super::*;
use crate::log::tests::record;

pub(crate) fn batch(id: i64, epoch: i16, sequence: i32, count: usize) -> Vec<Record> {
    (0..count)
        .map(|n| {
            let mut r = record(&format!("{id}/{epoch}/{sequence}/{n}"));
            r.producer_id = id;
            r.producer_epoch = epoch;
            r.sequence = sequence.wrapping_add(n as i32);
            r.offset = n as i64;
            r
        })
        .collect()
}

#[tokio::test]
async fn concurrent_allocators_and_producer_appends_survive_replacement() {
    let engine = LogEngine::in_memory("producers").unwrap();
    let ids = futures::future::join_all((0..32).map(|_| {
        let engine = engine.clone();
        async move { engine.allocate_producer_id().await.unwrap() }
    }))
    .await;
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), 32);
    let results = futures::future::join_all((0..32).map(|_| {
        let engine = engine.clone();
        async move { engine.append("events", 0, batch(0, 0, 0, 2)).await.unwrap() }
    }))
    .await;
    assert!(
        results
            .iter()
            .all(|r| r.base_offset == 0 && r.last_offset == 1)
    );
    let results = futures::future::join_all((1..17).map(|id| {
        let engine = engine.clone();
        async move {
            engine
                .append("events", 0, batch(id, 0, 0, 1))
                .await
                .unwrap()
        }
    }))
    .await;
    assert_eq!(
        results
            .iter()
            .map(|r| r.base_offset)
            .collect::<HashSet<_>>()
            .len(),
        16
    );
    let restarted = LogEngine::new(engine.store, "producers").unwrap();
    assert_eq!(restarted.allocate_producer_id().await.unwrap(), 32);
    assert_eq!(restarted.fetch("events", 0, 0).await.unwrap().len(), 18);
    assert_eq!(
        restarted
            .append("events", 0, batch(0, 0, 0, 2))
            .await
            .unwrap()
            .base_offset,
        0
    );
}

#[tokio::test]
async fn directory_splits_and_updates_arbitrary_producers_after_collection() {
    let engine = LogEngine::in_memory("producers").unwrap();
    // More than 64 half-full leaves forces an internal-page split as well.
    for id in 0..2100 {
        engine
            .append("events", 0, batch(id, 0, 0, 1))
            .await
            .unwrap();
    }
    let LogManifest::Indexed(root) = engine
        .load_manifest("events", 0)
        .await
        .unwrap()
        .unwrap()
        .manifest
    else {
        panic!()
    };
    assert_eq!(root.producers.as_ref().unwrap().level, 2);
    let report = engine
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                apply: true,
                max_bytes: Some(0),
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!((report.start_offset, report.next_offset), (2100, 2100));
    assert!(report.collected_objects > 2100);
    let restarted = LogEngine::new(engine.store, "producers").unwrap();
    for id in (0..2100).rev().step_by(37) {
        assert_eq!(
            restarted
                .append("events", 0, batch(id, 0, 0, 1))
                .await
                .unwrap()
                .base_offset,
            id
        );
        restarted
            .append("events", 0, batch(id, 0, 1, 1))
            .await
            .unwrap();
    }
    restarted
        .maintain(
            "events",
            0,
            &MaintenanceOptions {
                apply: true,
                ..MaintenanceOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        restarted.fetch("events", 0, 2100).await.unwrap().len(),
        (0..2100).step_by(37).count()
    );
}

#[tokio::test]
async fn sequence_wrap_preserves_one_canonical_batch_and_advances_to_zero() {
    let engine = LogEngine::in_memory("producers").unwrap();
    engine.append("events", 0, batch(1, 0, 0, 1)).await.unwrap();
    // Seed the predecessor at the sequence boundary without billions of appends.
    let LogManifest::Indexed(mut root) = engine
        .load_manifest("events", 0)
        .await
        .unwrap()
        .unwrap()
        .manifest
    else {
        panic!()
    };
    let state = ProducerState {
        id: 1,
        epoch: 0,
        batches: vec![BatchIdentity {
            sequence: i32::MAX - 2,
            count: 1,
            base_offset: 0,
            fingerprint: "a".repeat(64),
        }],
    };
    root.producers = Some(
        engine
            .update_producer("events", 0, root.producers.as_ref(), state, 1)
            .await
            .unwrap(),
    );
    engine
        .store
        .put(
            &engine.manifest_path("events", 0),
            Bytes::from(serde_json::to_vec(&root).unwrap()).into(),
        )
        .await
        .unwrap();
    let crossing = batch(1, 0, i32::MAX - 1, 3);
    assert_eq!(
        engine
            .append("events", 0, crossing.clone())
            .await
            .unwrap()
            .base_offset,
        1
    );
    assert_eq!(
        engine
            .append("events", 0, crossing)
            .await
            .unwrap()
            .last_offset,
        3
    );
    assert_eq!(
        engine
            .append("events", 0, batch(1, 0, 1, 1))
            .await
            .unwrap()
            .base_offset,
        4
    );
    let records = engine.fetch("events", 0, 1).await.unwrap();
    assert_eq!(
        records.iter().map(|r| r.sequence).collect::<Vec<_>>(),
        vec![i32::MAX - 1, i32::MAX, i32::MIN, 1]
    );
}
