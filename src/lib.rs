use std::{collections::BinaryHeap, pin::pin};

use futures_util::{Stream, TryStream, TryStreamExt};
use genawaiter_try::to_try_stream;

pub async fn threshold_min<K: Send + Ord, E: Send>(
    items: impl Send + TryStream<Ok = (K, u64), Error = E>,
    min_discard: impl Send + Sync + PartialOrd<K>,
    threshold: u64,
) -> impl Stream<Item = Result<K, E>> {
    to_try_stream(async move |co| {
        let mut items = pin!(items.into_stream());
        let mut heap = BinaryHeap::new();
        let mut total = 0;
        while let Some((k, v)) = items.try_next().await? {
            total += v;
            if min_discard > k {
                co.yield_(k).await;
            } else {
                heap.push((k, v));
            }
            while let Some((_, v)) = heap.peek()
                && let v = *v
                && total - v >= threshold
            {
                assert_eq!(heap.pop().unwrap().1, v);
                total -= v;
            }
        }
        for (k, _) in heap {
            co.yield_(k).await;
        }
        Ok(())
    })
}
