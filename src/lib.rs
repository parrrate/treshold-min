use std::{collections::BinaryHeap, pin::pin};

use futures_util::{Stream, TryStream, TryStreamExt};
use genawaiter_try::to_try_stream;

pub trait ThresholdMin: Sized + Send + TryStream<Ok = (Self::K, u64), Error: Send> {
    type K: Send + Ord;

    fn threshold_min(
        self,
        min_discard: impl Send + Sync + PartialOrd<Self::K>,
        threshold: u64,
    ) -> impl Send + Stream<Item = Result<Self::K, Self::Error>> {
        to_try_stream(async move |co| {
            let mut items = pin!(self.into_stream());
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
                    println!("total: {total}");
                }
            }
            for (k, _) in heap {
                co.yield_(k).await;
            }
            Ok(())
        })
    }
}

impl<S: Send + TryStream<Ok = (K, u64), Error: Send>, K: Send + Ord> ThresholdMin for S {
    type K = K;
}
