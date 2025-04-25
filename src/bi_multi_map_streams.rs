use super::{task::Task, MappedStreams};
use crate::mapped_streams::StMut;
use bisetmap::BisetMap;
use core::hash::Hash;
use futures_core::Stream;
use futures_task::Poll;
use futures_util::stream::StreamFuture;
use std::{marker::PhantomData, pin::Pin, sync::Arc, task::ready};

/// This is a BiMultiMap structure that associates (LeftKey, RightKey) pairs with a stream.
/// Bi - This is a two-way mapping; each kind of key is mapped to keys of the other kind (and
/// streams)
/// Multi - Each key can have associations with multiple keys of the other kind.
///
/// The result is a structure that can be indexed with either a left key or a right key to obtain a
/// set of keys of the other kind, and a set of streams, or can be indexed with a (left, right) key
/// pair to obtain at most one stream.
pub struct BiMultiMapStreams<L: Clone + Hash + Eq, R: Clone + Hash + Eq, St: Stream + Unpin> {
    bi_multi_map: BisetMap<L, R>,
    streams: MappedStreams<(L, R), St>,
}

impl<L: Clone + Hash + Eq, R: Clone + Hash + Eq, St: Stream + Unpin> BiMultiMapStreams<L, R, St> {
    /// Create a new mapping.
    pub fn new() -> Self {
        Self {
            bi_multi_map: BisetMap::new(),
            streams: MappedStreams::new(),
        }
    }

    /// Insert a new (left, right, stream) associations.
    /// Returns true if another stream was not removed to make room for the provided stream.
    pub fn insert(&mut self, left: L, right: R, stream: St) -> bool {
        self.bi_multi_map.insert(left.clone(), right.clone());
        self.streams.insert((left, right), stream)
    }

    /// Insert a new (left, right, stream) associations.
    /// Returns Some(fut) if a stream was evicted to make room for this association.
    pub fn insert_pin(&mut self, left: L, right: R, stream: St) -> Option<St>
    where
        St: Unpin,
    {
        let ret = self.streams.replace((left.clone(), right.clone()), stream);
        if ret.is_some() {
            self.bi_multi_map.insert(left, right);
        }
        ret
    }

    /// Check if there exists a (left, right) association.
    pub fn contains(&self, left: &L, right: &R) -> bool {
        self.bi_multi_map.contains(&left, &right)
    }

    /// Check if there exists any associations with the provided left key.
    pub fn contains_left(&self, left: &L) -> bool {
        self.bi_multi_map.key_exists(&left)
    }

    /// Check if there exists any associations with the provided right key.
    pub fn contains_right(&self, right: &R) -> bool {
        self.bi_multi_map.value_exists(&right)
    }

    /// Remove and return the corresponding stream, if one exists.
    pub fn remove(&mut self, left: &L, right: &R) -> Option<St>
    where
        St: Unpin,
    {
        self.bi_multi_map.remove(left, right);
        self.streams.remove(&(left.clone(), right.clone()))
    }

    /// Remove and drop the corresponding stream.
    /// Returns true if a stream was cancelled.
    pub fn cancel(&mut self, left: &L, right: &R) -> bool {
        self.bi_multi_map.remove(left, right);
        self.streams.cancel(&(left.clone(), right.clone()))
    }

    /// Get a mutable reference to the corresponding stream.
    pub fn get_mut(&mut self, left: &L, right: &R) -> Option<StMut<(L, R), St>>
    where
        St: Unpin,
    {
        self.streams.get_mut(&(left.clone(), right.clone()))
    }

    /// Get an iterator of mutable references to the streams and right keys that are associated
    /// with the provided left key.
    pub fn get_right_mut<'a>(&'a mut self, left: &L) -> RightIterMut<'a, L, R, St>
    where
        St: Unpin,
    {
        RightIterMut::new(self, left)
    }

    /// Get an iterator of mutable references to the streams and left keys that are associated
    /// with the provided right key.
    pub fn get_left_mut<'a>(&'a mut self, right: &R) -> LeftIterMut<'a, L, R, St>
    where
        St: Unpin,
    {
        LeftIterMut::new(self, right)
    }

    /// Remove and drop all streams corresponding to the provided left key.
    /// Returns the right keys of all streams that were dropped.
    pub fn left_cancel(&mut self, left: &L) -> Vec<R> {
        let rights = self.bi_multi_map.delete(left);
        rights.iter().for_each(|right| {
            self.streams.cancel(&(left.clone(), right.clone()));
        });
        rights
    }

    /// Remove and drop all streams corresponding to the provided right key.
    /// Returns the left keys of all streams that were dropped.
    pub fn right_cancel(&mut self, right: &R) -> Vec<L> {
        let lefts = self.bi_multi_map.rev_delete(right);
        lefts.iter().for_each(|left| {
            self.streams.cancel(&(left.clone(), right.clone()));
        });
        lefts
    }

    /// Remove and return the corresponding streams and their right keys.
    pub fn left_remove(&mut self, left: &L) -> Vec<(R, St)>
    where
        St: Unpin,
    {
        let rights = self.bi_multi_map.delete(left);
        rights
            .iter()
            .map(|right| {
                (
                    right.clone(),
                    self.streams.remove(&(left.clone(), right.clone())).unwrap(),
                )
            })
            .collect()
    }

    /// Remove and return the corresponding streams and their left keys.
    pub fn right_remove(&mut self, right: &R) -> Vec<(L, St)>
    where
        St: Unpin,
    {
        let lefts = self.bi_multi_map.rev_delete(right);
        lefts
            .iter()
            .map(|left| {
                (
                    left.clone(),
                    self.streams.remove(&(left.clone(), right.clone())).unwrap(),
                )
            })
            .collect()
    }

    /// The number of unique right keys.
    pub fn right_len(&self) -> usize {
        self.bi_multi_map.rev_len()
    }

    /// The number of unique left keys.
    pub fn left_len(&self) -> usize {
        self.bi_multi_map.len()
    }

    /// The number of streams contained in the mapping.
    pub fn len(&self) -> usize {
        self.streams.len()
    }

    /// Returns true of no streams are contained.
    pub fn is_empty(&self) -> bool {
        self.bi_multi_map.is_empty()
    }

    /// Empties the structure.
    pub fn clear(&mut self) {
        self.bi_multi_map.clear();
        self.streams.clear();
    }

    /// Returns a vector of all the (left, right) associations contained in the structure (not
    /// including the streams).
    pub fn collect_keys(&mut self) -> Vec<(L, R)> {
        self.bi_multi_map.flat_collect()
    }
}

impl<L: Clone + Hash + Eq, R: Clone + Hash + Eq, St: Stream + Unpin> Stream
    for BiMultiMapStreams<L, R, St>
{
    type Item = (L, R, Option<St::Item>);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut futures_task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.streams).poll_next(cx)) {
            Some(((left, right), output)) => {
                self.bi_multi_map.remove(&left, &right);
                Poll::Ready(Some((left, right, output)))
            }
            None => Poll::Ready(None),
        }
    }
}

pub struct RightIterMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, St: Stream + Unpin> {
    inner: Vec<(R, *const Task<(L, R), StreamFuture<St>>)>,
    _marker: PhantomData<&'a mut MappedStreams<(L, R), St>>,
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, St: Stream + Unpin>
    RightIterMut<'a, L, R, St>
{
    fn new(streams: &'a mut BiMultiMapStreams<L, R, St>, left: &L) -> Self {
        let rights = streams.bi_multi_map.get(left);
        let mut tasks = vec![];
        rights.into_iter().for_each(|right| {
            let key = (left.clone(), right);
            let hash_task = streams.streams.inner.hash_set.get(&key).unwrap();
            tasks.push((key.1, Arc::as_ptr(&hash_task.inner)));
        });
        RightIterMut {
            inner: tasks,
            _marker: PhantomData,
        }
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, St: Stream + Unpin> Iterator
    for RightIterMut<'a, L, R, St>
{
    type Item = (R, StMut<'a, (L, R), St>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .pop()
            .map(|(left, task)| (left, StMut::new_from_ptr(task)))
    }
}

pub struct LeftIterMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, St: Stream + Unpin> {
    inner: Vec<(L, *const Task<(L, R), StreamFuture<St>>)>,
    _marker: PhantomData<&'a mut MappedStreams<(L, R), St>>,
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, St: Stream + Unpin> Iterator
    for LeftIterMut<'a, L, R, St>
{
    type Item = (L, StMut<'a, (L, R), St>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .pop()
            .map(|(right, task)| (right, StMut::new_from_ptr(task)))
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, St: Stream + Unpin> LeftIterMut<'a, L, R, St> {
    fn new(streams: &'a mut BiMultiMapStreams<L, R, St>, right: &R) -> Self {
        let lefts = streams.bi_multi_map.rev_get(right);
        let mut tasks = vec![];
        lefts.into_iter().for_each(|left| {
            let key = (left, right.clone());
            let hash_task = streams.streams.inner.hash_set.get(&key).unwrap();
            tasks.push((key.0, Arc::as_ptr(&hash_task.inner)));
        });
        LeftIterMut {
            inner: tasks,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
#[allow(unused_imports)]
pub mod tests {
    use crate::BiMultiMapStreams;

    use super::MappedStreams;
    use futures::executor::block_on;
    use futures_core::Stream;
    use futures_lite::FutureExt as FutureLiteExt;
    use futures_task::Poll;
    use futures_timer::Delay;
    use futures_util::StreamExt;
    use std::time::Duration;
    use std::{pin::Pin, task::ready};

    struct DelayStream {
        num: u8,
        interval: u64,
        fut: Option<Delay>,
    }

    impl DelayStream {
        fn new(num: u8, interval: u64) -> Self {
            DelayStream {
                num,
                interval,
                fut: None,
            }
        }
    }

    impl Stream for DelayStream {
        type Item = ();

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut futures_task::Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            match &mut self.fut {
                Some(fut) => {
                    ready!(Pin::new(fut).poll(cx));
                    self.fut = None;
                    self.num = self.num - 1;
                    Poll::Ready(Some(()))
                }
                None => match self.num {
                    0 => Poll::Ready(None),
                    _ => {
                        println!("interval: {}", self.interval);
                        self.fut = Some(Delay::new(Duration::from_millis(self.interval)));
                        self.poll_next(cx)
                    }
                },
            }
        }
    }

    #[test]
    fn map_streams() {
        let mut streams = BiMultiMapStreams::new();
        let s1 = futures::stream::iter(vec![1]);
        let s2 = futures::stream::iter(vec![2]);
        streams.insert(1, 1, s1);
        streams.insert(2, 2, s2);
        let output: Vec<_> = block_on(streams.collect());
        assert_eq!(
            output,
            vec![(1, 1, Some(1)), (2, 2, Some(2)), (1, 1, None), (2, 2, None)]
        );
    }

    #[test]
    fn mutate_streams() {
        let mut streams = BiMultiMapStreams::new();
        streams.insert(1, 1, DelayStream::new(1, 500));
        streams.insert(2, 1, DelayStream::new(1, 600));
        streams.get_mut(&1, &1).unwrap().interval = 700;
        assert_eq!(block_on(streams.next()), Some((2, 1, Some(()))));
        assert_eq!(block_on(streams.next()), Some((2, 1, None)));
        assert_eq!(block_on(streams.next()), Some((1, 1, Some(()))));
        assert_eq!(block_on(streams.next()), Some((1, 1, None)));
        assert!(streams.is_empty());
    }

    #[test]
    fn remove_streams() {
        let mut streams: BiMultiMapStreams<i32, i32, DelayStream> = BiMultiMapStreams::new();
        streams.insert(1, 1, DelayStream::new(1, 50));
        streams.insert(2, 1, DelayStream::new(1, 60));
        streams.remove(&1, &1);
        assert_eq!(block_on(streams.next()), Some((2, 1, Some(()))));
    }
}
