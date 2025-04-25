//! An unbounded map of streams

use super::task::Task;
use super::{FutMut, MappedFutures};

use core::fmt::{self, Debug};
use core::hash::Hash;
use core::pin::Pin;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::ops::{Deref, DerefMut};

use futures_core::ready;
use futures_core::stream::{FusedStream, Stream};
use futures_core::task::{Context, Poll};

use futures_util::stream::{StreamExt, StreamFuture};

/// An unbounded bimultimap of streams
///
/// This "combinator" provides the ability to maintain a map of streams
/// and drive them all to completion.
///
/// Streams are inserted into this map with keys and their realized values are
/// yielded as they become ready. Streams will only be polled when they
/// generate notifications. This allows to coordinate a large number of streams.
///
/// You can start with an empty map with the `BiMultiMappedStreams::new` constructor or
/// you can collect from an iterator whose items are (leftkey, rightkey , stream) triples.
#[must_use = "streams do nothing unless polled"]
pub struct MappedStreams<K: Hash + Eq, St, S = RandomState>
where
    S: BuildHasher,
{
    pub(super) inner: MappedFutures<K, StreamFuture<St>, S>,
}

impl<K: Hash + Eq + Clone, St: Debug> Debug for MappedStreams<K, St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MappedStreams {{ ... }}")
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin> MappedStreams<K, St, RandomState> {
    /// Constructs a new, empty `MappedStreams`
    ///
    /// The returned `MappedStreams` does not contain any streams and, in this
    /// state, `MappedStreams::poll` will return `Poll::Ready(None)`.
    pub fn new() -> Self {
        Self {
            inner: MappedFutures::new(),
        }
    }
}

impl<K: Hash + Eq + Clone, St: Stream, S: BuildHasher> MappedStreams<K, St, S> {
    /// Constructs a new, empty `MappedStreams` that maps using the specified hasher.
    ///
    /// The returned `MappedStreams` does not contain any streams and, in this
    /// state, `MappedStreams::poll` will return `Poll::Ready(None)`.
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            inner: MappedFutures::with_hasher(hasher),
        }
    }

    // Gets the BuildHasher associated with these MappedStreams.
    pub fn hasher(&self) -> &S {
        self.inner.hasher()
    }

    /// Returns the number of streams contained in the map.
    ///
    /// This represents the total number of in-flight streams.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the map contains no streams
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Remove a stream from the set, dropping it.
    ///
    /// Returns true if a stream was cancelled.
    pub fn cancel(&mut self, key: &K) -> bool {
        self.inner.cancel(key)
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin, S: BuildHasher> MappedStreams<K, St, S> {
    /// Returns an iterator that allows inspecting each stream in the map.
    pub fn iter(&self) -> Iter<'_, K, St, S> {
        Iter(self.inner.iter())
    }

    /// Returns an iterator that allows modifying each stream in the map.
    pub fn iter_mut(&mut self) -> IterMut<'_, K, St, S> {
        IterMut(self.inner.iter_mut())
    }

    /// Clears the map, removing all streams.
    pub fn clear(&mut self) {
        self.inner.clear()
    }

    /// Remove a stream from the map and return it.
    pub fn remove(&mut self, key: &K) -> Option<St> {
        if let Some(st_fut) = self.inner.remove(key) {
            return st_fut.into_inner();
        }
        None
    }

    /// Get a mutable reference to the mapped stream.
    pub fn get_mut<'a>(&'a mut self, key: &K) -> Option<StMut<'a, K, St>> {
        if let Some(st_fut) = self.inner.get_mut(key) {
            return Some(StMut { inner: st_fut });
        }
        None
    }

    /// Get a shared reference to the mapped stream.
    pub fn get<'a>(&mut self, key: &K) -> Option<&'a St> {
        if let Some(st_fut) = self.inner.get(key) {
            return st_fut.get_ref();
        }
        None
    }

    /// Insert a stream into the map.
    ///
    /// This function submits the given stream to the map for managing. This
    /// function will not call `poll` on the submitted stream. The caller must
    /// ensure that `MappedStreams::poll` is called in order to receive task
    /// notifications.
    pub fn insert(&mut self, key: K, stream: St) -> bool {
        self.inner.insert(key, stream.into_future())
    }

    /// Insert a future into the set and return the displaced future, if there was one.
    pub fn replace(&mut self, key: K, stream: St) -> Option<St> {
        let replacing = self.remove(&key);
        self.insert(key, stream);
        replacing
    }
}

pub struct StMut<'a, K: Hash + Eq, St: Stream + Unpin> {
    inner: FutMut<'a, K, StreamFuture<St>>,
}

impl<'a, K: Hash + Eq, St: Stream + Unpin> Deref for StMut<'a, K, St> {
    type Target = St;

    fn deref(&self) -> &Self::Target {
        self.inner.get_ref().unwrap()
    }
}
impl<'a, K: Hash + Eq, St: Stream + Unpin> DerefMut for StMut<'a, K, St> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.get_mut().unwrap()
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin> Default for MappedStreams<K, St> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, K: Hash + Eq, St: Stream + Unpin> StMut<'a, K, St> {
    // fn new(task: &'a Arc<Task<K, StreamFuture<St>>>) -> Self {
    //     FutMut {
    //         inner: Arc::as_ptr(task),
    //         mutated: false,
    //         _marker: PhantomData,
    //     }
    // }
    pub(super) fn new_from_ptr(task: *const Task<K, StreamFuture<St>>) -> Self {
        StMut {
            inner: FutMut::new_from_ptr(task),
        }
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin> Stream for MappedStreams<K, St> {
    type Item = (K, Option<St::Item>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.inner.poll_next_unpin(cx)) {
                Some((key, (Some(item), remaining))) => {
                    self.insert(key.clone(), remaining);
                    return Poll::Ready(Some((key, Some(item))));
                }
                Some((key, (None, _))) => {
                    // `MappedFutures` thinks it isn't terminated
                    // because it yielded a Some.
                    // Return the key of the stream that terminated.
                    return Poll::Ready(Some((key, None)));
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin> FusedStream for MappedStreams<K, St> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

/// Convert a list of streams into a `Stream` of results from the streams.
///
/// This essentially takes a list of streams (e.g. a vector, an iterator, etc.)
/// and bundles them together into a single stream.
/// The stream will yield items as they become available on the underlying
/// streams internally, in the order they become available.
///
/// Note that the returned set can also be used to dynamically push more
/// streams into the set as they become available.
pub fn map_all<K, I, St>(streams: I) -> MappedStreams<K, St, RandomState>
where
    K: Hash + Eq + Clone,
    I: IntoIterator<Item = (K, St)>,
    St: Stream + Unpin,
{
    let mut map = MappedStreams::new();
    map.extend(streams);
    map
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin> FromIterator<(K, St)> for MappedStreams<K, St> {
    fn from_iter<T: IntoIterator<Item = (K, St)>>(iter: T) -> Self {
        let mut map = MappedStreams::new();

        for (key, stream) in iter {
            map.insert(key, stream);
        }
        map
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin> Extend<(K, St)> for MappedStreams<K, St> {
    fn extend<T: IntoIterator<Item = (K, St)>>(&mut self, iter: T) {
        for (key, st) in iter {
            self.insert(key, st);
        }
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin, S: BuildHasher> IntoIterator
    for MappedStreams<K, St, S>
{
    type Item = St;
    type IntoIter = IntoIter<K, St, S>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.inner.into_iter())
    }
}

impl<'a, K: Clone + Hash + Eq, St: Stream + Unpin, S: BuildHasher> IntoIterator
    for &'a MappedStreams<K, St, S>
{
    type Item = &'a St;
    type IntoIter = Iter<'a, K, St, S>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, K: Clone + Hash + Eq, St: Stream + Unpin, S: BuildHasher> IntoIterator
    for &'a mut MappedStreams<K, St, S>
{
    type Item = &'a mut St;
    type IntoIter = IterMut<'a, K, St, S>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// Immutable iterator over all streams in the map.
#[derive(Debug)]
pub struct Iter<'a, K: Hash + Eq, St: Unpin, S: BuildHasher>(
    super::Iter<'a, K, StreamFuture<St>, S>,
);

/// Mutable iterator over all streams in the map.
#[derive(Debug)]
pub struct IterMut<'a, K: Hash + Eq, St: Unpin, S: BuildHasher>(
    super::IterMut<'a, K, StreamFuture<St>, S>,
);

/// Owned iterator over all streams in the unordered set.
#[derive(Debug)]
pub struct IntoIter<K: Hash + Eq, St: Unpin, S: BuildHasher>(
    super::IntoIter<K, StreamFuture<St>, S>,
);

impl<'a, K: Clone + Hash + Eq, St: Stream + Unpin, S: BuildHasher> Iterator for Iter<'a, K, St, S> {
    type Item = &'a St;

    fn next(&mut self) -> Option<Self::Item> {
        let st = self.0.next()?;
        let next = st.get_ref();
        // This should always be true because MappedFutures removes completed futures.
        debug_assert!(next.is_some());
        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin, S: BuildHasher> ExactSizeIterator
    for Iter<'_, K, St, S>
{
}

impl<'a, K: Clone + Hash + Eq, St: Stream + Unpin, S: BuildHasher> Iterator
    for IterMut<'a, K, St, S>
{
    type Item = &'a mut St;

    fn next(&mut self) -> Option<Self::Item> {
        let st = self.0.next()?;
        let next = st.get_mut();
        // This should always be true because MappedFutures removes completed futures.
        debug_assert!(next.is_some());
        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin, S: BuildHasher> ExactSizeIterator
    for IterMut<'_, K, St, S>
{
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin, S: BuildHasher> Iterator for IntoIter<K, St, S> {
    type Item = St;

    fn next(&mut self) -> Option<Self::Item> {
        let st = self.0.next()?;
        let next = st.into_inner();
        // This should always be true because MappedFutures removes completed futures.
        debug_assert!(next.is_some());
        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin, S: BuildHasher> ExactSizeIterator
    for IntoIter<K, St, S>
{
}

#[cfg(test)]
pub mod tests {
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
        let mut streams = MappedStreams::new();
        let s1 = futures::stream::iter(vec![1]);
        let s2 = futures::stream::iter(vec![2]);
        streams.insert(1, s1);
        streams.insert(2, s2);
        let output: Vec<_> = block_on(streams.collect());
        assert_eq!(
            output,
            vec![(1, Some(1)), (2, Some(2)), (1, None), (2, None)]
        );
    }

    #[test]
    fn mutate_streams() {
        let mut streams = MappedStreams::new();
        streams.insert(1, DelayStream::new(1, 500));
        streams.insert(2, DelayStream::new(1, 600));
        streams.get_mut(&1).unwrap().interval = 700;
        assert_eq!(block_on(streams.next()), Some((2, Some(()))));
        assert_eq!(block_on(streams.next()), Some((2, None)));
        assert_eq!(block_on(streams.next()), Some((1, Some(()))));
        assert_eq!(block_on(streams.next()), Some((1, None)));
        assert!(streams.is_empty());
    }

    #[test]
    fn remove_streams() {
        let mut streams: MappedStreams<i32, DelayStream> = MappedStreams::new();
        streams.insert(1, DelayStream::new(1, 50));
        streams.insert(2, DelayStream::new(1, 60));
        streams.remove(&1);
        assert_eq!(block_on(streams.next()), Some((2, Some(()))));
    }
}
