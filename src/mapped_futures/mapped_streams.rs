//! An unbounded map of streams

use super::MappedFutures;

use core::fmt::{self, Debug};
use core::hash::Hash;
use core::iter::FromIterator;
use core::pin::Pin;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use futures_core::ready;
use futures_core::stream::{FusedStream, Stream};
use futures_core::task::{Context, Poll};

use futures_util::stream::{StreamExt, StreamFuture};

/// An unbounded set of streams
///
/// This "combinator" provides the ability to maintain a set of streams
/// and drive them all to completion.
///
/// Streams are pushed into this set and their realized values are
/// yielded as they become ready. Streams will only be polled when they
/// generate notifications. This allows to coordinate a large number of streams.
///
/// Note that you can create a ready-made `MappedStreams` via the
/// `select_all` function in the `stream` module, or you can start with an
/// empty set with the `MappedStreams::new` constructor.
#[must_use = "streams do nothing unless polled"]
pub struct MappedStreams<K: Hash + Eq, St, S = RandomState>
where
    S: BuildHasher,
{
    inner: MappedFutures<K, StreamFuture<St>, S>,
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

impl<K: Hash + Eq + Clone, St: Stream + Unpin, S: BuildHasher> MappedStreams<K, St, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            inner: MappedFutures::with_hasher(hasher),
        }
    }

    /// Returns the number of streams contained in the set.
    ///
    /// This represents the total number of in-flight streams.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the set contains no streams
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Push a stream into the set.
    ///
    /// This function submits the given stream to the set for managing. This
    /// function will not call `poll` on the submitted stream. The caller must
    /// ensure that `MappedStreams::poll` is called in order to receive task
    /// notifications.
    pub fn insert(&mut self, key: K, stream: St) {
        self.inner.insert(key, stream.into_future());
    }

    /// Returns an iterator that allows inspecting each stream in the set.
    pub fn iter(&self) -> Iter<'_, K, St, S> {
        Iter(self.inner.iter())
    }

    /// Returns an iterator that allows modifying each stream in the set.
    pub fn iter_mut(&mut self) -> IterMut<'_, K, St, S> {
        IterMut(self.inner.iter_mut())
    }

    /// Clears the set, removing all streams.
    pub fn clear(&mut self) {
        self.inner.clear()
    }
}

impl<K: Hash + Eq + Clone, St: Stream + Unpin> Default for MappedStreams<K, St> {
    fn default() -> Self {
        Self::new()
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
                    // We do not return, but poll `MappedFutures`
                    // in the next loop iteration.
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
///
/// This function is only available when the `std` or `alloc` feature of this
/// library is activated, and it is activated by default.
// pub fn map_all<I>(streams: I) -> MappedStreams<I::Item>
// where
//     I: IntoIterator,
//     I::Item: Stream + Unpin,
// {
//     let map = MappedStreams::new();
//
//     for stream in streams {
//         set.push(stream);
//     }
//     map
// }

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
            self.insert(key, st)
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

/// Immutable iterator over all streams in the unordered set.
#[derive(Debug)]
pub struct Iter<'a, K: Hash + Eq, St: Unpin, S: BuildHasher>(
    super::Iter<'a, K, StreamFuture<St>, S>,
);

/// Mutable iterator over all streams in the unordered set.
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
