use super::{task::Task, FutMut, MappedFutures};
use bisetmap::BisetMap;
use core::hash::Hash;
use futures_core::{Future, Stream};
use futures_task::Poll;
use std::{marker::PhantomData, pin::Pin, sync::Arc, task::ready};

/// This is a BiMultiMap structure that associates (LeftKey, RightKey) pairs with a future.
/// Bi - This is a two-way mapping; each kind of key is mapped to keys of the other kind (and futures)
/// Multi - Each key can have associations with multiple keys of the other kind.
///
/// The result is a structure that can be indexed with either a left key or a right key to obtain a
/// set of keys of the other kind, and a set of futures, or can be indexed with a (left, right) key
/// pair to obtain at most one future.
pub struct BiMultiMapFutures<L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> {
    bi_multi_map: BisetMap<L, R>,
    futures: MappedFutures<(L, R), Fut>,
}

impl<L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> BiMultiMapFutures<L, R, Fut> {
    /// Create a new mapping.
    pub fn new() -> Self {
        Self {
            bi_multi_map: BisetMap::new(),
            futures: MappedFutures::new(),
        }
    }

    /// Insert a new (left, right, future) associations.
    /// Returns true if another future was not removed to make room for the provided future.
    pub fn insert(&mut self, left: L, right: R, future: Fut) -> bool {
        self.bi_multi_map.insert(left.clone(), right.clone());
        self.futures.insert((left, right), future)
    }

    /// Insert a new (left, right, future) associations.
    /// Returns Some(fut) if a future was evicted to make room for this association.
    pub fn insert_pin(&mut self, left: L, right: R, future: Fut) -> Option<Fut>
    where
        Fut: Unpin,
    {
        let ret = self.futures.replace((left.clone(), right.clone()), future);
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

    /// Remove and return the corresponding future, if one exists.
    pub fn remove(&mut self, left: &L, right: &R) -> Option<Fut>
    where
        Fut: Unpin,
    {
        self.bi_multi_map.remove(left, right);
        self.futures.remove(&(left.clone(), right.clone()))
    }

    /// Remove and drop the corresponding future.
    /// Returns true if a future was cancelled.
    pub fn cancel(&mut self, left: &L, right: &R) -> bool {
        self.bi_multi_map.remove(left, right);
        self.futures.cancel(&(left.clone(), right.clone()))
    }

    /// Get a mutable reference to the corresponding future.
    pub fn get_mut(&mut self, left: &L, right: &R) -> Option<FutMut<(L, R), Fut>>
    where
        Fut: Unpin,
    {
        self.futures.get_mut(&(left.clone(), right.clone()))
    }

    /// Get a mutable reference to the corresponding future.
    pub fn get_pin_mut(&mut self, left: &L, right: &R) -> Option<Pin<FutMut<(L, R), Fut>>> {
        self.futures.get_pin_mut(&(left.clone(), right.clone()))
    }

    /// Get an iterator of mutable references to the futures and right keys that are associated
    /// with the provided left key.
    pub fn get_right_mut(&mut self, left: &L) -> RightIterMut<L, R, Fut>
    where
        Fut: Unpin,
    {
        RightIterMut::new(self, left)
    }

    /// Get an iterator of mutable references to the futures and left keys that are associated
    /// with the provided right key.
    pub fn get_left_mut(&mut self, right: &R) -> LeftIterMut<L, R, Fut>
    where
        Fut: Unpin,
    {
        LeftIterMut::new(self, right)
    }

    /// Get an iterator of mutable pinned references to the futures and right keys that are associated
    /// with the provided left key.
    pub fn get_right_pin_mut(&mut self, left: &L) -> RightIterPinMut<L, R, Fut> {
        RightIterPinMut::new(self, left)
    }

    /// Get an iterator of mutable pinned references to the futures and left keys that are associated
    /// with the provided right key.
    pub fn get_left_pin_mut(&mut self, right: &R) -> LeftIterPinMut<L, R, Fut> {
        LeftIterPinMut::new(self, right)
    }

    /// Remove and drop all futures corresponding to the provided left key.
    /// Returns the right keys of all futures that were dropped.
    pub fn left_cancel(&mut self, left: &L) -> Vec<R> {
        let rights = self.bi_multi_map.delete(left);
        rights.iter().for_each(|right| {
            self.futures.cancel(&(left.clone(), right.clone()));
        });
        rights
    }

    /// Remove and drop all futures corresponding to the provided right key.
    /// Returns the left keys of all futures that were dropped.
    pub fn right_cancel(&mut self, right: &R) -> Vec<L> {
        let lefts = self.bi_multi_map.rev_delete(right);
        lefts.iter().for_each(|left| {
            self.futures.cancel(&(left.clone(), right.clone()));
        });
        lefts
    }

    /// Remove and return the corresponding futures and their right keys.
    pub fn left_remove(&mut self, left: &L) -> Vec<(R, Fut)>
    where
        Fut: Unpin,
    {
        let rights = self.bi_multi_map.delete(left);
        rights
            .iter()
            .map(|right| {
                (
                    right.clone(),
                    self.futures.remove(&(left.clone(), right.clone())).unwrap(),
                )
            })
            .collect()
    }

    /// Remove and return the corresponding futures and their left keys.
    pub fn right_remove(&mut self, right: &R) -> Vec<(L, Fut)>
    where
        Fut: Unpin,
    {
        let lefts = self.bi_multi_map.rev_delete(right);
        lefts
            .iter()
            .map(|left| {
                (
                    left.clone(),
                    self.futures.remove(&(left.clone(), right.clone())).unwrap(),
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

    /// The number of futures contained in the mapping.
    pub fn len(&self) -> usize {
        self.futures.len()
    }

    /// Returns true of no futures are contained.
    pub fn is_empty(&self) -> bool {
        self.bi_multi_map.is_empty()
    }

    /// Empties the structure.
    pub fn clear(&mut self) {
        self.bi_multi_map.clear();
        self.futures.clear();
    }

    /// Returns a vector of all the (left, right) associations contained in the structure (not
    /// including the futures).
    pub fn collect_keys(&mut self) -> Vec<(L, R)> {
        self.bi_multi_map.flat_collect()
    }
}

impl<L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> Stream
    for BiMultiMapFutures<L, R, Fut>
{
    type Item = (L, R, Fut::Output);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut futures_task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.futures).poll_next(cx)) {
            Some(((left, right), output)) => Poll::Ready(Some((left, right, output))),
            None => Poll::Ready(None),
        }
    }
}

pub struct RightIterPinMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> {
    inner: Vec<(R, *const Task<(L, R), Fut>)>,
    _marker: PhantomData<&'a mut MappedFutures<(L, R), Fut>>,
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> RightIterPinMut<'a, L, R, Fut> {
    fn new(futures: &'a mut BiMultiMapFutures<L, R, Fut>, left: &L) -> Self {
        let rights = futures.bi_multi_map.get(left);
        let mut tasks = vec![];
        rights.into_iter().for_each(|right| {
            let key = (left.clone(), right);
            let hash_task = futures.futures.hash_set.get(&key).unwrap();
            tasks.push((key.1, Arc::as_ptr(&hash_task.inner)));
        });
        RightIterPinMut {
            inner: tasks,
            _marker: PhantomData,
        }
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> Iterator
    for RightIterPinMut<'a, L, R, Fut>
{
    type Item = (R, Pin<FutMut<'a, (L, R), Fut>>);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            self.inner
                .pop()
                .map(|(left, task)| (left, Pin::new_unchecked(FutMut::new_from_ptr(task))))
        }
    }
}

pub struct LeftIterPinMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> {
    inner: Vec<(L, *const Task<(L, R), Fut>)>,
    _marker: PhantomData<&'a mut MappedFutures<(L, R), Fut>>,
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> Iterator
    for LeftIterPinMut<'a, L, R, Fut>
{
    type Item = (L, Pin<FutMut<'a, (L, R), Fut>>);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            self.inner
                .pop()
                .map(|(right, task)| (right, Pin::new_unchecked(FutMut::new_from_ptr(task))))
        }
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> LeftIterPinMut<'a, L, R, Fut> {
    fn new(futures: &'a mut BiMultiMapFutures<L, R, Fut>, right: &R) -> Self {
        let lefts = futures.bi_multi_map.rev_get(right);
        let mut tasks = vec![];
        lefts.into_iter().for_each(|left| {
            let key = (left, right.clone());
            let hash_task = futures.futures.hash_set.get(&key).unwrap();
            tasks.push((key.0, Arc::as_ptr(&hash_task.inner)));
        });
        LeftIterPinMut {
            inner: tasks,
            _marker: PhantomData,
        }
    }
}

// impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin>
//     LeftIterMut<'a, L, R, Fut>
// {
//     pub fn key(&self) -> &R {
//         &self.right
//     }
// }

pub struct RightIterMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future>(
    RightIterPinMut<'a, L, R, Fut>,
);

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin> Iterator
    for RightIterMut<'a, L, R, Fut>
{
    type Item = (R, FutMut<'a, (L, R), Fut>);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(key, fut)| (key, Pin::into_inner(fut)))
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin>
    RightIterMut<'a, L, R, Fut>
{
    fn new(futures: &'a mut BiMultiMapFutures<L, R, Fut>, left: &L) -> Self {
        Self(RightIterPinMut::new(futures, left))
    }
}

pub struct LeftIterMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future>(
    LeftIterPinMut<'a, L, R, Fut>,
);

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin> Iterator
    for LeftIterMut<'a, L, R, Fut>
{
    type Item = (L, FutMut<'a, (L, R), Fut>);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(key, fut)| (key, Pin::into_inner(fut)))
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin>
    LeftIterMut<'a, L, R, Fut>
{
    fn new(futures: &'a mut BiMultiMapFutures<L, R, Fut>, right: &R) -> Self {
        Self(LeftIterPinMut::new(futures, right))
    }
}

#[cfg(test)]
#[allow(unused_imports)]
pub mod tests {
    use futures::executor::block_on;
    use futures_timer::Delay;
    use futures_util::StreamExt;
    use std::time::Duration;

    use super::BiMultiMapFutures;

    fn insert_millis(
        futures: &mut BiMultiMapFutures<u64, u64, Delay>,
        key: (u64, u64),
        millis: u64,
    ) {
        futures.insert(key.0, key.1, Delay::new(Duration::from_millis(millis)));
    }

    #[test]
    fn bi_multi_map_futures() {
        let mut futures = BiMultiMapFutures::new();
        insert_millis(&mut futures, (1, 1), 50);
        insert_millis(&mut futures, (1, 2), 60);
        insert_millis(&mut futures, (2, 3), 70);
        futures.cancel(&1, &2);
        let mut ones = futures.get_right_mut(&1);
        ones.next().unwrap().1.reset(Duration::from_millis(80));
        assert_eq!(block_on(futures.next()).unwrap(), ((2, 3, ())));
        assert_eq!(block_on(futures.next()).unwrap(), ((1, 1, ())));
        assert_eq!(block_on(futures.next()), None);
    }

    #[test]
    fn iter() {
        let mut futures = BiMultiMapFutures::new();
        insert_millis(&mut futures, (1, 3), 50);
        insert_millis(&mut futures, (1, 2), 100);
        insert_millis(&mut futures, (1, 1), 150);
        futures
            .get_right_mut(&1)
            .for_each(|(right, mut fut)| fut.reset(Duration::from_millis(right * 100)));
        assert_eq!(block_on(futures.next()).unwrap().1, 1);
        assert_eq!(block_on(futures.next()).unwrap().1, 2);
        assert_eq!(block_on(futures.next()).unwrap().1, 3);
        assert_eq!(block_on(futures.next()), None);
    }
}
