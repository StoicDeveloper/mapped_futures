use super::task::{HashTask, Task};
use super::MappedFutures;
use core::hash::Hash;
use core::marker::PhantomData;
use core::pin::Pin;
use core::ptr;
use core::sync::atomic::Ordering::Relaxed;
use std::hash::BuildHasher;

/// Mutable iterator over all futures in the unordered set.
#[derive(Debug)]
pub struct IterPinMut<'a, K: Hash + Eq, Fut, S: BuildHasher> {
    pub(super) task: *const Task<K, Fut>,
    pub(super) len: usize,
    pub(super) _marker: PhantomData<&'a mut MappedFutures<K, Fut, S>>,
}

/// Mutable iterator over all futures in the unordered set.
#[derive(Debug)]
pub struct IterMut<'a, K: Hash + Eq, Fut: Unpin, S: BuildHasher>(
    pub(super) IterPinMut<'a, K, Fut, S>,
);

/// Immutable iterator over all futures in the unordered set.
#[derive(Debug)]
pub struct IterPinRef<'a, K: Hash + Eq, Fut, S: BuildHasher> {
    pub(super) task: *const Task<K, Fut>,
    pub(super) len: usize,
    pub(super) pending_next_all: *mut Task<K, Fut>,
    pub(super) _marker: PhantomData<&'a MappedFutures<K, Fut, S>>,
}

/// Immutable iterator over all the futures in the unordered set.
#[derive(Debug)]
pub struct Iter<'a, K: Hash + Eq, Fut: Unpin, S: BuildHasher>(pub(super) IterPinRef<'a, K, Fut, S>);

/// Owned iterator over all futures in the unordered set.
#[derive(Debug)]
pub struct IntoIter<K: Hash + Eq, Fut: Unpin, S: BuildHasher> {
    pub(super) len: usize,
    pub(super) inner: MappedFutures<K, Fut, S>,
}

/// Immutable iterator over the keys in the mapping.
// #[derive(Debug)]
pub struct Keys<'a, K: Hash + Eq, Fut> {
    pub(super) inner: std::collections::hash_set::Iter<'a, HashTask<K, Fut>>,
}

impl<K: Hash + Eq, Fut: Unpin, S: BuildHasher> Iterator for IntoIter<K, Fut, S> {
    type Item = Fut;

    fn next(&mut self) -> Option<Self::Item> {
        // `head_all` can be accessed directly and we don't need to spin on
        // `Task::next_all` since we have exclusive access to the set.
        let task = self.inner.head_all.get_mut();

        if (*task).is_null() {
            return None;
        }

        unsafe {
            // Moving out of the future is safe because it is `Unpin`
            let future = (*(**task).future.get()).take().unwrap();

            // Mutable access to a previously shared `MappedFutures` implies
            // that the other threads already released the object before the
            // current thread acquired it, so relaxed ordering can be used and
            // valid `next_all` checks can be skipped.
            let next = (**task).next_all.load(Relaxed);
            *task = next;
            if !task.is_null() {
                *(**task).prev_all.get() = ptr::null_mut();
            }
            self.len -= 1;
            Some(future)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<K: Hash + Eq, Fut: Unpin, S: BuildHasher> ExactSizeIterator for IntoIter<K, Fut, S> {}

impl<'a, K: Hash + Eq, Fut, S: BuildHasher> Iterator for IterPinMut<'a, K, Fut, S> {
    type Item = Pin<&'a mut Fut>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.task.is_null() {
            return None;
        }

        unsafe {
            let future = (*(*self.task).future.get()).as_mut().unwrap();

            // Mutable access to a previously shared `MappedFutures` implies
            // that the other threads already released the object before the
            // current thread acquired it, so relaxed ordering can be used and
            // valid `next_all` checks can be skipped.
            let next = (*self.task).next_all.load(Relaxed);
            self.task = next;
            self.len -= 1;
            Some(Pin::new_unchecked(future))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<K: Hash + Eq, Fut, S: BuildHasher> ExactSizeIterator for IterPinMut<'_, K, Fut, S> {}

impl<'a, K: Hash + Eq, Fut: Unpin, S: BuildHasher> Iterator for IterMut<'a, K, Fut, S> {
    type Item = &'a mut Fut;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Pin::get_mut)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K: Hash + Eq, Fut: Unpin, S: BuildHasher> ExactSizeIterator for IterMut<'_, K, Fut, S> {}

impl<'a, K: Hash + Eq, Fut, S: BuildHasher> Iterator for IterPinRef<'a, K, Fut, S> {
    type Item = Pin<&'a Fut>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.task.is_null() {
            return None;
        }

        unsafe {
            let future = (*(*self.task).future.get()).as_ref().unwrap();

            // Relaxed ordering can be used since acquire ordering when
            // `head_all` was initially read for this iterator implies acquire
            // ordering for all previously inserted nodes (and we don't need to
            // read `len_all` again for any other nodes).
            let next = (*self.task).spin_next_all(self.pending_next_all, Relaxed);
            self.task = next;
            self.len -= 1;
            Some(Pin::new_unchecked(future))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<K: Hash + Eq, Fut, S: BuildHasher> ExactSizeIterator for IterPinRef<'_, K, Fut, S> {}

impl<'a, K: Hash + Eq, Fut: Unpin, S: BuildHasher> Iterator for Iter<'a, K, Fut, S> {
    type Item = &'a Fut;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Pin::get_ref)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K: Hash + Eq, Fut: Unpin, S: BuildHasher> ExactSizeIterator for Iter<'_, K, Fut, S> {}

impl<K: Hash + Eq, Fut: Unpin> ExactSizeIterator for Keys<'_, K, Fut> {}

impl<'a, K: Hash + Eq, Fut> Iterator for Keys<'a, K, Fut> {
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()?.key()
    }
}

// SAFETY: we do nothing thread-local and there is no interior mutability,
// so the usual structural `Send`/`Sync` apply.
unsafe impl<K: Hash + Eq, Fut: Send, S: BuildHasher> Send for IterPinRef<'_, K, Fut, S> {}
unsafe impl<K: Hash + Eq, Fut: Sync, S: BuildHasher> Sync for IterPinRef<'_, K, Fut, S> {}

unsafe impl<K: Hash + Eq, Fut: Send, S: BuildHasher> Send for IterPinMut<'_, K, Fut, S> {}
unsafe impl<K: Hash + Eq, Fut: Sync, S: BuildHasher> Sync for IterPinMut<'_, K, Fut, S> {}

unsafe impl<K: Hash + Eq, Fut: Send + Unpin, S: BuildHasher> Send for IntoIter<K, Fut, S> {}
unsafe impl<K: Hash + Eq, Fut: Sync + Unpin, S: BuildHasher> Sync for IntoIter<K, Fut, S> {}

unsafe impl<K: Hash + Eq, Fut: Send + Unpin> Send for Keys<'_, K, Fut> {}
unsafe impl<K: Hash + Eq, Fut: Sync + Unpin> Sync for Keys<'_, K, Fut> {}
