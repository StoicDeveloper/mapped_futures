use alloc::sync::Arc;
use core::cell::UnsafeCell;
use core::hash::Hash;
use core::ptr;
use core::sync::atomic::AtomicPtr;
use core::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use futures_util::task::AtomicWaker;

use super::abort::abort;
use super::task::Task;

pub(super) enum Dequeue<K: Hash + Eq, Fut> {
    Data(*const Task<K, Fut>),
    Empty,
    Inconsistent,
}

pub(super) struct ReadyToRunQueue<K: Hash + Eq, Fut> {
    // The waker of the task using `MappedFutures`.
    pub(super) waker: AtomicWaker,

    // Head/tail of the readiness queue
    pub(super) head: AtomicPtr<Task<K, Fut>>,
    pub(super) tail: UnsafeCell<*const Task<K, Fut>>,
    pub(super) stub: Arc<Task<K, Fut>>,
}

/// An MPSC queue into which the tasks containing the futures are inserted
/// whenever the future inside is scheduled for polling.
impl<K: Hash + Eq, Fut> ReadyToRunQueue<K, Fut> {
    /// The enqueue function from the 1024cores intrusive MPSC queue algorithm.
    pub(super) fn enqueue(&self, task: *const Task<K, Fut>) {
        unsafe {
            debug_assert!((*task).queued.load(Relaxed));

            // This action does not require any coordination
            (*task).next_ready_to_run.store(ptr::null_mut(), Relaxed);

            // Note that these atomic orderings come from 1024cores
            let task = task as *mut _;
            let prev = self.head.swap(task, AcqRel);
            (*prev).next_ready_to_run.store(task, Release);
        }
    }

    /// The dequeue function from the 1024cores intrusive MPSC queue algorithm
    ///
    /// Note that this is unsafe as it required mutual exclusion (only one
    /// thread can call this) to be guaranteed elsewhere.
    pub(super) unsafe fn dequeue(&self) -> Dequeue<K, Fut> {
        let mut tail = *self.tail.get();
        let mut next = (*tail).next_ready_to_run.load(Acquire);

        if tail == self.stub() {
            if next.is_null() {
                return Dequeue::Empty;
            }

            *self.tail.get() = next;
            tail = next;
            next = (*next).next_ready_to_run.load(Acquire);
        }

        if !next.is_null() {
            *self.tail.get() = next;
            debug_assert!(tail != self.stub());
            return Dequeue::Data(tail);
        }

        if self.head.load(Acquire) as *const _ != tail {
            return Dequeue::Inconsistent;
        }

        self.enqueue(self.stub());

        next = (*tail).next_ready_to_run.load(Acquire);

        if !next.is_null() {
            *self.tail.get() = next;
            return Dequeue::Data(tail);
        }

        Dequeue::Inconsistent
    }

    pub(super) fn stub(&self) -> *const Task<K, Fut> {
        Arc::as_ptr(&self.stub)
    }

    // Clear the queue of tasks.
    //
    // Note that each task has a strong reference count associated with it
    // which is owned by the ready to run queue. This method just pulls out
    // tasks and drops their refcounts.
    //
    // # Safety
    //
    // - All tasks **must** have had their futures dropped already (by MappedFutures::clear)
    // - The caller **must** guarantee unique access to `self`
    pub(crate) unsafe fn clear(&self) {
        loop {
            // SAFETY: We have the guarantee of mutual exclusion required by `dequeue`.
            match self.dequeue() {
                Dequeue::Empty => break,
                Dequeue::Inconsistent => abort("inconsistent in drop"),
                Dequeue::Data(ptr) => drop(Arc::from_raw(ptr)),
            }
        }
    }
}

impl<K: Hash + Eq, Fut> Drop for ReadyToRunQueue<K, Fut> {
    fn drop(&mut self) {
        // Once we're in the destructor for `Inner<Fut>` we need to clear out
        // the ready to run queue of tasks if there's anything left in there.

        // All tasks have had their futures dropped already by the `MappedFutures`
        // destructor above, and we have &mut self, so this is safe.
        unsafe {
            self.clear();
        }
    }
}
