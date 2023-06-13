use super::MappedFutures;
use bisetmap::BisetMap;
use core::hash::Hash;
use futures_core::Future;
use std::pin::Pin;

pub struct BiMultiMapFutures<L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> {
    bi_multi_map: BisetMap<L, R>,
    futures: MappedFutures<(L, R), Fut>,
}

impl<L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> BiMultiMapFutures<L, R, Fut> {
    pub fn new() -> Self {
        Self {
            bi_multi_map: BisetMap::new(),
            futures: MappedFutures::new(),
        }
    }
    pub fn insert(&mut self, left: L, right: R, future: Fut) -> bool {
        self.bi_multi_map.insert(left.clone(), right.clone());
        self.futures.insert((left, right), future)
    }
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

    pub fn contains(&self, left: &L, right: &R) -> bool {
        self.bi_multi_map.contains(&left, &right)
    }
    pub fn contains_left(&self, left: &L) -> bool {
        self.bi_multi_map.key_exists(&left)
    }
    pub fn contains_right(&self, right: &R) -> bool {
        self.bi_multi_map.value_exists(&right)
    }

    pub fn remove(&mut self, left: &L, right: &R) -> Option<Fut>
    where
        Fut: Unpin,
    {
        self.bi_multi_map.remove(left, right);
        self.futures.remove(&(left.clone(), right.clone()))
    }
    pub fn cancel(&mut self, left: &L, right: &R) -> bool {
        self.bi_multi_map.remove(left, right);
        self.futures.cancel(&(left.clone(), right.clone()))
    }

    pub fn get_mut(&mut self, left: &L, right: &R) -> Option<&mut Fut>
    where
        Fut: Unpin,
    {
        self.futures.get_mut(&(left.clone(), right.clone()))
    }

    pub fn get_pin_mut(&mut self, left: &L, right: &R) -> Option<Pin<&mut Fut>> {
        self.futures.get_pin_mut(&(left.clone(), right.clone()))
    }
    pub fn get_right_mut(&mut self, left: &L) -> RightIterMut<L, R, Fut>
    where
        Fut: Unpin,
    {
        // Return iterator that returns (right, Option<&mut Fut>)
        let rights = self.bi_multi_map.get(left);
        RightIterMut {
            left: left.clone(),
            inner: rights,
            futures: self,
        }
    }
    pub fn get_left_mut(&mut self, right: &R) -> LeftIterMut<L, R, Fut>
    where
        Fut: Unpin,
    {
        // Return iterator that returns (right, Option<&mut Fut>)
        let lefts = self.bi_multi_map.rev_get(right);
        LeftIterMut {
            right: right.clone(),
            inner: lefts,
            futures: self,
        }
    }
    pub fn get_right_pin_mut(&mut self, left: &L) -> RightIterPinMut<L, R, Fut> {
        // Return iterator that returns (right, Option<&mut Fut>)
        let rights = self.bi_multi_map.get(left);
        RightIterPinMut {
            left: left.clone(),
            inner: rights,
            futures: self,
        }
    }
    pub fn get_left_pin_mut(&mut self, right: &R) -> LeftIterPinMut<L, R, Fut> {
        // Return iterator that returns (right, Option<&mut Fut>)
        let lefts = self.bi_multi_map.rev_get(right);
        LeftIterPinMut {
            right: right.clone(),
            inner: lefts,
            futures: self,
        }
    }
    pub fn left_cancel(&mut self, left: &L) -> Vec<R> {
        let rights = self.bi_multi_map.delete(left);
        rights.iter().for_each(|right| {
            self.futures.cancel(&(left.clone(), right.clone()));
        });
        rights
    }
    pub fn right_cancel(&mut self, right: &R) -> Vec<L> {
        let lefts = self.bi_multi_map.rev_delete(right);
        lefts.iter().for_each(|left| {
            self.futures.cancel(&(left.clone(), right.clone()));
        });
        lefts
    }
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
    pub fn right_len(&self) -> usize {
        self.bi_multi_map.rev_len()
    }
    pub fn left_len(&self) -> usize {
        self.bi_multi_map.len()
    }
    pub fn len(&self) -> usize {
        self.futures.len()
    }
    pub fn is_empty(&self) -> bool {
        self.bi_multi_map.is_empty()
    }
    pub fn clear(&mut self) {
        self.bi_multi_map.clear();
        self.futures.clear();
    }
    pub fn collect_keys(&mut self) -> Vec<(L, R)> {
        self.bi_multi_map.flat_collect()
    }
}

pub struct RightIterMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin> {
    left: L,
    inner: Vec<R>,
    futures: &'a mut BiMultiMapFutures<L, R, Fut>,
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin> Iterator
    for RightIterMut<'a, L, R, Fut>
{
    type Item = (R, &'a mut Fut);

    fn next(&mut self) -> Option<Self::Item> {
        let right = self.inner.pop();
        match right {
            Some(right) => {
                let fut: Option<&'a mut Fut> = self
                    .futures
                    .futures
                    .get_mut(&(self.left.clone(), right.clone()));
                Some((right, fut.unwrap()))
                // None
            }
            None => None,
        }
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin>
    RightIterMut<'a, L, R, Fut>
{
    pub fn key(&self) -> &L {
        &self.left
    }
}

pub struct LeftIterMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin> {
    right: R,
    inner: Vec<L>,
    futures: &'a mut BiMultiMapFutures<L, R, Fut>,
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin> Iterator
    for LeftIterMut<'a, L, R, Fut>
{
    type Item = (L, &'a mut Fut);

    fn next(&mut self) -> Option<Self::Item> {
        let left = self.inner.pop();
        match left {
            Some(left) => {
                let fut: Option<&'a mut Fut> = self
                    .futures
                    .futures
                    .get_mut(&(left.clone(), self.right.clone()));
                Some((left, fut.unwrap()))
                // None
            }
            None => None,
        }
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future + Unpin>
    LeftIterMut<'a, L, R, Fut>
{
    pub fn key(&self) -> &R {
        &self.right
    }
}

pub struct RightIterPinMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> {
    left: L,
    inner: Vec<R>,
    futures: &'a mut BiMultiMapFutures<L, R, Fut>,
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> Iterator
    for RightIterPinMut<'a, L, R, Fut>
{
    type Item = (R, Pin<&'a mut Fut>);

    fn next(&mut self) -> Option<Self::Item> {
        let right = self.inner.pop();
        match right {
            Some(right) => {
                let fut: Option<Pin<&'a mut Fut>> = self
                    .futures
                    .futures
                    .get_pin_mut(&(self.left.clone(), right.clone()));
                Some((right, fut.unwrap()))
            }
            None => None,
        }
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> RightIterPinMut<'a, L, R, Fut> {
    pub fn key(&self) -> &L {
        &self.left
    }
}

pub struct LeftIterPinMut<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> {
    right: R,
    inner: Vec<L>,
    futures: &'a mut BiMultiMapFutures<L, R, Fut>,
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> Iterator
    for LeftIterPinMut<'a, L, R, Fut>
{
    type Item = (L, Pin<&'a mut Fut>);

    fn next(&mut self) -> Option<Self::Item> {
        let left = self.inner.pop();
        match left {
            Some(left) => {
                let fut: Option<Pin<&'a mut Fut>> = self
                    .futures
                    .futures
                    .get_pin_mut(&(left.clone(), self.right.clone()));
                Some((left, fut.unwrap()))
                // None
            }
            None => None,
        }
    }
}

impl<'a, L: Clone + Hash + Eq, R: Clone + Hash + Eq, Fut: Future> LeftIterPinMut<'a, L, R, Fut> {
    pub fn key(&self) -> &R {
        &self.right
    }
}
