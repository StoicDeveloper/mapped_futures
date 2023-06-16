# Mapped Futures

This library contains several structs that map keys to asynchronous tasks. It contains `MappedFutures`, `BiMultiMapFutures`, `MappedStreams`, and `BiMultiMapStreams`. Once added, the futures or streams can be mutated or removed if you have the key. These modules add mapping data structures to FuturesUnordered, so futures will only be polled after being woken, and will complete out of order.

You can create the mapping, insert futures, cancel them, and wait for the next completing future, which will return the future's output and its key. If the future is `Unpin` then a reference can be retrieved for mutation with `MappedFutures::get_mut()`, otherwise `MappedFutures::get_pin_mut()` must be used.

```
use crate::mapped_futures::*;
use futures::executor::block_on;
use futures::future::LocalBoxFuture;
use futures_timer::Delay;
use futures_util::StreamExt;
use std::time::Duration;

let mut futures = MappedFutures::new();
futures.insert(1, Delay::new(Duration::from_millis(50)));
futures.insert(2, Delay::new(Duration::from_millis(75)));
futures.insert(3, Delay::new(Duration::from_millis(100)));
assert_eq!(futures.cancel(&1), true);
futures.get_pin_mut(&2).unwrap().reset(Duration::from_millis(125));
assert_eq!(block_on(futures.next()).unwrap().0, 3);
assert_eq!(block_on(futures.next()).unwrap().0, 2);
assert_eq!(block_on(futures.next()), None);
```

In order to retrieve any owned futures from the mapping, the futures have to be `Unpin`, such as by enclosing them in `Box::pin()`.

```
use crate::mapped_futures::*;
use futures::executor::block_on;
use futures::future::LocalBoxFuture;
use futures_timer::Delay;
use futures_util::StreamExt;
use std::time::Duration;

futures.insert(1, Box::pin(Delay::new(Duration::from_millis(50))));
futures.insert(2, Box::pin(Delay::new(Duration::from_millis(75))));
futures.insert(3, Box::pin(Delay::new(Duration::from_millis(100))));
assert_eq!(block_on(futures.remove(&1)).unwrap().0, ());
assert_eq!(block_on(futures.replace(&2, Delay::new(Duration::from_millis(125)))).unwrap().0, ());
assert_eq!(block_on(futures.next()).unwrap().0, 3);
```

A similar interface exists for `MappedStreams` but with streams instead of futures. If your future mapping needs are more complex, you can use `BiMultiMapFutures`, which suppports one-to-many relationships between futures and two kinds of key. So each key will be associated with zero or more futures, but each (leftkey, rightkey) pair will be associated with at most one future. `BiMultiMapStreams` does the same for streams.
