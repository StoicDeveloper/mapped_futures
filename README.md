# Mapped Futures

This library contains the MappedFutures struct, which allows keys to be mapped to futures. Once added, the futures can be mutated or removed if you have the key. MappedFutures is implemented using a HashMap and FuturesUnordered, so futures will only be polled after being woken, and will complete out of order.

I created this package so that I could cancel futures associated with a key, without needed to build the cancelation into the future logic itself, as well 
