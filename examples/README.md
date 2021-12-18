# Coerce Examples
The Coerce actor runtime can be used for many different types of applications. Here are just a few different examples of
how you could use Coerce to create scalable, fault-tolerant, self-healing and high performance event-driven applications.

## Sharded Chat Example
This example showcases Coerce's cluster sharding, distributed PubSub, easy integration with streams and more.
Paired with [tokio_tungstenite](https://github.com/snapview/tokio-tungstenite), the chat example creates a cluster of 
websockets and a sharded cluster of `ChatStream` actors.
Users connect via a WebSocket, join chats via communicating via a Sharded actor and receive broadcasted chat messages via
a PubSub stream scoped to each `ChatStream`.

More information can be found [here](http://todo-link).
