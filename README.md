<p align="center"><img src="http://blog.jupo.org/static/img/curiodb-logo.png"></p>

Created by [Stephen McDonald][twitter]

CurioDB is a distributed and persistent [Redis][redis] clone, built
with [Scala][scala] and [Akka][akka]. Please note that despite the
fancy logo, this is a toy project, hence the name "Curio", and any
suitability as a drop-in replacement for Redis is purely incidental. :-)

## Installation

I've been using [sbt][sbt] to build the project, which you can install
on [OS X][sbt-osx], [Linux][sbt-linux] or [Windows][sbt-windows]. With
that done, you just need to clone this repository and run it:

```
$ git clone git://github.com/stephenmcd/curiodb.git
$ cd curiodb
$ sbt "~re-start --config=path/to/config.file"
```

You can also build a binary (executable JAR file):

```
$ sbt assembly
$ ./target/scala-2.11/curiodb-0.0.1 --config=path/to/config.file
```

Note that the `--config=path/to/config.file` argument is optional, see
the *Configuration* section below for more detail.

## Overview

Why build a Redis clone? Well, I'd been learning Scala and Akka and
wanted a nice project I could take them further with. I've used Redis
heavily in the past, and Akka gave me some really cool ideas for
implementing a clone, based on each key/value pair (or KV pair) in the
system being implemented as an [actor][actor-model]:

##### Concurrency

Since each KV pair in the system is an actor, CurioDB will happily use
all your CPU cores, so you can run 1 server using 32 cores instead of
32 servers each using 1 core (or use all 1,024 cores of your 32 server
cluster, why not). Each actor operates on its own value atomically,
so the atomic nature of Redis commands is still present, it just occurs
at the individual KV level instead of in the context of an entire
running instance of Redis.

##### Distributed by Default

Since each KV pair in the system is an actor, the interaction between
multiple KV pairs works the same way when they're located across the
network as it does when they're located on different processes on a
single machine. This negates the need for features of Redis like "hash
tagging", and allows commands that deal with multiple keys (`SUNION`,
`SINTER`, `MGET`, `MSET`, etc) to operate seamlessly across a cluster.

##### Virtual Memory

Since each KV pair in the system is an actor, the unit of disk storage
is the individual KV pair, not a single instance's entire data
set. This makes Redis' [abandoned virtual memory feature][redis-vm] a
lot more feasible. With CurioDB, an actor can simply persist its value
to disk after some criteria occurs, and shut itself down until
requested again.

##### Simple Implementation

Scala is concise, you get a lot done with very little code, but that's
just the start - CurioDB leverages Akka very heavily, taking care of
clustering, concurrency, persistence, and a whole lot more. This means
the bulk of CurioDB's code mostly deals with implementing all of the
[Redis commands][redis-commands], so far weighing in at only a paltry
1,000 lines of Scala! Currently, the majority of commands have been
fully implemented, as well as the [Redis wire protocol][redis-protocol]
itself, so [existing client libraries][redis-clients] can be used. Some
commands have been purposely omitted where they don't make sense, such
as cluster management, and things specific to Redis' storage format.

##### Pluggable Storage

Since Akka Persistence is used for storage, many strange scenarios
become available. Want to use [PostgreSQL][postgresql] or
[Cassandra][cassandra] for storage, with CurioDB as the front-end
interface for Redis commands? [This should be possible!][storage-backends]
By default, CurioDB uses Akka's built-in [LevelDB storage][leveldb-storage].

## Design

Here's a bad diagram representing one server in the cluster, and the
flow of a client sending a command:

<p align="center"><img src="http://blog.jupo.org/static/img/curiodb.png"></p>

* An outside client sends a command to the server actor
  ([Server.scala][server-source]). There's at most one per cluster node
  (which could be used to support load balancing), and at least one per
  cluster (not all nodes need to listen for outside clients).
* Upon receiving a new outside client connection, the server actor will
  create a Client Node actor ([System.scala][system-source]), it's
  responsible for the life-cycle of a single client connection, as well
  as parsing the incoming and writing the outgoing protocol, such as the
  Redis protocol for TCP clients, or JSON for HTTP clients
  ([Server.scala][server-source]).
* Key Node actors ([System.scala][system-source]) manage the key space
  for the entire system, which are distributed across the entire
  cluster using consistent hashing. A Client Node will forward the
  command to the matching Key Node for its key.
* A Key Node is then responsible for creating, removing, and
  communicating with each KV Node actor, which are the actual actors
  that store the underlying value for each key, such as a strings,
  hashes, and sorted sets, and perform the operations on them for each
  of their respective commands. ([Data.scala][data-source]).
* The KV Node then sends a response back to the originating Client
  Node, which returns it to the outside client.

Not diagrammed, but in addition to the above:

* Some commands require coordination with multiple KV Nodes, in which
  case a temporary Aggregate actor
  ([Aggregation.scala][aggregation-source]) is created by the Client
  Node, which coordinates the results for multiple commands via Key
  Nodes and KV Nodes in the same way a Client Node does.
* PubSub is implemented by adding behavior to Key Nodes and Client
  Nodes, which act as PubSub servers and clients respectively
  ([PubSub.scala][pubsub-source]).
* Lua scripting is fully supported
  ([Scripting.scala][scripting-source]) thanks to [LuaJ][luaj], and is
  implemented similarly to PubSub, where behavior is added to Key Nodes
  which store and run compiled Lua scripts (via `EVALSHA`), and Client
  Nodes which can run uncompiled scripts directly (via `EVAL`).

## Transactions

Distributed transactions are fully supported, both by way of the
`MULTI` and `EXEC` commands, and for Lua scripts with the `EVAL` and
`EVALSHA` commands. Transactions are implemented using basic
[two-phase commit (2PC)][2pc] with rollback support,
[multiversion concurrency control (MVCC)][mvcc], and configurable
[isolation levels][isolation].

##### 2PC

A Client Node acts as a transaction coordinator in 2PC parlance. It is
responsible for coordinating initial agreement with each Node that will
participate in the transaction, aggregating responses for all executed
(but uncommitted) commands, and then finally coordinating the commit
phase for each participating Node. Given the use of MVCC, performing
rollback on errors during a transaction is fully supported, and is the
default behavior. This differs however from the way Redis deals with
errors, as it [does not support transaction rollbacks][redis-rollback],
therefore in CurioDB the behavior can be configured by changing the
`curiodb.transactions.on-error` setting from `rollback` to `commit`, if
this level of compatibility with Redis is required.

##### MVCC

Each KV Node in the system stores multiple versions of its underlying
value internally, using a map that contains each transaction's version,
as well as the current committed version of the value, or "main" value.
When a transaction begins, the main value is copied into the map,
stored against its transaction ID, and from that point, all commands
received within the transaction will read and write to the transaction
version until the transaction is commited, at which point the
transaction version is copied back to the main value.

##### Isolation

Three levels of [transaction isolation][isolation] are available,
which can be configured by the `curiodb.transactions.isolation`
setting, to control how a key's value is read during a command:

* `repeatable` (default): Inside a transaction, only the transaction's
  version will be read, otherwise when outside of a transaction, the
  current committed version will be read.
* `committed`: Inside or outside of a transaction, the current
  committed version will be read.
* `uncommitted`: Inside or outside of a transaction, the most recently
  written version will be read, even if uncommitted.

Note there is no `serializable` isolation level typically found in SQL
databases, since neither Redis nor CurioDB have a notion of range
queries.

## Configuration

Here are the few configuration settings and their default values that
CurioDB implements, along with the large range of settings
[provided by Akka][akka-config] itself, which both use
[typesafe-config][typesafe-config] - consult those projects for
detailed information on configuration implementation.

```
curiodb {

  // Addresses listening for clients.
  listen = [
    "tcp://127.0.0.1:6379"    // TCP server using Redis protocol.
    "http://127.0.0.1:2600"   // HTTP server using JSON.
    "ws://127.0.0.1:6200"     // WebSocket server, also using JSON.
  ]

  // Duration settings (either time value, or "off").
  persist-after = 1 second    // Like "save" in Redis.
  sleep-after   = 10 seconds  // Virtual memory threshold.
  expire-after  = off         // Automatic key expiry.

  transactions {
    timeout   = 3 seconds     // Max time a transaction may take to run.
    isolation = repeatable    // "repeatable", "committed", or "uncommitted".
    on-error  = rollback      // "commit" or "rollback".
  }

  commands {
    timeout  = 1 second       // Max time a command may take to run.
    disabled = [SHUTDOWN]     // List of disabled commands.
    debug    = off            // Print debug info for every command run.
  }

  // Cluster nodes.
  nodes = {
    node1: "tcp://127.0.0.1:9001"
    // node2: "tcp://127.0.0.1:9002"
    // node3: "tcp://127.0.0.1:9003"
  }

  // Current cluster node (from the "nodes" keys above).
  node = node1

}
```

You can also optionally provide your own configuration file, using the
`--config=path/to/config.file` command-line argument. Your
configuration file need only define the values you wish to override.
For example, suppose you wanted to only listen over TCP, and disable
extra commands:

```
curiodb.listen = ["tcp://127.0.0.1:3333"]

curiodb.commands.disabled = [SHUTDOWN, DEL, FLUSHDB, FLUSHALL]
```

The `sleep-after` and `expire-after` settings are worth some
explanation. Each of these configure a time duration that starts each
time a command is run against a key, and elapses if no further commands
run against that key within the duration. Once the duration elapses,
an action is performed. After the `sleep-after` duration elapses for a
key, it will persist its value to disk, and shut down, essentially
going to sleep - this is how virtual memory is implemented.
`expire-after` is similar, but after its duration elapses, the key is
deleted entirely, just as if the `EXPIRE` command was used.

## HTTP/WebSocket JSON API

As alluded to in the configuration example above, CurioDB also supports
a HTTP/WebSocket JSON API, as well as the same wire protocol that Redis
implements over TCP. Commands are issued with HTTP POST requests, or
WebSocket messages, containing a JSON Object with a single `args` key,
containing an Array of arguments. Responses are returned as a JSON
Object with a single `result` key.

HTTP:

```
$ curl -X POST -d '{"args": ["SET", "foo", "bar"]}' http://127.0.0.1:2600
{"result":"OK"}

$ curl -X POST -d '{"args": ["MGET", "foo", "baz"]}' http://127.0.0.1:2600
{"result":["bar",null]}
```

WebSocket:

```
var socket = new WebSocket('ws://127.0.0.1:6200');

socket.onmessage = function(response) {
  console.log(JSON.parse(response.data));
};

socket.send(JSON.stringify({args: ["DEL", "foo"]}));
```

`SUBSCRIBE` and `PSUBSCRIBE` commands work as expected over WebSockets,
and are also fully supported by the HTTP API, by using chunked transfer
encoding to allow a single HTTP connection to receive a stream of
published messages over an extended period of time.

In the case of errors such as invalid arguments to a command, WebSocket
connections will transmit a JSON Object with a single `error` key
containing the error message, while HTTP requests will return a
response with a 400 status, contaning the error message in the response
body.

## Disadvantages compared to Redis

* I haven't measured it, but it's safe to say memory consumption is
  much poorer due to the JVM. Somewhat alleviated by the virtual memory
  feature.
* It's slower, but not by as much as you'd expect. Without any
  optimization, it's roughly about half the speed of Redis. See the
  performance section below.
* PubSub pattern matching may perform poorly. PubSub channels are
  distributed throughout the cluster using consistent hashing, which
  makes pattern matching impossible. To work around this, patterns get
  stored on every node in the cluster, and the `PSUBSCRIBE` and
  `PUNSUBSCRIBE` commands get broadcast to all of them. This needs
  rethinking!

Mainly though, Redis is an extremely mature and battle-tested project
that's been developed by many over the years, while CurioDB is a
one-man hack project worked on over a few months. As much as this
document attempts to compare them, they're really not comparable in
that light. That said, it's been tons of fun building it, and it has
some cool ideas thanks to Akka. I hope others can get something out of
it too.

## Performance

These are the results of `redis-benchmark -q` on an early 2014
MacBook Air running OS X 10.9 (the numbers are requests per second):

Benchmark      | Redis    | CurioDB  | %
---------------|----------|----------|----
`PING_INLINE`  | 57870.37 | 46296.29 | 79%
`PING_BULK`    | 55432.37 | 44326.24 | 79%
`SET`          | 50916.50 | 33233.63 | 65%
`GET`          | 53078.56 | 38580.25 | 72%
`INCR`         | 57405.28 | 33875.34 | 59%
`LPUSH`        | 45977.01 | 28082.00 | 61%
`LPOP`         | 56369.79 | 23894.86 | 42%
`SADD`         | 59101.65 | 25733.40 | 43%
`SPOP`         | 50403.23 | 33886.82 | 67%
`LRANGE_100`   | 22246.94 | 11228.38 | 50%
`LRANGE_300`   |  9984.03 |  6144.77 | 61%
`LRANGE_500`   |  6473.33 |  4442.67 | 68%
`LRANGE_600`   |  5323.40 |  3511.11 | 65%
`MSET`         | 34554.25 | 15547.26 | 44%

*Generated with the bundled [benchmark.py][benchmark-script] script.*

## Further Reading

These are some articles I published on developing CurioDB:

* [CurioDB: A Distributed and Persistent Redis Clone][intro-article]
* [Embedding Lua in Scala using Java][lua-article]
* [Distributed Transactions in Actor Systems][transactions-article]

## License

BSD.

[twitter]: http://twitter.com/stephen_mcd
[scala]: http://www.scala-lang.org/
[akka]: http://akka.io/
[sbt]: http://www.scala-sbt.org/
[sbt-osx]: http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Mac.html
[sbt-linux]: http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Linux.html
[sbt-windows]: http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Windows.html
[redis]: http://redis.io/
[actor-model]: https://en.wikipedia.org/wiki/Actor_model
[redis-vm]: http://redis.io/topics/virtual-memory
[redis-commands]: http://redis.io/commands
[redis-protocol]: http://redis.io/topics/protocol
[redis-clients]: http://redis.io/clients
[postgresql]: http://www.postgresql.org/
[cassandra]: http://cassandra.apache.org/
[storage-backends]: http://akka.io/community/#snapshot-plugins
[leveldb-storage]: http://doc.akka.io/docs/akka/snapshot/scala/persistence.html#Local_snapshot_store
[server-source]: https://github.com/stephenmcd/curiodb/blob/master/src/main/scala/Server.scala
[system-source]: https://github.com/stephenmcd/curiodb/blob/master/src/main/scala/System.scala
[data-source]: https://github.com/stephenmcd/curiodb/blob/master/src/main/scala/Data.scala
[aggregation-source]: https://github.com/stephenmcd/curiodb/blob/master/src/main/scala/Aggregation.scala
[pubsub-source]: https://github.com/stephenmcd/curiodb/blob/master/src/main/scala/PubSub.scala
[scripting-source]: https://github.com/stephenmcd/curiodb/blob/master/src/main/scala/Scripting.scala
[luaj]: http://www.luaj.org/luaj/3.0/README.html
[2pc]: https://en.wikipedia.org/wiki/Two-phase_commit_protocol
[mvcc]: https://en.wikipedia.org/wiki/Multiversion_concurrency_control
[isolation]: https://en.wikipedia.org/wiki/Isolation_(database_systems)
[redis-rollback]: http://redis.io/topics/transactions#why-redis-does-not-support-roll-backs
[akka-config]: http://doc.akka.io/docs/akka/snapshot/general/configuration.html#listing-of-the-reference-configuration
[typesafe-config]: https://github.com/typesafehub/config#standard-behavior
[benchmark-script]: https://github.com/stephenmcd/curiodb/blob/master/benchmark.py
[intro-article]: http://blog.jupo.org/2015/07/08/curiodb-a-distributed-persistent-redis-clone/
[lua-article]: http://blog.jupo.org/2015/08/08/embedding-lua-in-scala-with-java-oh-my/
[transactions-article]: http://blog.jupo.org/2016/01/28/distributed-transactions-in-actor-systems/
