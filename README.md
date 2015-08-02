Created by [Stephen McDonald][twitter]

CurioDB is a distributed and persistent [Redis][redis] clone, built
with [Scala][scala] and [Akka][akka]. Please note that this is a toy
project, hence the name "Curio", and any suitability as a drop-in
replacement for Redis is purely incidental. :-)

## Installation

I've been using [SBT][sbt] to build the project, which you can install
on [OSX][sbt-osx], [Linux][sbt-linux] or [Windows][sbt-windows]. With
that done, you just need to clone this repository and run it:

```
$ git clone git://github.com/stephenmcd/curiodb.git
$ cd curiodb
$ sbt run
```

You can also build a binary (executable JAR file):

```
$ sbt assembly
$ ./target/scala-2.11/curiodb-0.0.1
```

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

![Image](http://i.imgur.com/9KkP9uZ.png)

* An outside client sends a command to the server actor
  ([Server.scala][server-source]). There's at most one per cluster node
  (which could be used to support load balancing), and at least one per
  cluster (not all nodes need to listen for outside clients).
* Upon receiving a new outside client connection, the server actor will
  create a Client Node actor ([System.scala][system-source]), it's
  responsible for the life-cycle of a single client connection, as well
  as parsing the incoming and writing the outgoing Redis wire protocol.
* Key Node actors ([System.scala][system-source]) manage the key space
  for the entire system, which are distributed across the entire
  cluster using consistent hashing. A Client Node will forward the
  command to the matching Key Node for its key.
* A Key Node is then responsible for creating, removing, and
  communicating with each KV actor, which are the actual Nodes for each
  key and value, such as a string, hash, sorted set, etc
  ([Data.scala][data-source]).
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

## Configuration

Here are the few default configuration settings CurioDB implements,
along with the large range of settings [provided by Akka][akka-config]
itself, which both use [typesafe-config][typesafe-config] -
consult those projects for more info.

```
curiodb {

  listen = "tcp://127.0.0.1:6379"  // Address listening for clients
  persist-after = 1000             // Like "save" in Redis
  sleep-after = 1000               // Virtual memory millisecond threshold
  node = node1                     // Current cluster node (from the
                                   // nodes keys below)
  // Cluster nodes.
  nodes = {
    node1: "tcp://127.0.0.1:9001"
    // node2: "tcp://127.0.0.1:9002"
    // node3: "tcp://127.0.0.1:9003"
  }

}
```

## Disadvantages over Redis

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
* No transaction support.
* Lua scripts are not atomic.

Mainly though, Redis is an extremely mature and battle-tested project
that's been developed by many over the years, while CurioDB is a
one-man hack project worked on over a few months. As much as this
document attempts to compare them, they're really not comparable in
that light. That said, it's been tons of fun building it, and it has
some cool ideas thanks to Akka. I hope others can get something out of
it too.

## Performance

These are the results of `redis-benchmark -q` on an early 2014
MacBook Air running OSX 10.9 (the numbers are requests per second):

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
[akka-config]: http://doc.akka.io/docs/akka/snapshot/general/configuration.html#listing-of-the-reference-configuration
[typesafe-config]: https://github.com/typesafehub/config#standard-behavior
[benchmark-script]: https://github.com/stephenmcd/curiodb/blob/master/benchmark.py
