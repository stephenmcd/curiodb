Created by [Stephen McDonald](http://twitter.com/stephen_mcd>)

CurioDB is a distributed and persistent [Redis](http://redis.io/) clone,
built with [Scala](http://scala-lang.org/) and [Akka](http://akka.io/).
Please note that this is a toy project, hence the name "Curio", and any
suitability as a drop-in replacement for Redis is purely incidental. :-)

## Installation

I've been using [SBT](http://www.scala-sbt.org/) to build the project,
which you can install on
[OSX](http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Mac.html),
[Linux](http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Linux.html) or
[Windows](http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Windows.html).
With that done, you just need to clone this repository and run it:

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

Why build a Redis clone? Well, I'd learnt the basics of Scala and Akka
and wanted a nice project I could take them further with. I've used Redis
heavily in the past, and Akka gave me some really cool ideas for
implementing a clone, based on each K/V pair in the system being
implemented as an [actor](https://en.wikipedia.org/wiki/Actor_model):

##### Concurrency

Since each K/V pair in the system is an actor, CurioDB will happily use
all your CPU cores, so you can run 1 server using 32 cores instead of
32 servers each using 1 core (or use all 1,024 cores of your 32 server
cluster, why not). Each actor operates on its own value atomically,
so the atomic nature of Redis commands is still present, it just occurs
at the individual K/V level instead of in the context of an entire
running instance of Redis.

##### Distributed by Default

Since each K/V pair in the system is an actor, the interaction between
multiple K/V pairs works the same way when they're located across the
network as it does when they're located on different processes on the
same machine. This negates the need for features of Redis like "hash
tagging", and allows commands that deal with multiple keys (`SUNION`,
`SINTER`, `MGET`, `MSET`, etc) to operate seamlessly across a cluster.

##### Virtual Memory

Since each K/V pair in the system is an actor, the atomic storage
unit is the individual K/V pair, not a single instance's entire data
set. This makes Redis' abandoned virtual memory feature a lot more
feasible. With CurioDB, an actor can simply persist its value to
disk after some criteria occurs, and shut itself down until requested
again.

##### Simple Implementation

Scala is concise, you get a lot done with very little code, but that's
just the start - CurioDB leverages Akka very heavily. Akka takes
care of clustering, concurrency, persistence, and a whole lot more,
so the bulk of CurioDB's code mostly deals with implementing all of the
[Redis commands](http://redis.io/commands), weighing in at a tiny 1,000
lines of Scala! Currently, the majority of commands have been fully
implemented, as well as the Redis wire protocol itself, so existing
client libraries can be used. Some commands have been purposely omitted
where they don't make sense, such as cluster management, and things
specific to Redis' storage format.

##### Pluggable Storage

Since Akka Persistence is used for storage, many strange scenarios
become available. Want to use [PostgreSQL or Cassandra](http://akka.io/community/#snapshot-plugins)
for storage, with CurioDB as the front-end interface? This should be
possible! By default, CurioDB uses Akka's built-in [LevelDB storage](http://doc.akka.io/docs/akka/snapshot/scala/persistence.html#Local_snapshot_store).

## Design

Here's a bad diagram representing one server in the cluster, and the
flow of a client sending a command:

![Image](http://i.imgur.com/9KkP9uZ.png)

* An outside client sends a command to the server actor (Server.scala).
  There's at most one per cluster node (which could be used to support
  load balancing), and at least one per cluster (not all nodes need to
  listen for outside clients).
* Upon receiving a new outside client connection, the server actor will
  create a Client Node actor (System.scala), it's responsible for the
  life-cycle of a single client connection, as well as parsing the
  incoming and writing the outgoing Redis wire protocol.
* Key Node actors (System.scala) manage the key space for the entire
  system, which are distributed across the entire cluster using consistent
  hashing. A Client Node will forward the command to the matching KeyNode
  for its key.
* A Key Node is then responsible for creating, removing, and communicating
  with each KV actor, which are the actual Nodes for each key and value,
  such as a string, hash, sorted set, etc (Data.scala).
* The KV Node then sends a response back to the originating Client Node,
  which returns it to the outside client.
* Some commands require coordination with multiple KV Nodes, in which
  case a temporary Aggregate actor (Aggregation.scala) is created by
  the Client Node, which coordinates the results for multiple commands
  via Key Nodes and KV Nodes in the same way a Client Node does.
* PubSub is implemented by adding behavior to Key Nodes and Client Nodes,
  which act as PubSub servers and clients respectivly (PubSub.scala).

## Configuration

Here are the few default configuration settings CurioDB implements,
along with the large range of settings [provided by Akka](http://doc.akka.io/docs/akka/snapshot/general/configuration.html#listing-of-the-reference-configuration)
itself, which both use [typesafe-config](https://github.com/typesafehub/config#standard-behavior) -
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
* No Lua scripting. :-(

Mainly though, Redis is an extremely mature and battle-tested project
that's been developed by many over the years, while CurioDB is a one-man
hack project worked on over a few months. As much as this document
attempts to compare them, they're really not comparable in that light.
That said, it's been tons of fun building it, and it has some cool ideas
thanks to Akka. I hope others can get something out of it too.

## Performance

These are the results of `redis-benchmark -q` on an early 2014
MacBook Air running OSX 10.9 (the numbers are requests per second):

Benchmark    | Redis    | CurioDB  | %
-------------|----------|----------|----
PING_INLINE  | 68965.52 | 51546.39 | 74%
PING_BULK    | 70921.98 | 48543.69 | 68%
SET          | 67704.80 | 44843.05 | 66%
GET          | 70621.47 | 44662.79 | 63%
INCR         | 71581.96 | 41288.19 | 58%
LPUSH        | 70472.16 | 40436.71 | 57%
LPOP         | 69589.42 | 9694.62  | 14%
SADD         | 69686.41 | 41305.25 | 59%
SPOP         | 70771.41 | 41271.15 | 58%
LPUSH        | 70372.98 | 40225.26 | 57%
LRANGE_100   | 24319.07 | 11795.24 | 49%
LRANGE_300   | 9894.13  | 6295.25  | 64%
LRANGE_500   | 7449.90  | 4763.27  | 64%
LRANGE_600   | 5765.68  | 3976.46  | 69%
MSET         | 46728.97 | 14607.07 | 31%

## License

BSD.
