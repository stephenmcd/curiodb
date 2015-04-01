package curiodb

import akka.actor._
import akka.cluster.Cluster
import akka.dispatch.ControlMessage
import akka.event.LoggingReceive
import akka.io.{IO, Tcp}
import akka.persistence._
import akka.routing.{Broadcast, FromConfig}
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import akka.util.ByteString
import com.dictiography.collections.{IndexedTreeMap, IndexedTreeSet}
import com.typesafe.config.ConfigFactory
import java.net.{InetSocketAddress, URI}
import net.agkn.hll.HLL
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.math.{min, max}
import scala.util.{Success, Failure, Random, Try}

case class Spec(
  args: Any = 0,
  default: (Seq[String] => Any) = (_ => ()),
  keyed: Boolean = true,
  writes: Boolean = false,
  overwrites: Boolean = false)

case class Error(message: String = "syntax error", prefix: String = "ERR")

case class SimpleReply(message: String = "OK")

object Commands {

  val many = Int.MaxValue - 1
  val pairs = 2 to many by 2

  val error  = (_: Seq[String]) => Error("no such key")
  val nil    = (_: Seq[String]) => null
  val ok     = (_: Seq[String]) => SimpleReply()
  val zero   = (_: Seq[String]) => 0
  val neg1   = (_: Seq[String]) => -1
  val neg2   = (_: Seq[String]) => -2
  val nils   = (x: Seq[String]) => x.map(_ => nil)
  val zeros  = (x: Seq[String]) => x.map(_ => zero)
  val seq    = (_: Seq[String]) => Seq()
  val string = (_: Seq[String]) => ""
  val scan   = (_: Seq[String]) => Seq("0", "")

  val specs = Map(

    "string" -> Map(
      "append"       -> Spec(args = 1, writes = true),
      "decr"         -> Spec(writes = true),
      "decrby"       -> Spec(args = 1, writes = true),
      "get"          -> Spec(default = nil),
      "getrange"     -> Spec(args = 2, default = string),
      "getset"       -> Spec(args = 1, writes = true),
      "incr"         -> Spec(writes = true),
      "incrby"       -> Spec(args = 1, writes = true),
      "incrbyfloat"  -> Spec(args = 1, writes = true),
      "psetex"       -> Spec(args = 2, overwrites = true),
      "set"          -> Spec(args = 1 to 4, overwrites = true),
      "setex"        -> Spec(args = 2, overwrites = true),
      "setnx"        -> Spec(args = 1, default = zero, writes = true),
      "setrange"     -> Spec(args = 2, writes = true),
      "strlen"       -> Spec(default = zero)
    ),

    "hash" -> Map(
      "_rename"      -> Spec(args = 1),
      "_hstore"      -> Spec(args = pairs, overwrites = true),
      "hdel"         -> Spec(args = 1 to many, default = zeros, writes = true),
      "hexists"      -> Spec(args = 1, default = zero),
      "hget"         -> Spec(args = 1, default = nil),
      "hgetall"      -> Spec(default = seq),
      "hincrby"      -> Spec(args = 2, writes = true),
      "hincrbyfloat" -> Spec(args = 2, writes = true),
      "hkeys"        -> Spec(default = seq),
      "hlen"         -> Spec(default = zero),
      "hmget"        -> Spec(args = 1 to many, default = nils),
      "hmset"        -> Spec(args = pairs, writes = true),
      "hscan"        -> Spec(args = 1 to 3, default = scan),
      "hset"         -> Spec(args = 2, writes = true),
      "hsetnx"       -> Spec(args = 2, writes = true),
      "hvals"        -> Spec(default = seq)
    ),

    "list" -> Map(
      "_rename"      -> Spec(args = 1),
      "_lstore"      -> Spec(args = 0 to many, overwrites = true),
      "_sort"        -> Spec(args = 0 to many),
      "blpop"        -> Spec(args = 1 to many, default = nil, writes = true),
      "brpop"        -> Spec(args = 1 to many, default = nil, writes = true),
      "brpoplpush"   -> Spec(args = 2, default = nil, writes = true),
      "lindex"       -> Spec(args = 1, default = nil),
      "linsert"      -> Spec(args = 3, default = zero, writes = true),
      "llen"         -> Spec(default = zero),
      "lpop"         -> Spec(default = nil, writes = true),
      "lpush"        -> Spec(args = 1 to many, writes = true),
      "lpushx"       -> Spec(args = 1, default = zero, writes = true),
      "lrange"       -> Spec(args = 2, default = seq),
      "lrem"         -> Spec(args = 2, default = zero),
      "lset"         -> Spec(args = 2, default = error, writes = true),
      "ltrim"        -> Spec(args = 2, default = ok),
      "rpop"         -> Spec(default = nil, writes = true),
      "rpoplpush"    -> Spec(args = 1, default = nil, writes = true),
      "rpush"        -> Spec(args = 1 to many, writes = true),
      "rpushx"       -> Spec(args = 1, default = zero, writes = true)
    ),

    "set" -> Map(
      "_rename"      -> Spec(args = 1),
      "_sstore"      -> Spec(args = 0 to many, overwrites = true),
      "_sort"        -> Spec(args = 0 to many),
      "sadd"         -> Spec(args = 1 to many, writes = true),
      "scard"        -> Spec(default = zero),
      "sismember"    -> Spec(args = 1, default = zero),
      "smembers"     -> Spec(default = seq),
      "smove"        -> Spec(args = 2, default = error, writes = true),
      "spop"         -> Spec(default = nil, writes = true),
      "srandmember"  -> Spec(args = 0 to 1, default = nil),
      "srem"         -> Spec(args = 1 to many, default = zero, writes = true),
      "sscan"        -> Spec(args = 1 to 3, default = scan)
    ),

    "sortedset" -> Map(
      "_rename"          -> Spec(args = 1),
      "_zstore"          -> Spec(args = 0 to many, overwrites = true),
      "_zget"            -> Spec(default = seq),
      "_sort"            -> Spec(args = 0 to many),
      "zadd"             -> Spec(args = pairs, writes = true),
      "zcard"            -> Spec(default = zero),
      "zcount"           -> Spec(args = 2, default = zero),
      "zincrby"          -> Spec(args = 2, writes = true),
      "zlexcount"        -> Spec(args = 2, default = zero),
      "zrange"           -> Spec(args = 2 to 3, default = seq),
      "zrangebylex"      -> Spec(args = 2 to 5, default = seq),
      "zrangebyscore"    -> Spec(args = 2 to 6, default = seq),
      "zrank"            -> Spec(args = 1, default = nil),
      "zrem"             -> Spec(args = 1 to many, default = zero, writes = true),
      "zremrangebylex"   -> Spec(args = 2, writes = true),
      "zremrangebyrank"  -> Spec(args = 2, writes = true),
      "zremrangebyscore" -> Spec(args = 2, writes = true),
      "zrevrange"        -> Spec(args = 2 to 3, default = seq),
      "zrevrangebylex"   -> Spec(args = 2 to 5, default = seq),
      "zrevrangebyscore" -> Spec(args = 2 to 6, default = seq),
      "zrevrank"         -> Spec(args = 1, default = nil),
      "zscan"            -> Spec(args = 1 to 3, default = scan),
      "zscore"           -> Spec(args = 1, default = nil)
    ),

    "bitmap" -> Map(
      "_rename"      -> Spec(args = 1),
      "_bstore"      -> Spec(args = 0 to many, overwrites = true),
      "_bget"        -> Spec(default = seq),
      "setbit"       -> Spec(args = 2, writes = true),
      "bitcount"     -> Spec(args = 0 to 2, default = zero),
      "bitpos"       -> Spec(args = 1 to 3, default = (args: Seq[String]) => -args(0).toInt),
      "getbit"       -> Spec(args = 1, default = zero)
    ),

    "hyperloglog" -> Map(
      "_rename"     -> Spec(args = 1),
      "_pfstore"    -> Spec(args = 0 to many, overwrites = true),
      "_pfget"      -> Spec(default = seq),
      "_pfcount"    -> Spec(default = zero),
      "pfadd"       -> Spec(args = 1 to many, writes = true)
    ),

    "keys" -> Map(
      "_del"         -> Spec(default = nil, writes = true),
      "_keys"        -> Spec(args = 0 to 1, keyed = false),
      "_randomkey"   -> Spec(keyed = false),
      "_flushdb"     -> Spec(keyed = false),
      "_flushall"    -> Spec(keyed = false),
      "exists"       -> Spec(args = 1 to many, keyed = false),
      "expire"       -> Spec(args = 1, default = zero),
      "expireat"     -> Spec(args = 1, default = zero),
      "persist"      -> Spec(default = zero),
      "pexpire"      -> Spec(args = 1, default = zero),
      "pexpireat"    -> Spec(args = 1, default = zero),
      "pttl"         -> Spec(default = neg2),
      "rename"       -> Spec(args = 1, default = error),
      "renamenx"     -> Spec(args = 1, default = error),
      "sort"         -> Spec(args = 0 to many, default = seq),
      "ttl"          -> Spec(default = neg2),
      "type"         -> Spec()
    ),

    "client" -> Map(
      "bitop"        -> Spec(args = 3 to many, keyed = false),
      "dbsize"       -> Spec(keyed = false),
      "del"          -> Spec(args = 1 to many, keyed = false),
      "echo"         -> Spec(args = 1, keyed = false),
      "flushdb"      -> Spec(keyed = false),
      "flushall"     -> Spec(keyed = false),
      "keys"         -> Spec(args = 1, keyed = false),
      "mget"         -> Spec(args = 1 to many, keyed = false),
      "mset"         -> Spec(args = pairs, keyed = false),
      "msetnx"       -> Spec(args = pairs, keyed = false),
      "ping"         -> Spec(keyed = false),
      "quit"         -> Spec(keyed = false),
      "randomkey"    -> Spec(keyed = false),
      "pfcount"      -> Spec(args = 1 to many, keyed = false),
      "pfmerge"      -> Spec(args = 2 to many),
      "scan"         -> Spec(args = 1 to 3, keyed = false),
      "shutdown"     -> Spec(args = 0 to 1, keyed = false),
      "sdiff"        -> Spec(args = 0 to many, keyed = false),
      "sdiffstore"   -> Spec(args = 1 to many),
      "select"       -> Spec(args = 1, keyed = false),
      "sinter"       -> Spec(args = 0 to many, keyed = false),
      "sinterstore"  -> Spec(args = 1 to many),
      "sunion"       -> Spec(args = 0 to many, keyed = false),
      "sunionstore"  -> Spec(args = 1 to many),
      "time"         -> Spec(keyed = false),
      "zinterstore"  -> Spec(args = 2 to many),
      "zunionstore"  -> Spec(args = 2 to many)
    )

  )

  def get(command: String) = specs.find(_._2.contains(command)).getOrElse(("", Map[String, Spec]()))

  def default(command: String, args: Seq[String]) = get(command)._2(command).default(args)

  def keyed(command: String): Boolean = get(command)._2(command).keyed

  def writes(command: String): Boolean = get(command)._2(command).writes || overwrites(command)

  def overwrites(command: String): Boolean = get(command)._2(command).overwrites

  def nodeType(command: String) = get(command)._1

  def argsInRange(command: String, args: Seq[String]) = get(command)._2(command).args match {
    case fixed: Int => args.size == fixed
    case range: Range => range.contains(args.size)
  }

}

case class Payload(input: Seq[Any] = Seq(), db: String = "0", destination: Option[ActorRef] = None) {
  val command = if (input.size > 0) input(0).toString.toLowerCase else ""
  val nodeType = if (command != "") Commands.nodeType(command) else ""
  val key = if (nodeType != "" && input.size > 1 && Commands.keyed(command)) input(1).toString else ""
  val args = input.drop(if (key == "") 1 else 2).map(_.toString)
  lazy val argsPaired = (0 to args.size - 2 by 2).map {i => (args(i), args(i + 1))}
  lazy val argsUpper = args.map(_.toUpperCase)
}

case class Unrouted(payload: Payload) extends ConsistentHashable {
  override def consistentHashKey: Any = payload.key
}

case class Response(key: String, value: Any)

trait PayloadProcessing extends Actor {

  var payload = Payload()

  def args = payload.args

  def argsPaired = payload.argsPaired

  def argsUpper = payload.argsUpper

  def route(
      input: Seq[Any] = Seq(),
      destination: Option[ActorRef] = None,
      clientPayload: Option[Payload] = None) =

    context.system.actorSelection("/user/keys") ! Unrouted(clientPayload match {
      case Some(payload) => payload
      case None => Payload(input, payload.db, destination)
    })

  def respond(response: Any) = if (response != ()) payload.destination.foreach {destination =>
    destination ! Response(payload.key, response)
  }

  def randomItem(iterable: Iterable[String]) = {
    if (iterable.isEmpty) "" else iterable.toSeq(Random.nextInt(iterable.size))
  }

  def pattern(values: Iterable[String], pattern: String) = {
    val regex = ("^" + pattern.map {
      case '.'|'('|')'|'+'|'|'|'^'|'$'|'@'|'%'|'\\' => "\\" + _
      case '*' => ".*"
      case '?' => "."
      case c => c
    }.mkString("") + "$").r
    values.filter(regex.pattern.matcher(_).matches)
  }

  def scan(values: Iterable[String]) = {
    val count = if (args.size >= 3) args(2).toInt else 10
    val start = if (args.size >= 1) args(0).toInt else 0
    val end = start + count
    val filtered = if (args.size >= 2) pattern(values, args(1)) else values
    val next = if (end < filtered.size) end else 0
    Seq(next.toString) ++ filtered.slice(start, end)
  }

  def bounds(from: Int, to: Int, size: Int) =
    (if (from < 0) size + from else from, if (to < 0) size + to else to)

  def slice[T](value: Seq[T]): Seq[T] = {
    val (from, to) = bounds(args(0).toInt, args(1).toInt, value.size)
    value.slice(from, to + 1)
  }

}

case class Persist extends ControlMessage

case class Delete extends ControlMessage

abstract class Node[T] extends PersistentActor with PayloadProcessing with ActorLogging {

  var value: T
  var lastSnapshot: Option[SnapshotMetadata] = None
  var persisting: Boolean = false
  val persistAfter = context.system.settings.config.getInt("curiodb.persist-after")

  type Run = PartialFunction[String, Any]

  def run: Run

  def persistenceId = self.path.name

  def save = {
    if (persistAfter == 0) saveSnapshot(value)
    else if (persistAfter > 0 && !persisting) {
      persisting = true
      context.system.scheduler.scheduleOnce(persistAfter milliseconds) {
        self ! Persist
      }
    }
  }

  def deleteOldSnapshots(stopping: Boolean = false) =
    if (persistAfter >= 0)
      lastSnapshot.foreach {meta =>
        val criteria = if (stopping) SnapshotSelectionCriteria()
        else SnapshotSelectionCriteria(meta.sequenceNr, meta.timestamp - 1)
        deleteSnapshots(criteria)
      }

  override def receiveRecover: Receive = {
    case SnapshotOffer(meta, snapshot) =>
      lastSnapshot = Some(meta)
      value = snapshot.asInstanceOf[T]
  }

  def receiveCommand: Receive = {
    case SaveSnapshotSuccess(meta) => lastSnapshot = Some(meta); deleteOldSnapshots()
    case SaveSnapshotFailure(_, e) => log.error(e, "Snapshot write failed")
    case Persist => persisting = false; saveSnapshot(value)
    case Delete => deleteOldSnapshots(stopping = true); context stop self
    case p: Payload =>
      payload = p
      respond(Try(run(payload.command)) match {
        case Success(response) => if (Commands.writes(payload.command)) save; response
        case Failure(e) => log.error(e, s"Error running: $payload"); Error
      })
  }

  override def receive = LoggingReceive(super.receive)

  def rename(fromValue: Any, toCommand: String) = if (payload.key != args(0)) {
    route(Seq("_del", payload.key))
    route(Seq(toCommand, args(0)) ++ (fromValue match {
      case x: Iterable[Any] => x
      case x => Seq(x)
    }))
  }

  def sort(values: Iterable[String]): Any = {
    // TODO: BY/GET support.
    var sorted = if (argsUpper.contains("ALPHA")) values.toSeq.sorted else values.toSeq.sortBy(_.toFloat)
    if (argsUpper.contains("DESC")) sorted = sorted.reverse
    val limit = argsUpper.indexOf("LIMIT")
    if (limit > -1) sorted = sorted.slice(args(limit + 1).toInt, args(limit + 2).toInt)
    val store = argsUpper.indexOf("STORE")
    if (store > -1) {route(Seq("_lstore", args(store + 1)) ++ sorted); sorted.size}
    else sorted
  }

}

class StringNode extends Node[String] {

  var value = ""

  def valueOrZero = if (value == "") "0" else value

  def expire(command: String) = route(Seq(command, payload.key, args(1)))

  def run = {
    case "_rename"     => rename(value, "set")
    case "get"         => value
    case "set"         => value = args(0); SimpleReply()
    case "setnx"       => run("set"); true
    case "getset"      => val x = value; value = args(0); x
    case "append"      => value += args(0); value
    case "getrange"    => slice(value).mkString
    case "setrange"    => value.patch(args(0).toInt, args(1), 1)
    case "strlen"      => value.size
    case "incr"        => value = (valueOrZero.toInt + 1).toString; value.toInt
    case "incrby"      => value = (valueOrZero.toInt + args(0).toInt).toString; value.toInt
    case "incrbyfloat" => value = (valueOrZero.toFloat + args(0).toFloat).toString; value
    case "decr"        => value = (valueOrZero.toInt - 1).toString; value.toInt
    case "decrby"      => value = (valueOrZero.toInt - args(0).toInt).toString; value.toInt
    case "setex"       => val x = run("set"); expire("expire"); x
    case "psetex"      => val x = run("set"); expire("pexpire"); x
  }

}

class BitmapNode extends Node[mutable.BitSet] {

  var value = mutable.BitSet()

  def last = value.lastOption.getOrElse(0)

  def run = {
    case "_rename"     => rename(value, "_bstore")
    case "_bstore"     => value.clear; value ++= args.map(_.toInt); last / 8 + (if (value.isEmpty) 0 else 1)
    case "_bget"       => value
    case "bitcount"    => value.size
    case "getbit"      => value(args(0).toInt)
    case "setbit"      => val x = run("getbit"); value(args(0).toInt) = args(1) == "1"; x
    case "bitpos"      =>
      var x = value
      if (args.size > 1) {
        val (from, to) = bounds(args(1).toInt, if (args.size == 3) args(2).toInt else last, last + 1)
        x = x.range(from, to + 1)
      }
      if (args(0) == "1") x.headOption.getOrElse(-1)
      else (0 to x.lastOption.getOrElse(-1)).collectFirst({
        case i: Int if !x.contains(i) => i
      }).getOrElse(if (args.size > 1 && value.size > 1) -1 else 0)
  }

}

class HyperLogLogNode extends Node[HLL] {

  var value = new HLL(
    context.system.settings.config.getInt("curiodb.hyperloglog.register-log"),
    context.system.settings.config.getInt("curiodb.hyperloglog.register-width")
  )

  def run = {
    case "_rename"  => rename(value.toBytes.map(_.toString), "_pfstore")
    case "_pfcount" => value.cardinality.toInt
    case "_pfstore" => value.clear(); value = HLL.fromBytes(args.map(_.toByte).toArray); SimpleReply()
    case "_pfget"   => value
    case "pfadd"    =>
      val x = value.cardinality
      args.foreach {x => value.addRaw(x.hashCode.toLong)}
      if (x == value.cardinality) 0 else 1
  }

}

class HashNode extends Node[mutable.Map[String, String]] {

  var value = mutable.Map[String, String]()

  def set(arg: Any) = {val x = arg.toString; value(args(0)) = x; x}

  override def run = {
    case "_rename"      => rename(run("hgetall"), "_hstore")
    case "_hstore"      => value.clear; run("hmset")
    case "hkeys"        => value.keys
    case "hexists"      => value.contains(args(0))
    case "hscan"        => scan(value.keys)
    case "hget"         => value.getOrElse(args(0), null)
    case "hsetnx"       => if (!value.contains(args(0))) run("hset") else false
    case "hgetall"      => value.flatMap(x => Seq(x._1, x._2))
    case "hvals"        => value.values
    case "hdel"         => val x = run("hexists"); value -= args(0); x
    case "hlen"         => value.size
    case "hmget"        => args.map(value.get(_))
    case "hmset"        => argsPaired.foreach {args => value(args._1) = args._2}; SimpleReply()
    case "hincrby"      => set(value.getOrElse(args(0), "0").toInt + args(1).toInt).toInt
    case "hincrbyfloat" => set(value.getOrElse(args(0), "0").toFloat + args(1).toFloat)
    case "hset"         => val x = !value.contains(args(0)); set(args(1)); x
  }

}

class ListNode extends Node[mutable.ArrayBuffer[String]] {

  var value = mutable.ArrayBuffer[String]()
  var blocked = mutable.LinkedHashSet[Payload]()

  def block: Any = {
    if (value.isEmpty) {
      blocked += payload
      context.system.scheduler.scheduleOnce(args.last.toInt seconds) {
        blocked -= payload
        respond(null)
      }; ()
    } else run(payload.command.tail)
  }

  def unblock(result: Any) = {
    while (value.size > 0 && blocked.size > 0) {
      payload = blocked.head
      blocked -= payload
      respond(run(payload.command.tail))
    }
    result
  }

  def run = ({
    case "_rename"    => rename(value, "_lstore")
    case "_lstore"    => value.clear; run("rpush")
    case "_sort"      => sort(value)
    case "lpush"      => value ++= args.reverse; run("llen")
    case "rpush"      => value ++= args; run("llen")
    case "lpushx"     => run("lpush")
    case "rpushx"     => run("rpush")
    case "lpop"       => val x = value(0); value -= x; x
    case "rpop"       => val x = value.last; value.reduceToSize(value.size - 1); x
    case "lset"       => value(args(0).toInt) = args(1); SimpleReply()
    case "lindex"     => val x = args(0).toInt; if (x >= 0 && x < value.size) value(x) else null
    case "lrem"       => value.remove(args(0).toInt)
    case "lrange"     => slice(value)
    case "ltrim"      => value = slice(value).asInstanceOf[mutable.ArrayBuffer[String]]; SimpleReply()
    case "llen"       => value.size
    case "blpop"      => block
    case "brpop"      => block
    case "brpoplpush" => block
    case "rpoplpush"  => val x = run("rpop"); route("lpush" +: args :+ x.toString); x
    case "linsert"    =>
      val i = value.indexOf(args(1)) + (if (args(0) == "AFTER") 1 else 0)
      if (i >= 0) {value.insert(i, args(2)); respond(run("llen"))} else -1
  }: Run) andThen unblock

}

class SetNode extends Node[mutable.Set[String]] {

  var value = mutable.Set[String]()

  def run = {
    case "_rename"     => rename(value, "_sstore")
    case "_sstore"     => value.clear; run("sadd")
    case "_sort"       => sort(value)
    case "sadd"        => val x = (args.toSet &~ value).size; value ++= args; x
    case "srem"        => val x = (args.toSet & value).size; value --= args; x
    case "scard"       => value.size
    case "sismember"   => value.contains(args(0))
    case "smembers"    => value
    case "srandmember" => randomItem(value)
    case "spop"        => val x = run("srandmember"); value -= x.toString; x
    case "sscan"       => scan(value)
    case "smove"       => val x = value.remove(args(1)); if (x) {route("sadd" +: args)}; x
  }

}

case class SortedSetEntry(score: Int, key: String = "")(implicit ordering: Ordering[(Int, String)])
    extends Ordered[SortedSetEntry] {
  def compare(that: SortedSetEntry) = ordering.compare((this.score, this.key), (that.score, that.key))
}

class SortedSetNode extends Node[(IndexedTreeMap[String, Int], IndexedTreeSet[SortedSetEntry])] {

  var value = (new IndexedTreeMap[String, Int](), new IndexedTreeSet[SortedSetEntry]())

  def keys = value._1

  def scores = value._2

  def add(score: Int, key: String) = {
    val exists = remove(key)
    keys.put(key, score); scores.add(SortedSetEntry(score, key))
    !exists
  }

  def remove(key: String) = {
    val exists = keys.containsKey(key)
    if (exists) {scores.remove(SortedSetEntry(keys.get(key), key)); keys.remove(key)}
    exists
  }

  def increment(key: String, by: Int) = {
    val score = (if (keys.containsKey(key)) keys.get(key) else 0) + by
    remove(key); add(score, key)
    score
  }

  def rank(key: String, reverse: Boolean = false) = {
    val index = scores.entryIndex(SortedSetEntry(keys.get(key), key))
    if (reverse) keys.size - index else index
  }

  def range(from: SortedSetEntry, to: SortedSetEntry, reverse: Boolean): Seq[String] = {
    if (from.score > to.score) return Seq()
    var result = scores.subSet(from, true, to, true).toSeq
    result = limit[SortedSetEntry](if (reverse) result.reverse else result)
    if (argsUpper.contains("WITHSCORES")) result.flatMap(x => Seq(x.key, x.score.toString))
    else result.map(_.key)
  }

  def rangeByIndex(from: String, to: String, reverse: Boolean = false) = {
    var (fromIndex, toIndex) = bounds(from.toInt, to.toInt, keys.size)
    if (reverse) {
      fromIndex = keys.size - fromIndex - 1
      toIndex = keys.size - toIndex - 1
    }
    range(scores.exact(fromIndex), scores.exact(toIndex), reverse)
  }

  def rangeByScore(from: String, to: String, reverse: Boolean = false) = {
    def parse(arg: String, dir: Int) = arg match {
      case "-inf" => if (scores.isEmpty) 0 else scores.first().score
      case "+inf" => if (scores.isEmpty) 0 else scores.last().score
      case arg if arg.startsWith("(") => arg.toInt + dir
      case _ => arg.toInt
    }
    range(SortedSetEntry(parse(from, 1)), SortedSetEntry(parse(to, -1) + 1), reverse)
  }

  def rangeByKey(from: String, to: String, reverse: Boolean = false): Seq[String] = {
    def parse(arg: String) = arg match {
      case "-" => ""
      case "+" => if (keys.size == 0) "" else keys.lastKey() + "x"
      case arg if "[(".indexOf(arg.head) > -1 => arg.tail
    }
    val (fromKey, toKey) = (parse(from), parse(to))
    if (fromKey > toKey) return Seq()
    val result = keys.subMap(fromKey, from.head == '[', toKey, to.head == '[').toSeq
    limit[(String, Int)](if (reverse) result.reverse else result).map(_._1)
  }

  def limit[T](values: Seq[T]) = {
    val i = argsUpper.indexOf("LIMIT")
    if (i > 1) values.slice(args(i + 1).toInt, args(i + 1).toInt + args(i + 2).toInt)
    else values
  }

  def run = {
    case "_rename"          => rename(scores.flatMap(x => Seq(x.score, x.key)), "_zstore")
    case "_zstore"          => keys.clear; scores.clear; run("zadd")
    case "_zget"            => keys
    case "_sort"            => sort(keys.keys)
    case "zadd"             => argsPaired.map(arg => add(arg._1.toInt, arg._2)).filter(x => x).size
    case "zcard"            => keys.size
    case "zcount"           => rangeByScore(args(0), args(1)).size
    case "zincrby"          => increment(args(0), args(1).toInt)
    case "zlexcount"        => rangeByKey(args(0), args(1)).size
    case "zrange"           => rangeByIndex(args(0), args(1))
    case "zrangebylex"      => rangeByKey(args(0), args(1))
    case "zrangebyscore"    => rangeByScore(args(0), args(1))
    case "zrank"            => rank(args(0))
    case "zrem"             => remove(args(0))
    case "zremrangebylex"   => rangeByKey(args(0), args(1)).map(remove).filter(x => x).size
    case "zremrangebyrank"  => rangeByIndex(args(0), args(1)).map(remove).filter(x => x).size
    case "zremrangebyscore" => rangeByScore(args(0), args(1)).map(remove).filter(x => x).size
    case "zrevrange"        => rangeByIndex(args(1), args(0), reverse = true)
    case "zrevrangebylex"   => rangeByKey(args(1), args(0), reverse = true)
    case "zrevrangebyscore" => rangeByScore(args(1), args(0), reverse = true)
    case "zrevrank"         => rank(args(0), reverse = true)
    case "zscan"            => scan(keys.keys)
    case "zscore"           => keys.get(args(0))
  }

}

@SerialVersionUID(1L)
class NodeEntry(
    val nodeType: String,
    @transient val node: ActorRef,
    @transient var expiry: Option[(Long, Cancellable)] = None)
  extends Serializable

class KeyNode extends Node[mutable.Map[String, mutable.Map[String, NodeEntry]]] {

  var value = mutable.Map[String, mutable.Map[String, NodeEntry]]()
  val wrongType = Error("Operation against a key holding the wrong kind of value", prefix = "WRONGTYPE")

  def dbFor(name: String) = value.getOrElseUpdate(name, mutable.Map[String, NodeEntry]())

  def db = dbFor(payload.db)

  def expire(when: Long): Int = {
    run("persist")
    val expires = (when - System.currentTimeMillis).toInt milliseconds
    val cancellable = context.system.scheduler.scheduleOnce(expires) {
      self ! Payload(Seq("_del", payload.key), db = payload.db)
    }
    db(payload.key).expiry = Some((when, cancellable))
    1
  }

  def ttl = db(payload.key).expiry match {
    case Some((when, _)) => when - System.currentTimeMillis
    case None => -1
  }

  def validate = {
    val exists      = db.contains(payload.key)
    val nodeType    = if (exists) db(payload.key).nodeType else ""
    val invalidType = nodeType != "" && payload.nodeType != nodeType &&
      payload.nodeType != "keys" && !Commands.overwrites(payload.command)
    val cantExist   = payload.command == "lpushx" || payload.command == "rpushx"
    val mustExist   = payload.command == "setnx"
    val default     = Commands.default(payload.command, payload.args)
    if (invalidType) Some(wrongType)
    else if ((exists && cantExist) || (!exists && mustExist)) Some("0")
    else if (!exists && default != ()) Some(default)
    else None
  }

  def node = {
    if (payload.nodeType == "keys") self
    else db.get(payload.key) match {
      case Some(entry) => entry.node
      case None => create(payload.db, payload.key, payload.nodeType)
    }
  }

  def create(db: String, key: String, nodeType: String, recovery: Boolean = false) = {
    dbFor(db)(key) = new NodeEntry(nodeType, context.actorOf(nodeType match {
      case "string"      => Props[StringNode]
      case "bitmap"      => Props[BitmapNode]
      case "hyperloglog" => Props[HyperLogLogNode]
      case "hash"        => Props[HashNode]
      case "list"        => Props[ListNode]
      case "set"         => Props[SetNode]
      case "sortedset"   => Props[SortedSetNode]
    }, s"$db-$nodeType-$key"))
    if (!recovery) save; dbFor(db)(key).node
  }

  def delete(key: String) = db.remove(key) match {
    case Some(entry) => entry.node ! Delete; true
    case None => false
  }

  override def run = {
    case "_del"       => (payload.key +: args).map(delete)
    case "_keys"      => pattern(db.keys, args(0))
    case "_randomkey" => randomItem(db.keys)
    case "_flushdb"   => db.clear; SimpleReply()
    case "_flushall"  => value.clear; SimpleReply()
    case "exists"     => args.map(db.contains)
    case "ttl"        => ttl / 1000
    case "pttl"       => ttl
    case "expire"     => expire(System.currentTimeMillis + (args(0).toInt * 1000))
    case "pexpire"    => expire(System.currentTimeMillis + args(0).toInt)
    case "expireat"   => expire(args(0).toLong / 1000)
    case "pexpireat"  => expire(args(0).toLong)
    case "type"       => if (db.contains(payload.key)) db(payload.key).nodeType else null
    case "renamenx"   => val x = db.contains(payload.key); if (x) {run("rename")}; x
    case "rename"     => db(payload.key).node ! Payload(Seq("_rename", payload.key, args(0)), db = payload.db); SimpleReply()
    case "persist"    =>
      val entry = db(payload.key)
      entry.expiry match {
        case Some((_, cancellable)) => cancellable.cancel(); entry.expiry = None; 1
        case None => 0
      }
    case "sort"       =>
      db(payload.key).nodeType match {
        case "list" | "set" | "sortedset" =>
          val sortArgs = Seq("_sort", payload.key) ++ payload.args
          db(payload.key).node ! Payload(sortArgs, db = payload.db, destination = payload.destination)
        case _ => wrongType
      }
  }

  override def receiveCommand = ({
    case Unrouted(p) => payload = p; validate match {
      case Some(errorOrDefault) => respond(errorOrDefault)
      case None =>
        val overwrite = !db.get(payload.key).filter(_.nodeType != payload.nodeType).isEmpty
        if (Commands.overwrites(payload.command) && overwrite) delete(payload.key)
        node ! payload
    }
  }: Receive) orElse super.receiveCommand

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot) =>
      snapshot.asInstanceOf[mutable.Map[String, mutable.Map[String, NodeEntry]]].foreach {db =>
        db._2.foreach {item => create(db._1, item._1, item._2.nodeType, recovery = true)}
      }
  }

}

class ClientNode extends Node[Null] {

  var value = null
  var quitting = false
  val buffer = new StringBuilder()
  var client: Option[ActorRef] = None
  var aggregateId = 0
  var db = payload.db
  val end = "\r\n"

  def aggregate(props: Props): Unit = {
    aggregateId += 1
    context.actorOf(props, s"aggregate-${payload.command}-${aggregateId}") ! payload
  }

  def run = {
    case "mset"        => argsPaired.foreach {args => route(Seq("set", args._1, args._2))}; SimpleReply()
    case "msetnx"      => aggregate(Props[AggregateMSetNX])
    case "mget"        => aggregate(Props[AggregateMGet])
    case "bitop"       => aggregate(Props[AggregateBitOp])
    case "dbsize"      => aggregate(Props[AggregateDBSize])
    case "del"         => aggregate(Props[AggregateDel])
    case "keys"        => aggregate(Props[AggregateKeys])
    case "flushdb"     => aggregate(Props[AggregateFlushDB])
    case "flushall"    => aggregate(Props[AggregateFlushAll])
    case "pfcount"     => aggregate(Props[AggregateHyperLogLogCount])
    case "pfmerge"     => aggregate(Props[AggregateHyperLogLogMerge])
    case "randomkey"   => aggregate(Props[AggregateRandomKey])
    case "scan"        => aggregate(Props[AggregateScan])
    case "sdiff"       => aggregate(Props[AggregateSet])
    case "sinter"      => aggregate(Props[AggregateSet])
    case "sunion"      => aggregate(Props[AggregateSet])
    case "sdiffstore"  => aggregate(Props[AggregateSetStore])
    case "sinterstore" => aggregate(Props[AggregateSetStore])
    case "sunionstore" => aggregate(Props[AggregateSetStore])
    case "zinterstore" => aggregate(Props[AggregateSortedSetStore])
    case "zunionstore" => aggregate(Props[AggregateSortedSetStore])
    case "select"      => db = args(0); SimpleReply()
    case "echo"        => args(0)
    case "ping"        => SimpleReply("PONG")
    case "time"        => val x = System.nanoTime; Seq(x / 1000000000, x % 1000000)
    case "shutdown"    => context.system.shutdown()
    case "quit"        => quitting = true; SimpleReply()
  }

  def validate = {
    if (payload.nodeType == "")
      Some(Error(s"unknown command '${payload.command}'"))
    else if ((payload.key == "" && Commands.keyed(payload.command))
        || !Commands.argsInRange(payload.command, payload.args))
      Some(Error(s"wrong number of arguments for '${payload.command}' command"))
    else None
  }

  def readInput(received: String) = {

    buffer.append(received)
    var pos = 0

    def next(length: Int = 0) = {
      val to = if (length <= 0) buffer.indexOf(end, pos) else pos + length
      val part = buffer.slice(pos, to)
      if (part.size != to - pos) throw new Exception()
      pos = to + end.size; part.stripLineEnd
    }

    def parts: Seq[String] = {
      val part = next()
      part.head match {
        case '-'|'+'|':' => Seq(part.tail)
        case '$'         => Seq(next(part.tail.toInt))
        case '*'         => (1 to part.tail.toInt).map(_ => parts.head)
        case _           => part.split(" ")
      }
    }

    Try(parts) match {
      case Success(output) => buffer.clear; output
      case Failure(_)      => Seq[String]()
    }

  }

  def writeOutput(response: Any): String = response match {
    case x: Iterable[Any]   => s"*${x.size}${end}${x.map(writeOutput).mkString}"
    case x: Boolean         => writeOutput(if (x) 1 else 0)
    case x: Number          => s":$x$end"
    case Error(msg, prefix) => s"-$prefix $msg$end"
    case SimpleReply(msg)   => s"+$msg$end"
    case null               => s"$$-1$end"
    case x                  => s"$$${x.toString.size}$end$x$end"
  }

  override def receiveCommand = ({
    case Tcp.Received(data) =>
      val input = readInput(data.utf8String)
      if (input.size > 0) {
        payload = Payload(input, db = db, destination = Some(self))
        client = Some(sender())
        validate match {
          case Some(error) => respond(error)
          case None =>
            if (payload.nodeType == "client") self ! payload
            else route(clientPayload = Option(payload))
        }
      }
    case Tcp.PeerClosed => context stop self
    case Response(_, response) => client.foreach {client =>
      client ! Tcp.Write(ByteString(writeOutput(response)))
      if (quitting) self ! Delete
    }
  }: Receive) orElse super.receiveCommand

}

abstract class Aggregate[T](val command: String)
    extends Actor with PayloadProcessing with ActorLogging {

  var responses = mutable.Map[String, T]()

  def keys = args

  def ordered = keys.map(responses(_))

  def complete: Any = ordered

  def begin = keys.foreach {key => route(Seq(command, key), destination = Some(self))}

  def receive = LoggingReceive {
    case p: Payload => payload = p; begin
    case Response(key, value) =>
      val keyOrIndex = if (responses.contains(key)) (responses.size + 1).toString else key
      responses(keyOrIndex) = value.asInstanceOf[T]
      if (responses.size == keys.size) {
        respond(Try(complete) match {
          case Success(response) => response
          case Failure(e) => log.error(e, s"Error running: $payload"); Error
        })
        context stop self
      }
  }

}

class AggregateMGet extends Aggregate[String]("get")

abstract class AggregateSetReducer[T](command: String) extends Aggregate[T](command) {
  lazy val reducer: (mutable.Set[String], mutable.Set[String]) => mutable.Set[String] = payload.command.tail match {
    case x if x.startsWith("diff")  => (_ &~ _)
    case x if x.startsWith("inter") => (_ & _)
    case x if x.startsWith("union") => (_ | _)
  }
}

class BaseAggregateSet extends AggregateSetReducer[mutable.Set[String]]("smembers")

class AggregateSet extends BaseAggregateSet {
  override def complete: Any = ordered.reduce(reducer)
}

class AggregateSetStore extends BaseAggregateSet {
  override def complete: Unit = {
    route(Seq("_sstore", payload.key) ++ ordered.reduce(reducer), destination = payload.destination)
  }
}

class AggregateSortedSetStore extends AggregateSetReducer[IndexedTreeMap[String, Int]]("_zget") {

  lazy val aggregatePos = argsUpper.indexOf("AGGREGATE")
  lazy val aggregateName = if (aggregatePos == -1) "SUM" else argsUpper(aggregatePos + 1)
  lazy val weightPos = argsUpper.indexOf("WEIGHTS")
  lazy val aggregate: (Int, Int) => Int = aggregateName match {
    case "SUM" => (_ + _)
    case "MIN" => min _
    case "MAX" => max _
  }

  def weight(i: Int) = if (weightPos == -1) 1 else args(weightPos + i + 1).toInt

  override def keys = args.slice(1, args(0).toInt + 1)

  override def complete: Unit = {
    var i = 0
    val result = ordered.reduce({(x, y) =>
      val out = new IndexedTreeMap[String, Int]()
      reducer(x.keySet, y.keySet).foreach {key =>
        lazy val xVal = x.get(key) * (if (i == 0) weight(i) else 1)
        lazy val yVal = y.get(key) * weight(i + 1)
        val value = if (!y.containsKey(key)) xVal
          else if (!x.containsKey(key)) yVal
          else aggregate(xVal, yVal)
        out.put(key, value)
      }; i += 1; out
    }).entrySet.toSeq.flatMap(e => Seq(e.getValue.toString, e.getKey))
    route(Seq("_zstore", payload.key) ++ result, destination = payload.destination)
  }

}

class AggregateBitOp extends Aggregate[mutable.BitSet]("_bget") {
  override def keys = args.drop(2)
  override def complete: Unit = {
    val result = args(0).toUpperCase match {
      case "AND" => ordered.reduce(_ & _)
      case "OR"  => ordered.reduce(_ | _)
      case "XOR" => ordered.reduce(_ ^ _)
      case "NOT" =>
        val from = ordered(0).headOption.getOrElse(1) - 1
        val to = ordered(0).lastOption.getOrElse(1) - 1
        mutable.BitSet(from until to: _*) ^ ordered(0)
    }
    route(Seq("_bstore", args(1)) ++ result, destination = payload.destination)
  }
}

class AggregateHyperLogLogCount extends Aggregate[Long]("_pfcount") {
  override def complete = responses.values.sum
}

class AggregateHyperLogLogMerge extends Aggregate[HLL]("_pfget") {
  override def complete = {
    val result = ordered.reduce({(x, y) => x.union(y); x}).toBytes.map(_.toString)
    route(Seq("_pfstore", payload.key) ++ result, destination = payload.destination)
  }
}

abstract class AggregateBroadcast[T](command: String) extends Aggregate[T](command) {
  lazy val broadcast = Broadcast(Payload(command +: broadcastArgs, db = payload.db, destination = Some(self)))
  def broadcastArgs = payload.args
  override def keys = (1 to context.system.settings.config.getInt("curiodb.keynodes")).map(_.toString)
  override def begin = context.system.actorSelection("/user/keys") ! broadcast
}

abstract class BaseAggregateKeys extends AggregateBroadcast[Iterable[String]]("_keys") {
  def reduced = responses.values.reduce(_ ++ _)
}

class AggregateKeys extends BaseAggregateKeys {
  override def complete = reduced
}

class AggregateScan extends BaseAggregateKeys {
  override def broadcastArgs = Seq("*")
  override def complete = scan(reduced)
}

class AggregateDBSize extends BaseAggregateKeys {
  override def broadcastArgs = Seq("*")
  override def complete = reduced.size
}

class AggregateRandomKey extends AggregateBroadcast[String]("_randomkey") {
  override def complete = randomItem(responses.values.filter(_ != ""))
}

abstract class AggregateBool(command: String) extends AggregateBroadcast[Iterable[Boolean]](command) {
  def trues = responses.values.flatten.filter(_ == true)
}

class AggregateDel extends AggregateBool("_del") {
  override def broadcastArgs = payload.args
  override def complete = trues.size
}

class AggregateMSetNX extends AggregateBool("exists") {
  override def keys = payload.argsPaired.map(_._1)
  override def complete = {
    if (trues.isEmpty) payload.argsPaired.foreach {args => route(Seq("set", args._1, args._2))}
    trues.isEmpty
  }
}

abstract class AggregateSimpleReply(command: String) extends AggregateBroadcast[String](command) {
  override def complete = SimpleReply()
}

class AggregateFlushDB extends AggregateSimpleReply("_flushdb")

class AggregateFlushAll extends AggregateSimpleReply("_flushall")

class Server(listen: URI) extends Actor {
  IO(Tcp)(context.system) ! Tcp.Bind(self, new InetSocketAddress(listen.getHost, listen.getPort))
  def receive = LoggingReceive {
    case Tcp.Connected(_, _) => sender() ! Tcp.Register(context.actorOf(Props[ClientNode]))
  }
}

object CurioDB {
  def main(args: Array[String]): Unit = {

    val sysName   = "curiodb"
    val config    = ConfigFactory.load()
    val listen    = new URI(config.getString("curiodb.listen"))
    val node      = if (args.isEmpty) config.getString("curiodb.node") else args(0)
    val nodes     = config.getObject("curiodb.nodes").map(node => (node._1 -> new URI(node._2.unwrapped.toString)))
    val keyNodes  = nodes.size * config.getInt("akka.actor.deployment./keys.cluster.max-nr-of-instances-per-node")
    val seedNodes = nodes.values.map(u => s""" "akka.${u.getScheme}://${sysName}@${u.getHost}:${u.getPort}" """)

    val system = ActorSystem(sysName, ConfigFactory.parseString(s"""
      curiodb.keynodes = ${keyNodes}
      curiodb.node = ${node}
      akka.cluster.seed-nodes = [${seedNodes.mkString(",")}]
      akka.cluster.min-nr-of-members = ${nodes.size}
      akka.remote.netty.tcp.hostname = "${nodes(node).getHost}"
      akka.remote.netty.tcp.port = ${nodes(node).getPort}
      akka.actor.deployment./keys.nr-of-instances = ${keyNodes}
    """).withFallback(config))

    Cluster(system).registerOnMemberUp {
      println("All nodes are up!")
      system.actorOf(Props[KeyNode].withRouter(FromConfig()), name = "keys")
    }

    system.actorOf(Props(new Server(listen)), "server")
    system.awaitTermination()

  }
}
