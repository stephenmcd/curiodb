package curiodb

import akka.actor._
import akka.cluster.Cluster
import akka.event.LoggingReceive
import akka.io.{IO, Tcp}
import akka.persistence._
import akka.routing.{Broadcast, FromConfig}
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import java.net.{InetSocketAddress, URI}
import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap, Set, LinkedHashSet}
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure, Random, Try}

case class Spec(
  val args: Any = 0,
  val default: (Seq[String] => Any) = (_ => ()),
  val keyed: Boolean = true,
  val writes: Boolean = false,
  val overwrites: Boolean = false)

case class Error(message: String, prefix: String = "ERR")

object Commands {

  val many = Int.MaxValue - 1
  val evens = 2 to many by 2

  val error  = (_: Seq[String]) => Error("no such key")
  val nil    = (_: Seq[String]) => null
  val ok     = (_: Seq[String]) => "OK"
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
      "bitcount"     -> Spec(args = 0 to 2, default = zero),
      "bitpos"       -> Spec(args = 1 to 3, default = neg1),
      "decr"         -> Spec(writes = true),
      "decrby"       -> Spec(args = 1, writes = true),
      "get"          -> Spec(default = nil),
      "getbit"       -> Spec(args = 1, default = zero),
      "getrange"     -> Spec(args = 2, default = string),
      "getset"       -> Spec(args = 1, writes = true),
      "incr"         -> Spec(writes = true),
      "incrby"       -> Spec(args = 1, writes = true),
      "incrbyfloat"  -> Spec(args = 1, writes = true),
      "psetex"       -> Spec(args = 2, overwrites = true),
      "set"          -> Spec(args = 1 to 4, overwrites = true),
      "setbit"       -> Spec(args = 2, writes = true),
      "setex"        -> Spec(args = 2, overwrites = true),
      "setnx"        -> Spec(args = 1, default = zero, writes = true),
      "setrange"     -> Spec(args = 2, writes = true),
      "strlen"       -> Spec(default = zero)
    ),

    "hash" -> Map(
      "_rename"      -> Spec(args = 1),
      "_hstore"      -> Spec(args = evens, overwrites = true),
      "hdel"         -> Spec(args = 1 to many, default = zeros, writes = true),
      "hexists"      -> Spec(args = 1, default = zero),
      "hget"         -> Spec(args = 1, default = nil),
      "hgetall"      -> Spec(default = seq),
      "hincrby"      -> Spec(args = 2, writes = true),
      "hincrbyfloat" -> Spec(args = 2, writes = true),
      "hkeys"        -> Spec(default = seq),
      "hlen"         -> Spec(default = zero),
      "hmget"        -> Spec(args = 1 to many, default = nils),
      "hmset"        -> Spec(args = evens, writes = true),
      "hscan"        -> Spec(args = 1 to 3, default = scan),
      "hset"         -> Spec(args = 2, writes = true),
      "hsetnx"       -> Spec(args = 2, writes = true),
      "hvals"        -> Spec(default = seq)
    ),

    "list" -> Map(
      "_rename"      -> Spec(args = 1),
      "_lstore"      -> Spec(args = 0 to many, overwrites = true),
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

    "keys" -> Map(
      "_del"         -> Spec(default = nil, writes = true),
      "_keys"        -> Spec(args = 0 to 1, keyed = false),
      "_randomkey"   -> Spec(keyed = false),
      "exists"       -> Spec(args = 1 to many, keyed = false),
      "expire"       -> Spec(args = 1, default = zero),
      "expireat"     -> Spec(args = 1, default = zero),
      "persist"      -> Spec(default = zero),
      "pexpire"      -> Spec(args = 1, default = zero),
      "pexpireat"    -> Spec(args = 1, default = zero),
      "pttl"         -> Spec(default = neg2),
      "rename"       -> Spec(args = 1, default = error),
      "renamenx"     -> Spec(args = 1, default = error),
      "ttl"          -> Spec(default = neg2),
      "type"         -> Spec()
    ),

    "client" -> Map(
      "bitop"        -> Spec(args = 3 to many, default = zero),
      "del"          -> Spec(args = 1 to many, keyed = false),
      "keys"         -> Spec(args = 1, keyed = false),
      "scan"         -> Spec(args = 1 to 3, keyed = false),
      "sdiff"        -> Spec(args = 0 to many, default = seq, keyed = false),
      "sdiffstore"   -> Spec(args = 1 to many, default = zero),
      "sinter"       -> Spec(args = 0 to many, default = seq, keyed = false),
      "sinterstore"  -> Spec(args = 1 to many, default = zero),
      "sunion"       -> Spec(args = 0 to many, default = seq, keyed = false),
      "sunionstore"  -> Spec(args = 1 to many, default = zero),
      "randomkey"    -> Spec(keyed = false),
      "mget"         -> Spec(args = 1 to many, keyed = false),
      "mset"         -> Spec(args = evens, keyed = false),
      "msetnx"       -> Spec(args = evens, keyed = false)
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

case class Payload(input: Seq[Any] = Seq(), destination: Option[ActorRef] = None) {
  val command = if (input.size > 0) input(0).toString.toLowerCase else ""
  val nodeType = if (command != "") Commands.nodeType(command) else ""
  val key = if (nodeType != "" && input.size > 1 && Commands.keyed(command)) input(1).toString else ""
  val args = input.slice(if (key == "") 1 else 2, input.size).map(_.toString)
  lazy val argPairs = (0 to args.size - 2 by 2).map {i => (args(i), args(i + 1))}
}

case class Unrouted(payload: Payload) extends ConsistentHashable {
  override def consistentHashKey: Any = payload.key
}

case class Response(key: String, value: Any)

trait PayloadProcessing extends Actor {

  var payload = Payload()

  def args = payload.args

  def argPairs = payload.argPairs

  def route(
      input: Seq[Any] = Seq(),
      destination: Option[ActorRef] = None,
      payload: Option[Payload] = None) =
    context.system.actorSelection("/user/keys") ! Unrouted(payload match {
      case Some(payload) => payload
      case None => Payload(input, destination)
    })

  def deliver(response: Any) =
    if (response != ()) payload.destination.foreach {destination =>
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

  def slice[T](value: Seq[T]): Seq[T] = {
    val to = args(1).toInt
    value.slice(args(0).toInt, if (to < 0) value.size + 1 + to else to + 1)
  }

}

abstract class Node[T] extends PersistentActor with PayloadProcessing with ActorLogging {

  var value: T
  var lastSnapshot: Option[SnapshotMetadata] = None
  val persistence = context.system.settings.config.getBoolean("curiodb.persistence")

  type Run = PartialFunction[String, Any]

  def run: Run

  def persistenceId = self.path.name

  def save = if (persistence) saveSnapshot(value)

  def deleteOldSnapshots(stopping: Boolean = false) =
    if (persistence)
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
    case "_del" => deleteOldSnapshots(stopping = true); context stop self
    case p: Payload =>
      payload = p
      deliver(Try(run(payload.command)) match {
        case Success(response) => if (Commands.writes(payload.command)) save; response
        case Failure(e) => log.error(e, s"Error running: $payload"); "unknown error"
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

}

class StringNode extends Node[String] {

  var value = ""

  def valueOrZero = if (value == "") "0" else value

  def expire(command: String) = route(Seq(command, payload.key, args(1)))

  def run = {
    case "_rename"     => rename(value, "set")
    case "get"         => value
    case "set"         => value = args(0); "OK"
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
    case "bitcount"    => value.getBytes.map{_.toInt.toBinaryString.count(_ == "1")}.sum
    case "bitop"       => Error("not implemented")
    case "bitpos"      => Error("not implemented")
    case "getbit"      => Error("not implemented")
    case "setbit"      => Error("not implemented")
    case "setex"       => val x = run("set"); expire("expire"); x
    case "psetex"      => val x = run("set"); expire("pexpire"); x
  }

}

class HashNode extends Node[MutableMap[String, String]] {

  var value = MutableMap[String, String]()

  def set(arg: Any) = {val x = arg.toString; value(args(0)) = x; x}

  override def run = {
    case "_rename"      => rename(run("hgetall"), "_hstore")
    case "_hstore"      => value.clear; run("hmset")
    case "hkeys"        => value.keys
    case "hexists"      => value.contains(args(0))
    case "hscan"        => scan(value.keys)
    case "hget"         => value.getOrElse(args(0), null)
    case "hsetnx"       => if (!value.contains(args(0))) run("hset") else false
    case "hgetall"      => value.map(x => Seq(x._1, x._2)).flatten
    case "hvals"        => value.values
    case "hdel"         => val x = run("hexists"); value -= args(0); x
    case "hlen"         => value.size
    case "hmget"        => args.map(value.get(_))
    case "hmset"        => argPairs.foreach {args => value(args._1) = args._2}; "OK"
    case "hincrby"      => set(value.getOrElse(args(0), "0").toInt + args(1).toInt).toInt
    case "hincrbyfloat" => set(value.getOrElse(args(0), "0").toFloat + args(1).toFloat)
    case "hset"         => val x = !value.contains(args(0)); set(args(1)); x
  }

}

class ListNode extends Node[ArrayBuffer[String]] {

  var value = ArrayBuffer[String]()
  var blocked = LinkedHashSet[Payload]()

  def block: Any = {
    if (value.isEmpty) {
      blocked += payload
      context.system.scheduler.scheduleOnce(args.last.toInt seconds) {
        blocked -= payload
        deliver(null)
      }; ()
    } else run(payload.command.tail)
  }

  def unblock(result: Any) = {
    while (value.size > 0 && blocked.size > 0) {
      payload = blocked.head
      blocked -= payload
      deliver(run(payload.command.tail))
    }
    result
  }

  def run = ({
    case "_rename"    => rename(value, "_lstore")
    case "_lstore"    => value.clear; run("lpush")
    case "lpush"      => args ++=: value; deliver(run("llen"))
    case "rpush"      => value ++= args; deliver(run("llen"))
    case "lpushx"     => run("lpush")
    case "rpushx"     => run("rpush")
    case "lpop"       => val x = value(0); value -= x; x
    case "rpop"       => val x = value.last; value.reduceToSize(value.size - 1); x
    case "lset"       => value(args(0).toInt) = args(1); deliver("OK")
    case "lindex"     => val x = args(0).toInt; if (x >= 0 && x < value.size) value(x) else null
    case "lrem"       => value.remove(args(0).toInt)
    case "lrange"     => slice(value)
    case "ltrim"      => value = slice(value).asInstanceOf[ArrayBuffer[String]]; "OK"
    case "llen"       => value.size
    case "blpop"      => block
    case "brpop"      => block
    case "brpoplpush" => block
    case "rpoplpush"  => val x = run("rpop"); route("lpush" +: args :+ x.toString); x
    case "linsert"    =>
      val i = value.indexOf(args(1)) + (if (args(0) == "AFTER") 1 else 0)
      if (i >= 0) {value.insert(i, args(2)); deliver(run("llen"))} else -1
  }: Run) andThen unblock

}

class SetNode extends Node[Set[String]] {

  var value = Set[String]()

  def run = {
    case "_rename"     => rename(value, "_sstore")
    case "_sstore"     => value.clear; run("sadd")
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

@SerialVersionUID(1L)
class NodeEntry(
    val nodeType: String,
    @transient val node: ActorRef,
    @transient var expiry: Option[(Long, Cancellable)] = None)
  extends Serializable

class KeyNode extends Node[MutableMap[String, NodeEntry]] {

  var value = MutableMap[String, NodeEntry]()
  val wrongType = Error("Operation against a key holding the wrong kind of value", prefix = "WRONGTYPE")

  def expire(when: Long): Int = {
    run("persist")
    val expires = ((when - System.currentTimeMillis).toInt milliseconds)
    val cancellable = context.system.scheduler.scheduleOnce(expires) {
      self ! Payload(Seq("_del", payload.key))
    }
    value(payload.key).expiry = Some((when, cancellable))
    1
  }

  def ttl = {
    value(payload.key).expiry match {
      case Some((when, _)) => when - System.currentTimeMillis
      case None => -1
    }
  }

  def validate = {
    val exists      = value.contains(payload.key)
    val nodeType    = if (exists) value(payload.key).nodeType else ""
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
    else if (value.contains(payload.key)) value(payload.key).node
    else create(payload.key, payload.nodeType)
  }

  def create(key: String, nodeType: String, recovery: Boolean = false) = {
    value(key) = new NodeEntry(nodeType, context.actorOf(nodeType match {
      case "string" => Props[StringNode]
      case "hash"   => Props[HashNode]
      case "list"   => Props[ListNode]
      case "set"    => Props[SetNode]
    }, s"$nodeType-$key"))
    if (!recovery) save
    value(key).node
  }

  def delete(key: String) = value.remove(key) match {
    case Some(entry) => entry.node ! "_del"; true
    case None => false
  }

  override def run = {
    case "_del"       => (payload.key +: args).map(delete)
    case "_keys"      => pattern(value.keys, args(0))
    case "_randomkey" => randomItem(value.keys)
    case "exists"     => args.map(value.contains)
    case "ttl"        => ttl / 1000
    case "pttl"       => ttl
    case "expire"     => expire(System.currentTimeMillis + (args(0).toInt * 1000))
    case "pexpire"    => expire(System.currentTimeMillis + args(0).toInt)
    case "expireat"   => expire(args(0).toLong / 1000)
    case "pexpireat"  => expire(args(0).toLong)
    case "type"       => if (value.contains(payload.key)) value(payload.key).nodeType else null
    case "renamenx"   => val x = value.contains(payload.key); if (x) {run("rename")}; x
    case "rename"     => value(payload.key).node ! Payload(Seq("_rename", payload.key, args(0))); "OK"
    case "persist"    =>
      val entry = value(payload.key)
      entry.expiry match {
        case Some((_, cancellable)) => cancellable.cancel(); entry.expiry = None; 1
        case None => 0
      }
  }

  override def receiveCommand = ({
    case Unrouted(p) => payload = p; validate match {
      case Some(errorOrDefault) => deliver(errorOrDefault)
      case None =>
        val overwrite = !value.get(payload.key).filter(_.nodeType != payload.nodeType).isEmpty
        if (Commands.overwrites(payload.command) && overwrite) delete(payload.key)
        node ! payload
    }
  }: Receive) orElse super.receiveCommand

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot) =>
      snapshot.asInstanceOf[MutableMap[String, NodeEntry]].foreach {item =>
        create(item._1, item._2.nodeType, recovery = true)
      }
  }

}

class ClientNode extends Node[Null] {

  var value = null
  val buffer = new StringBuilder()
  var client: Option[ActorRef] = None
  var aggregateId = 0

  def aggregate(props: Props): Unit = {
    aggregateId += 1
    context.actorOf(props, s"aggregate-${payload.command}-${aggregateId}") ! payload
  }

  def run = {
    case "mset"        => argPairs.foreach {args => route(Seq("set", args._1, args._2))}; "OK"
    case "msetnx"      => aggregate(Props[AggregateMSetNX])
    case "mget"        => aggregate(Props[AggregateMGet])
    case "del"         => aggregate(Props[AggregateDel])
    case "keys"        => aggregate(Props[AggregateKeys])
    case "scan"        => aggregate(Props[AggregateScan])
    case "randomkey"   => aggregate(Props[AggregateRandomKey])
    case "sdiff"       => aggregate(Props[AggregateSDiff])
    case "sinter"      => aggregate(Props[AggregateSInter])
    case "sunion"      => aggregate(Props[AggregateSUnion])
    case "sdiffstore"  => aggregate(Props[AggregateSDiffStore])
    case "sinterstore" => aggregate(Props[AggregateSInterStore])
    case "sunionstore" => aggregate(Props[AggregateSUnionStore])
    case "select"      => "OK"
  }

  def validate = {
    if (payload.nodeType == "")
      Some(Error(s"unknown command '${payload.command}'"))
    else if ((payload.key == "" && Commands.keyed(payload.command))
        || !Commands.argsInRange(payload.command, payload.args))
      Some(Error(s"wrong number of arguments for '${payload.command}' command"))
    else None
  }

  def fromResp(received: StringBuilder) =
    received.stripLineEnd.split("\r\n", -1).filterNot {x: String =>
      x.size > 1 && "*$".indexOf(x.head) > -1
    }

  def toResp(response: Any): String = response match {
    case x: Iterable[Any]   => s"*${x.size}\r\n${x.map(toResp).mkString}\r\n"
    case x: Boolean         => toResp(if (x) 1 else 0)
    case x: Integer         => s":$x\r\n"
    case Error(msg, prefix) => s"-$prefix $msg\r\n"
    case null               => "$-1\r\n"
    case x                  => s"+$x\r\n"
  }

  override def receiveCommand = ({
    case Tcp.Received(data) =>
      val received = data.utf8String
      buffer.append(received)
      if (received.endsWith("\r\n")) {
        payload = Payload(fromResp(buffer), destination = Some(self))
        buffer.clear()
        client = Some(sender())
        validate match {
          case Some(error) => deliver(error)
          case None =>
            if (payload.nodeType == "client") self ! payload
            else route(payload = Option(payload))
        }
      }
    case Tcp.PeerClosed => context stop self
    case Response(_, response) => client.foreach {client =>
      client ! Tcp.Write(ByteString(toResp(response)))
    }
  }: Receive) orElse super.receiveCommand

}

abstract class Aggregate[T](val command: String) extends Actor with PayloadProcessing {

  var responses = MutableMap[String, T]()

  def keys = args

  def ordered = keys.map((key: String) => responses(key))

  def complete: Any = ordered

  def begin = keys.foreach {key => route(Seq(command, key), Some(self))}

  def receive = LoggingReceive {
    case p: Payload => payload = p; begin
    case Response(key, value) =>
      val keyOrIndex = if (responses.contains(key)) (responses.size + 1).toString else key
      responses(keyOrIndex) = value.asInstanceOf[T]
      if (responses.size == keys.size) {deliver(complete); context stop self}
  }

}

class AggregateMGet extends Aggregate[String]("get")

abstract class AggregateSet(reducer: (Set[String], Set[String]) => Set[String])
  extends Aggregate[Set[String]]("smembers") {
  override def complete: Any = ordered.reduce(reducer)
}

class AggregateSetStore(reducer: (Set[String], Set[String]) => Set[String]) extends AggregateSet(reducer) {
  override def complete: Unit = {
    val result = super.complete.asInstanceOf[Set[String]].toSeq
    route(Seq("_sstore", payload.key) ++ result, destination = payload.destination)
  }
}

class AggregateSDiff extends AggregateSet(_ &~ _)

class AggregateSInter extends AggregateSet(_ & _)

class AggregateSUnion extends AggregateSet(_ | _)

class AggregateSDiffStore extends AggregateSetStore(_ &~ _)

class AggregateSInterStore extends AggregateSetStore(_ & _)

class AggregateSUnionStore extends AggregateSetStore(_ | _)

abstract class AggregateBroadcast[T](command: String) extends Aggregate[T](command) {
  lazy val broadcast = Broadcast(Payload(command +: broadcastArgs, Some(self)))
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

class AggregateRandomKey extends AggregateBroadcast[String]("_randomkey") {
  override def complete = randomItem(responses.values.filter(_ != ""))
}

class AggregateScan extends BaseAggregateKeys {
  override def broadcastArgs = Seq("*")
  override def complete = scan(reduced)
}

abstract class AggregateBool(command: String) extends AggregateBroadcast[Iterable[Boolean]](command) {
  def trues = responses.values.flatten.filter(_ == true)
}

class AggregateDel extends AggregateBool("_del") {
  override def broadcastArgs = payload.args
  override def complete = trues.size
}

class AggregateMSetNX extends AggregateBool("exists") {
  override def keys = payload.argPairs.map(_._1)
  override def complete = {
    val x = trues.isEmpty
    if (x) payload.argPairs.foreach {args => route(Seq("set", args._1, args._2))}
    x
  }
}

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
