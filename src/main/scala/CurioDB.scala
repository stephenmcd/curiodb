package curiodb

import akka.actor.{ActorSystem, Actor, ActorContext, ActorRef, ActorLogging, Cancellable, Props}
import akka.event.LoggingReceive
import akka.io.{IO, Tcp}
import akka.routing.{Broadcast, ConsistentHashingPool}
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import akka.util.ByteString
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap, Set, LinkedHashSet}
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure, Random, Try}
import java.net.InetSocketAddress

class Spec(val args: Any, val default: (Seq[String] => Any), val keyed: Boolean)

object Spec {
  def apply(args: Any = 0, default: (Seq[String] => Any) = (_ => ()), keyed: Boolean = true) = {
    new Spec(args, default, keyed)
  }
}

object Commands {

  val many = Int.MaxValue - 1
  val evens = 2 to many by 2

  val error    = (_: Seq[String]) => "error"
  val nil      = (_: Seq[String]) => "nil"
  val ok       = (_: Seq[String]) => "OK"
  val zero     = (_: Seq[String]) => 0
  val negative = (_: Seq[String]) => -1
  val nils     = (x: Seq[String]) => x.map(_ => nil)
  val zeros    = (x: Seq[String]) => x.map(_ => zero)
  val seq      = (_: Seq[String]) => Seq()
  val string   = (_: Seq[String]) => ""
  val scan     = (_: Seq[String]) => Seq("0", "")

  val specs = Map(

    "string" -> Map(
      "_rename"     -> Spec(args = 1, default = nil),
      "append"       -> Spec(args = 1),
      "bitcount"     -> Spec(args = 0 to 2, default = zero),
      "bitop"        -> Spec(args = 3 to many, default = zero),
      "bitpos"       -> Spec(args = 1 to 3, default = negative),
      "decr"         -> Spec(),
      "decrby"       -> Spec(args = 1),
      "get"          -> Spec(default = nil),
      "getbit"       -> Spec(args = 1, default = zero),
      "getrange"     -> Spec(args = 2, default = string),
      "getset"       -> Spec(args = 1),
      "incr"         -> Spec(),
      "incrby"       -> Spec(args = 1),
      "incrbyfloat"  -> Spec(args = 1),
      "psetex"       -> Spec(args = 2),
      "set"          -> Spec(args = 1 to 4),
      "setbit"       -> Spec(args = 2),
      "setex"        -> Spec(args = 2),
      "setnx"        -> Spec(args = 1, default = zero),
      "setrange"     -> Spec(args = 2),
      "strlen"       -> Spec(default = zero)
    ),

    "hash" -> Map(
      "_hrename"    -> Spec(args = 1, default = nil),
      "hdel"         -> Spec(args = 1 to many, default = zeros),
      "hexists"      -> Spec(args = 1, default = zero),
      "hget"         -> Spec(args = 1, default = nil),
      "hgetall"      -> Spec(default = seq),
      "hincrby"      -> Spec(args = 2),
      "hincrbyfloat" -> Spec(args = 2),
      "hkeys"        -> Spec(default = seq),
      "hlen"         -> Spec(default = zero),
      "hmget"        -> Spec(args = 1 to many, default = nils),
      "hmset"        -> Spec(args = evens),
      "hscan"        -> Spec(args = 1 to 3, default = scan),
      "hset"         -> Spec(args = 2),
      "hsetnx"       -> Spec(args = 2),
      "hvals"        -> Spec(default = seq)
    ),

    "list" -> Map(
      "_lrename"    -> Spec(args = 1, default = nil),
      "blpop"        -> Spec(args = 1 to many, default = nil),
      "brpop"        -> Spec(args = 1 to many, default = nil),
      "brpoplpush"   -> Spec(args = 2, default = nil),
      "lindex"       -> Spec(args = 1, default = nil),
      "linsert"      -> Spec(args = 3, default = zero),
      "llen"         -> Spec(default = zero),
      "lpop"         -> Spec(default = nil),
      "lpush"        -> Spec(args = 1 to many),
      "lpushx"       -> Spec(args = 1, default = zero),
      "lrange"       -> Spec(args = 2, default = seq),
      "lrem"         -> Spec(args = 2, default = zero),
      "lset"         -> Spec(args = 2, default = error),
      "ltrim"        -> Spec(args = 2, default = ok),
      "rpop"         -> Spec(default = nil),
      "rpoplpush"    -> Spec(args = 1, default = nil),
      "rpush"        -> Spec(args = 1 to many),
      "rpushx"       -> Spec(args = 1, default = zero)
    ),

    "set" -> Map(
      "_srename"    -> Spec(args = 1, default = nil),
      "_sstore"      -> Spec(args = 1 to many),
      "sadd"         -> Spec(args = 1 to many),
      "scard"        -> Spec(default = zero),
      "sismember"    -> Spec(args = 1, default = zero),
      "smembers"     -> Spec(default = seq),
      "smove"        -> Spec(args = 2, default = error),
      "spop"         -> Spec(default = nil),
      "srandmember"  -> Spec(args = 0 to 1, default = nil),
      "srem"         -> Spec(args = 1 to many, default = zero),
      "sscan"        -> Spec(args = 1 to 3, default = scan)
    ),

    "keys" -> Map(
      "_del"         -> Spec(default = nil),
      "_keys"        -> Spec(args = 0 to 1),
      "_randomkey"   -> Spec(keyed = false),
      "exists"       -> Spec(),
      "expire"       -> Spec(args = 1),
      "expireat"     -> Spec(args = 1),
      "persist"      -> Spec(args = 1),
      "pexpire"      -> Spec(args = 1),
      "pexpireat"    -> Spec(args = 1),
      "pttl"         -> Spec(),
      "rename"       -> Spec(args = 1, default = error),
      "renamenx"     -> Spec(args = 1, default = error),
      "sort"         -> Spec(args = 1 to many, default = seq),
      "ttl"          -> Spec(),
      "type"         -> Spec()
    ),

    "client" -> Map(
      "sdiff"        -> Spec(args = 0 to many, default = seq, keyed = false),
      "sdiffstore"   -> Spec(args = 1 to many, default = zero),
      "sinter"       -> Spec(args = 0 to many, default = seq, keyed = false),
      "sinterstore"  -> Spec(args = 1 to many, default = zero),
      "sunion"       -> Spec(args = 0 to many, default = seq, keyed = false),
      "sunionstore"  -> Spec(args = 1 to many, default = zero),
      "del"          -> Spec(args = 1 to many, keyed = false),
      "keys"         -> Spec(args = 1, keyed = false),
      "scan"         -> Spec(args = 1 to 3, keyed = false),
      "randomkey"    -> Spec(keyed = false),
      "mget"         -> Spec(args = 1 to many, keyed = false),
      "mset"         -> Spec(args = evens, keyed = false),
      "msetnx"       -> Spec(args = evens, keyed = false)
    )

  )

  def get(command: String) = specs.find(_._2.contains(command)).getOrElse(("", Map[String, Spec]()))

  def default(command: String, args: Seq[String]) = get(command)._2(command).default(args)

  def keyed(command: String): Boolean = get(command)._2(command).keyed

  def nodeType(command: String) = get(command)._1

  def argsInRange(command: String, args: Seq[String]) =
    get(command)._2(command).args match {
      case fixed: Int => args.size == fixed
      case range: Range => range.contains(args.size)
    }

}

case class Payload(input: Seq[Any] = Seq(), destination: Option[ActorRef] = None) {

  val command = if (input.size > 0) input(0).toString.toLowerCase else ""
  val nodeType = if (command != "") Commands.nodeType(command) else ""
  val forKeyNode = nodeType == "keys"
  val forClientNode = nodeType == "client"
  val key = if (input.size > 1 && Commands.keyed(command)) input(1).toString else ""
  val args = input.slice(if (key == "") 1 else 2, input.size).map(_.toString)
  lazy val argPairs = (0 to args.size - 2 by 2).map {i => (args(i), args(i + 1))}

  def routeError = {
    if (nodeType == "") Some("Unknown command")
    else if (key == "" && Commands.keyed(command)) Some("No key specified")
    else if (!Commands.argsInRange(command, args)) Some("Invalid number of args")
    else None
  }

  def keyError(keyExists: Boolean, existingNodeType: String) = {

    val invalidType = !forKeyNode && keyExists && existingNodeType != nodeType
    val cantExist = command == "lpushx" || command == "rpushx"
    val mustExist = command == "setnx"
    val default = Commands.default(command, args)

    if (invalidType) Some(s"Invalid command ${command} for ${existingNodeType}")
    else if ((keyExists && cantExist) || (!keyExists && mustExist)) Some(0)
    else if (!keyExists && default != ()) Some(default)
    else None

  }

  def deliver(response: Any) = {
    destination.foreach {dest => if (response != ()) dest ! Response(key, response)}
  }

}

case class Unrouted(payload: Payload) extends ConsistentHashable {
  override def consistentHashKey: Any = payload.key
}

case class Response(key: String, value: Any)

trait PayloadProcessing {

  val context: ActorContext
  var payload = Payload()

  def args = payload.args

  def argPairs = payload.argPairs

  def route(input: Seq[Any] = Seq(), destination: Option[ActorRef] = None) = {
    val payload = Payload(input, destination)
    payload.routeError match {
      case Some(error) => payload.deliver(error)
      case None =>
        if (payload.forClientNode) {
          destination.foreach {dest => dest ! payload}
        } else {
          context.system.actorSelection("/user/keys") ! Unrouted(payload)
        }
    }
  }

}

object Rand {

  def string(size: Int = 8) = Random.alphanumeric.take(size).mkString

  def item(seq: Iterable[String]) = {
    if (seq.isEmpty) "" else seq.toSeq(Random.nextInt(seq.size))
  }

}

trait Scanning {

  def args: Seq[String]

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

}

abstract class LoggingActor extends Actor with ActorLogging {

  def receiver: Receive

  def receive = LoggingReceive(receiver)

}

abstract class Node extends LoggingActor with PayloadProcessing {

  type Run = PartialFunction[String, Any]

  def run: Run

  def receiver = {
    case "del" => context stop self
    case p: Payload =>
      payload = p
      payload.deliver(Try(run(payload.command)) match {
        case Success(response) => response
        case Failure(e) => log.error(s"$e"); "error"
      })
  }

  def rename(to: String, value: Any) = {
    route(Seq("_del", payload.key))
    route(Seq(to, args(0)) ++ (value match {
      case x: Iterable[Any] => x
      case x => Seq(x)
    }))
  }

}

class StringNode extends Node {

  var value = ""

  def valueOrZero = if (value == "") "0" else value

  def expire(command: String) = route(Seq(command, payload.key, args(1)))

  def run = {
    case "_rename"     => rename("set", value)
    case "get"         => value
    case "set"         => value = args(0); "OK"
    case "setnx"       => run("set"); true
    case "getset"      => val x = value; value = args(0); x
    case "append"      => value += args(0); value
    case "getrange"    => value.slice(args(0).toInt, args(1).toInt)
    case "setrange"    => value.patch(args(0).toInt, args(1), 1)
    case "strlen"      => value.size
    case "incr"        => value = (valueOrZero.toInt + 1).toString; value
    case "incrby"      => value = (valueOrZero.toInt + args(0).toInt).toString; value
    case "incrbyfloat" => value = (valueOrZero.toFloat + args(0).toFloat).toString; value
    case "decr"        => value = (valueOrZero.toInt - 1).toString; value
    case "decrby"      => value = (valueOrZero.toInt - args(0).toInt).toString; value
    case "bitcount"    => value.getBytes.map{_.toInt.toBinaryString.count(_ == "1")}.sum
    case "bitop"       => "Not implemented"
    case "bitpos"      => "Not implemented"
    case "getbit"      => "Not implemented"
    case "setbit"      => "Not implemented"
    case "setex"       => val x = run("set"); expire("expire"); x
    case "psetex"      => val x = run("set"); expire("pexpire"); x
  }

}

class BaseHashNode[T] extends Node with Scanning {

  var value = MutableMap[String, T]()

  def exists = value.contains _

  def run = {
    case "hkeys"   => value.keys
    case "hexists" => exists(args(0))
    case "hscan"   => scan(value.keys)
  }

}

class HashNode extends BaseHashNode[String] {

  def set(arg: Any) = {val x = arg.toString; value(args(0)) = x; x}

  def flatten = value.map(x => Seq(x._1, x._2)).flatten

  override def run = ({
    case "_hrename"     => rename("hmset", flatten)
    case "hget"         => value.get(args(0))
    case "hsetnx"       => if (exists(args(0))) run("hset") else false
    case "hgetall"      => flatten
    case "hvals"        => value.values
    case "hdel"         => val x = run("hexists"); value -= args(0); x
    case "hlen"         => value.size
    case "hmget"        => args.map(value.get(_))
    case "hmset"        => argPairs.foreach {args => value(args._1) = args._2}; "OK"
    case "hincrby"      => set(value.getOrElse(args(0), "0").toInt + args(1).toInt)
    case "hincrbyfloat" => set(value.getOrElse(args(0), "0").toFloat + args(1).toFloat)
    case "hset"         => val x = !exists(args(0)); set(args(1)); x
  }: Run) orElse super.run

}

class ListNode extends Node {

  var value = ArrayBuffer[String]()
  var blocked = LinkedHashSet[Payload]()

  def slice = {
    val to = args(1).toInt
    value.slice(args(0).toInt, if (to < 0) value.size + 1 + to else to + 1)
  }

  def block: Any = {
    if (value.isEmpty) {
      blocked += payload
      context.system.scheduler.scheduleOnce(args.last.toInt seconds) {
        blocked -= payload
        payload.deliver("nil")
      }; ()
    } else run(payload.command.tail)
  }

  def unblock(result: Any) = {
    while (value.size > 0 && blocked.size > 0) {
      payload = blocked.head
      blocked -= payload
      payload.deliver(run(payload.command.tail))
    }
    result
  }

  def run = ({
    case "_lrename"   => rename("lpush", value)
    case "lpush"      => args ++=: value; payload.deliver(run("llen"))
    case "rpush"      => value ++= args; payload.deliver(run("llen"))
    case "lpushx"     => run("lpush")
    case "rpushx"     => run("rpush")
    case "lpop"       => val x = value(0); value -= x; x
    case "rpop"       => val x = value.last; value.reduceToSize(value.size - 1); x
    case "lset"       => value(args(0).toInt) = args(1); payload.deliver("OK")
    case "lindex"     => value(args(0).toInt)
    case "lrem"       => value.remove(args(0).toInt)
    case "lrange"     => slice
    case "ltrim"      => value = slice; "OK"
    case "llen"       => value.size
    case "blpop"      => block
    case "brpop"      => block
    case "brpoplpush" => block
    case "rpoplpush"  => val x = run("rpop"); route("lpush" +: args :+ x.toString); x
    case "linsert"    =>
      val i = value.indexOf(args(1)) + (if (args(0) == "AFTER") 1 else 0)
      if (i >= 0) {value.insert(i, args(2)); payload.deliver(run("llen"))} else -1
  }: Run) andThen unblock

}

class SetNode extends Node with Scanning {

  var value = Set[String]()

  def run = {
    case "_srename"    => rename("smembers", value)
    case "_sstore"     => value.clear; run("sadd")
    case "sadd"        => val x = (args.toSet &~ value).size; value ++= args; x
    case "srem"        => val x = (args.toSet & value).size; value --= args; x
    case "scard"       => value.size
    case "sismember"   => value.contains(args(0))
    case "smembers"    => value
    case "srandmember" => Rand.item(value)
    case "spop"        => val x = run("srandmember"); value -= x.toString; x
    case "sscan"       => scan(value)
    case "smove"       =>
      val x = value.contains(args(1))
      if (x) {value -= args(1); route("sadd" +: args)}; x
  }

}

class NodeEntry(val node: ActorRef, val nodeType: String, var expiry: Option[(Long, Cancellable)] = None)

class KeyNode extends BaseHashNode[NodeEntry] {

  def expire(when: Long): Boolean = {
    val x = run("persist") != -2
    if (x) {
      val expires = ((when - System.currentTimeMillis).toInt milliseconds)
      val cancellable = context.system.scheduler.scheduleOnce(expires) {
        self ! Payload(Seq("_del", payload.key))
      }
      value(payload.key).expiry = Some((when, cancellable))
    }; x
  }

  def ttl = {
    if (!exists(payload.key)) -2
    else value(payload.key).expiry match {
      case None => -1
      case Some((when, _)) => when - System.currentTimeMillis
    }
  }

  override def run = ({
    case "_del"       => val x = exists(payload.key); value -= payload.key; x
    case "_keys"      => if (payload.key != "") pattern(value.keys, payload.key) else value.keys
    case "_randomkey" => Rand.item(value.keys)
    case "exists"     => exists(payload.key)
    case "ttl"        => ttl / 1000
    case "pttl"       => ttl
    case "expire"     => expire(System.currentTimeMillis + (args(0).toInt * 1000))
    case "pexpire"    => expire(System.currentTimeMillis + args(0).toInt)
    case "expireat"   => expire(args(0).toLong / 1000)
    case "pexpireat"  => expire(args(0).toLong)
    case "sort"       => "Not implemented"
    case "type"       => if (exists(payload.key)) value(args(0)).nodeType else "nil"
    case "renamenx"   => val x = exists(payload.key); if (x) {run("rename")}; x
    case "rename"     =>
      if (payload.key != args(0)) {
        route(Seq("_del", args(0)))
        val command = value(payload.key).nodeType match {
          case "string" => "_rename"
          case "hash"   => "_hrename"
          case "list"   => "_lrename"
          case "set"    => "_srename"
        }
        route(Seq(command, payload.key, args(0)))
        "OK"
      } else "error"
    case "persist"    =>
      val x = exists(payload.key)
      if (x) {
        val expiry = value(args(0)).expiry.get
        if (expiry != None) expiry._2.cancel()
      }; x
  }: Run) orElse super.run

  override def receiver = ({
    case Unrouted(payload) =>
      val keyExists = exists(payload.key)
      val nodeType = if (keyExists) value(payload.key).nodeType else ""
      payload.keyError(keyExists, nodeType) match {
        case Some(error) => payload.deliver(error)
        case None =>
          val node = if (payload.forKeyNode) self
            else if (keyExists) value(payload.key).node
            else {
              val created = context.actorOf(payload.nodeType match {
                case "string" => Props[StringNode]
                case "hash"   => Props[HashNode]
                case "list"   => Props[ListNode]
                case "set"    => Props[SetNode]
              }, s"${payload.key}-${Rand.string()}")
              value(payload.key) = new NodeEntry(created, payload.nodeType)
              created
            }
          node ! payload
      }
  }: Receive) orElse super.receiver

}

class ClientNode extends Node {

  val buffer = new StringBuilder()
  var client: Option[ActorRef] = None

  def aggregate(props: Props): Unit = {
    context.actorOf(props, s"aggregate-${Rand.string()}") ! payload
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
    case "sinter"      => aggregate(Props[AggregateSInterStore])
    case "sunion"      => aggregate(Props[AggregateSUnion])
    case "sdiffstore"  => aggregate(Props[AggregateSDiffStore])
    case "sinterstore" => aggregate(Props[AggregateSInterStore])
    case "sunionstore" => aggregate(Props[AggregateSUnionStore])
  }

  // TODO: read/write redis protocol.
  override def receiver = ({
    case Tcp.Received(data) =>
      val received = data.utf8String
      buffer.append(received)
      if (received.endsWith("\n")) {
        client = Some(sender())
        route(buffer.stripLineEnd.split(' '), destination = Some(self))
        buffer.clear()
      }
    case Tcp.PeerClosed => context stop self
    case Response(_, value) =>
      val message = (value match {
        case x: Iterable[Any] => x.mkString("\n")
        case x: Boolean => if (x) "1" else "0"
        case x => x.toString
      })
      client.foreach {client => client ! Tcp.Write(ByteString(message + "\n"))
  }
  }: Receive) orElse super.receiver

}

abstract class Aggregate[T](val command: String) extends LoggingActor with PayloadProcessing {

  val responses = MutableMap[String, T]()
  lazy val ordered = args.map((key: String) => responses(key))

  def complete: Any

  def begin = args.foreach {key => route(Seq(command, key), Some(self))}

  def receiver = {
    case p: Payload => payload = p; begin
    case Response(key, value) =>
      val keyOrIndex = if (key == "") (responses.size + 1).toString else key
      responses(keyOrIndex) = value.asInstanceOf[T]
      if (responses.size == args.size) {
        payload.deliver(complete)
        context stop self
      }
  }

}

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

class AggregateMGet extends Aggregate[String]("get") {
  override def complete = ordered
}

abstract class AggregateBool(command: String) extends Aggregate[Boolean](command) {
  def filtered = responses.values.filter(_ == true)
}

class AggregateDel extends AggregateBool("_del") {
  override def complete = filtered.size
}

class AggregateMSetNX extends AggregateBool("exists") {
  // This is hugely slow since it needs to run exists once per key.
  // What we need is a way to initially group the keys by hash value,
  // then send each group as one payload. This would form a strategy for
  // exists handling multiple keys in general, in which case it would
  // become a ClientNode command.
  override def args = payload.argPairs.map(_._1)
  override def complete = {
    val x = filtered.isEmpty
    if (x) payload.argPairs.foreach {args => route(Seq("set", args._1, args._2))}
    x
  }
}

abstract class AggregateBroadcast[T](command: String) extends Aggregate[T](command) {
  val keyNodes = context.system.settings.config.getInt("curiodb.keynodes")
  lazy val broadcast = Broadcast(Payload(command +: payload.args, Some(self)))
  override def args = (1 to keyNodes).map(_.toString)
  override def begin = context.system.actorSelection("/user/keys") ! broadcast
}

class AggregateKeys extends AggregateBroadcast[Iterable[String]]("_keys") {
  override def complete = responses.values.reduce(_ ++ _)
}

class AggregateRandomKey extends AggregateBroadcast[String]("_randomkey") {
  override def complete = Rand.item(responses.values.filter(_ != ""))
}

class AggregateScan extends AggregateKeys with Scanning {
  override def complete = scan(super.complete)
}

class Server(host: String, port: Int) extends LoggingActor {
  import context.system
  IO(Tcp) ! Tcp.Bind(self, new InetSocketAddress(host, port))
  def receiver = {
    case Tcp.Connected(_, _) => sender() ! Tcp.Register(context.actorOf(Props[ClientNode]))
  }
}

object CurioDB extends App {
  val system = ActorSystem()
  val host = system.settings.config.getString("curiodb.host")
  val port = system.settings.config.getInt("curiodb.port")
  val keyNodes = system.settings.config.getInt("curiodb.keynodes")
  system.actorOf(ConsistentHashingPool(keyNodes).props(Props[KeyNode]), name = "keys")
  system.actorOf(Props(new Server(host, port)), "server")
  system.awaitTermination()
}
