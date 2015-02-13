package curiodb

import akka.actor.{ActorSystem, Actor, ActorContext, ActorSelection, ActorRef, ActorLogging, Cancellable, Props}
import akka.event.LoggingReceive
import akka.io.{IO, Tcp}
import akka.pattern.ask
import akka.routing.{Broadcast, ConsistentHashingPool}
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import akka.util.{ByteString, Timeout}
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap, Set, LinkedHashSet}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure, Random, Try}
import java.net.InetSocketAddress

class Spec(val args: Any, val default: (Seq[String] => Any))

object Spec {
  def apply(args: Any = 0, default: (Seq[String] => Any) = (_ => ())) = {
    new Spec(args, default)
  }
}

object Commands {

  val many = Int.MaxValue
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
      "_renamed"     -> Spec(args = 1, default = nil),
      "append"       -> Spec(args = 1),
      "bitcount"     -> Spec(args = 0 to 2, default = zero),
      "bitop"        -> Spec(args = 3 to many, default = zero),
      "bitpos"       -> Spec(args = 1 to 3, default = negative),
      "decr"         -> Spec(),
      "decrby"       -> Spec(args = 1),
      "get"          -> Spec(default = nil),
      "getbit"       -> Spec(args = 1, zero),
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
      "_hrenamed"    -> Spec(args = 1, default = nil),
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
      "_lrenamed"    -> Spec(args = 1, default = nil),
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
      "_srenamed"    -> Spec(args = 1, default = nil),
      "sadd"         -> Spec(args = 1 to many),
      "scard"        -> Spec(default = zero),
      "sdiff"        -> Spec(args = 0 to many, default = seq),
      "sdiffstore"   -> Spec(args = 1 to many, default = zero),
      "sinter"       -> Spec(args = 0 to many, default = seq),
      "sinterstore"  -> Spec(args = 1 to many, default = zero),
      "sismember"    -> Spec(args = 1, default = zero),
      "smembers"     -> Spec(default = seq),
      "smove"        -> Spec(args = 2, default = error),
      "spop"         -> Spec(default = nil),
      "srandmember"  -> Spec(args = 0 to 1, default = nil),
      "srem"         -> Spec(args = 1 to many, default = zero),
      "sscan"        -> Spec(args = 1 to 3, default = scan),
      "sunion"       -> Spec(args = 0 to many, default = seq),
      "sunionstore"  -> Spec(args = 1 to many, default = zero)
    ),

    "keys" -> Map(
      "_del"         -> Spec(default = nil),
      "_keys"        -> Spec(args = 0 to 1),
      "exists"       -> Spec(),
      "expire"       -> Spec(args = 2),
      "expireat"     -> Spec(args = 2),
      "persist"      -> Spec(args = 1),
      "pexpire"      -> Spec(args = 2),
      "pexpireat"    -> Spec(args = 2),
      "pttl"         -> Spec(args = 1),
      "rename"       -> Spec(args = 1, default = error),
      "renamenx"     -> Spec(args = 1, default = error),
      "sort"         -> Spec(args = 1 to many, default = seq),
      "ttl"          -> Spec(args = 1),
      "type"         -> Spec(args = 1)
    ),

    "client" -> Map(
      "del"          -> Spec(args = 1 to many),
      "keys"         -> Spec(args = 1),
      "scan"         -> Spec(args = 1 to 3),
      "randomkey"    -> Spec(),
      "mget"         -> Spec(args = 1 to many),
      "mset"         -> Spec(args = evens),
      "msetnx"       -> Spec(args = evens)
    )

  )

  def get(command: String) =
    specs.find(_._2.contains(command)).getOrElse(("", Map[String, Spec]()))

  def default(command: String, args: Seq[String]) =
    get(command)._2(command).default(args)

  def nodeType(command: String) = get(command)._1

  def argsInRange(command: String, args: Seq[String]) =
    get(command)._2(command).args match {
      case fixed: Int => args.size == fixed
      case range: Range => range.contains(args.size)
    }

}

case class Payload(input: Seq[Any] = Seq(), destination: Option[ActorRef] = None) {

  // TODO: read redis protocol
  val command = if (input.size > 0) input(0).toString.toLowerCase else ""
  val nodeType = if (command != "") Commands.nodeType(command) else ""
  val forKeyNode = nodeType == "keys"
  val forClientNode = nodeType == "client"
  val key = if (input.size > 1 && !forClientNode) input(1).toString else ""
  val args = input.slice(if (forClientNode) 1 else 2, input.size).map(_.toString)

  def argPairs = (0 to args.size - 2 by 2).map {i => (args(i), args(i + 1))}

  def deliver(response: Any) = {
    destination.foreach {dest => if (response != ()) dest ! Response(key, response)}
  }

}

case class Unrouted(payload: Payload) extends ConsistentHashable {
  override def consistentHashKey: Any = payload.key
}

case class Response(key: String, value: Any)

case class PayloadError(message: String) extends Exception(message)

abstract class BaseActor extends Actor with ActorLogging {

  def receiver: Receive

  def receive = LoggingReceive(receiver)

  def route(input: Seq[Any] = Seq(), destination: Option[ActorRef] = None) = {
    val payload = Payload(input, destination)
    if (payload.forClientNode) {
      destination.foreach {dest => dest ! payload}
    } else if (payload.nodeType == "") {
      throw PayloadError("Unknown command")
    } else if (payload.key == "") {
      throw PayloadError("No key specified")
    } else if (!Commands.argsInRange(payload.command, payload.args)) {
      throw PayloadError("Invalid number of args")
    } else {
      context.system.actorSelection("/user/keys") ! Unrouted(payload)
    }
  }

}

abstract class Node extends BaseActor {

  implicit var payload = Payload()

  type Run = PartialFunction[String, Any]

  def run: Run

  def args = payload.args

  def argPairs = payload.argPairs

  def randKey = Random.alphanumeric.take(8).mkString

  def receiver = {
    case "del" => context stop self
    case p: Payload =>
      payload = p
      payload.deliver(Try(run(p.command)) match {
        case Success(response) => response
        case Failure(e) => log.error(s"$e"); "error"
      })
  }

  def aggregate: Unit = {
    context.actorOf(Props(payload.command match {
      case "del"       => classOf[AggregateDel]
      case "mget"      => classOf[AggregateMget]
      case "msetnx"    => classOf[AggregateMsetnx]
      case "keys"      => classOf[AggregateKeys]
      case "randomkey" => classOf[AggregateRandomKey]
    }, payload), s"aggregate-${randKey}")
  }

  def pattern(values: Iterable[String], pattern: String) = {
    val regex = ("^" + args(1).map {
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

  def renamed(to: String, value: Any) = {
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
    case "_renamed"    => renamed("set", value)
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

class BaseHashNode[T] extends Node {

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
    case "_hrenamed"    => renamed("hmset", flatten)
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
    if (value.size == 0) {
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
    case "_lrenamed"  => renamed("lpush", value)
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

class SetNode extends Node {

  var value = Set[String]()

  // broken - no routing
  def others(keys: Seq[String]) = {
    val timeout_ = 2 seconds
    implicit val timeout: Timeout = timeout_
    val futures = Future.traverse(keys.toList) {key =>
      context.system.actorSelection(s"/user/$key") ? Payload(Seq("smembers", key))
    }
    Await.result(futures, timeout_).asInstanceOf[Seq[Response]].map {response: Response =>
      response.value.asInstanceOf[Set[String]]
    }
  }

  def run = {
    case "_srenamed"   => renamed("smembers", value)
    case "sadd"        => val x = (args.toSet &~ value).size; value ++= args; x
    case "srem"        => val x = (args.toSet & value).size; value --= args; x
    case "scard"       => value.size
    case "sismember"   => value.contains(args(0))
    case "smembers"    => value
    case "srandmember" => value.toSeq(Random.nextInt(value.size))
    case "spop"        => val x = run("srandmember"); value -= x.toString; x
    case "sdiff"       => others(args).fold(value)(_ &~ _)
    case "sinter"      => others(args).fold(value)(_ & _)
    case "sunion"      => others(args).fold(value)(_ | _)
    case "sdiffstore"  => value = others(args).reduce(_ &~ _); run("scard")
    case "sinterstore" => value = others(args).reduce(_ & _); run("scard")
    case "sunionstore" => value = others(args).reduce(_ | _); run("scard")
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
          case "string" => "_renamed"
          case "hash"   => "_hrenamed"
          case "list"   => "_lrenamed"
          case "set"    => "_srenamed"
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
      val cantExist = payload.command == "lpushx" || payload.command == "rpushx"
      val mustExist = payload.command == "setnx"
      val default = Commands.default(payload.command, payload.args)
      val invalid = exists(payload.key) && value(payload.key).nodeType != payload.nodeType
      if (payload.forKeyNode) {
        self ! payload
      } else if (invalid) {
        payload.deliver(s"Invalid command ${payload.command} for ${value(payload.key).nodeType}")
      } else if (!exists(payload.key) && default != ()) {
        payload.deliver(default)
      } else if (exists(payload.key) && !cantExist) {
        value(payload.key).node ! payload
      } else if (!exists(payload.key) && !mustExist) {
        val node = context.actorOf(payload.nodeType match {
          case "string" => Props[StringNode]
          case "hash"   => Props[HashNode]
          case "list"   => Props[ListNode]
          case "set"    => Props[SetNode]
        }, s"${payload.key}-${randKey}")
        value(payload.key) = new NodeEntry(node, payload.nodeType)
        node ! payload
      } else payload.deliver(0)
  }: Receive) orElse super.receiver

}

abstract class Aggregator(command: String, payload: Payload) extends BaseActor {

  val responses = MutableMap[String, Any]()
  def complete: Any
  def keys = payload.args

  keys.foreach {key => route(Seq(command, key), Some(self))}

  def receiver = {
    case Response(key, value) =>
      responses(key) = value
      if (responses.size == keys.size) {
        payload.deliver(complete)
        context stop self
      }
  }

}

class AggregateMget(payload: Payload) extends Aggregator("get", payload) {
  override def complete = keys.map((key: String) => responses(key))
}

class AggregateDel(payload: Payload) extends Aggregator("_del", payload) {
  override def complete = responses.filter((arg: (String, Any)) => arg._2 == true).size
}

class AggregateMsetnx(payload: Payload) extends Aggregator("exists", payload) {
  override def keys = payload.argPairs.map(_._1)
  override def complete = {
    val x = responses.filter((arg: (String, Any)) => arg._2 == true).isEmpty
    if (x) payload.argPairs.foreach {args => route(Seq("set", args._1, args._2))}
    x
  }
}

class AggregateKeys(payload: Payload) extends BaseActor {

  val responses = ArrayBuffer[Iterable[String]]()
  var keyNodes = context.system.settings.config.getInt("curiodb.keynodes")

  def complete = responses.reduce(_ ++ _)

  val args = if (payload.key != "") Seq("_keys", payload.key) else Seq("_keys")

  context.system.actorSelection("/user/keys") ! Broadcast(Payload(args, Some(self)))

  def receiver = {
    case Response(_, value) =>
      responses += value.asInstanceOf[Iterable[String]]
      if (responses.size == keyNodes) {
        payload.deliver(complete)
        context stop self
      }
  }

}

class AggregateRandomKey(payload: Payload) extends AggregateKeys(payload: Payload) {
  override def complete = {
    val keys = super.complete.toSeq
    if (keys.size > 0) Seq(keys(Random.nextInt(keys.size))) else Seq()
  }
}

class ClientNode extends Node {

  val buffer = new StringBuilder()
  var client: Option[ActorRef] = None

  def run = {
    case "mset"      => argPairs.foreach {args => route(Seq("set", args._1, args._2))}; "OK"
    case "msetnx"    => aggregate
    case "mget"      => aggregate
    case "del"       => aggregate
    case "keys"      => aggregate
    case "randomkey" => aggregate
  }

  def reply(data: String) = client.foreach {client =>
    client ! Tcp.Write(ByteString(data + "\n"))
  }

  override def receiver = ({
    case Tcp.Received(data) =>
      val received = data.utf8String
      buffer.append(received)
      if (received.endsWith("\n")) {
        client = Some(sender())
        Try(route(buffer.stripLineEnd.split(' '), Some(self))) recover {
          case error: PayloadError => reply(error.message)
        }
        buffer.clear()
      }
    case Tcp.PeerClosed => context stop self
    case Response(_, value) =>
      reply(value match {
        case x: Iterable[Any] => x.mkString("\n")
        case x: Boolean => if (x) "1" else "0"
        case x => x.toString
      })
  }: Receive) orElse super.receiver

}

class Server(host: String, port: Int) extends BaseActor {

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
