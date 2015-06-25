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
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.math.{min, max}
import scala.util.{Success, Failure, Random, Try}

/**
 * Loads all of the command definitions from commands.conf into a
 * structure (maps of maps of maps) that we can use to query the
 * various bits of behaviour for a command.
 */
object Commands {

  /**
   * A CommandSet holds all of the commands for a particular node type.
   * Each key is a command name, mapped to its properties.
   */
  type CommandSet = mutable.Map[String, mutable.Map[String, String]]

  /**
   * Node types mapped to CommandSet instances, which we populate from
   * the commands.conf file.
   */
  val commands = mutable.Map[String, CommandSet]()

  ConfigFactory.load("commands.conf").getConfig("commands").entrySet.foreach {entry =>
    val parts = entry.getKey.replace("\"", "").split('.')
    commands.getOrElseUpdate(parts(0), mutable.Map[String, mutable.Map[String, String]]())
    commands(parts(0)).getOrElseUpdate(parts(1), mutable.Map[String, String]())
    commands(parts(0))(parts(1))(parts(2)) = entry.getValue.unwrapped.toString
  }

  /**
   * Returns the CommandSet for a given command.
   */
  def commandSet(command: String): Option[(String, CommandSet)] =
    commands.find(_._2.contains(command))

  /**
   * Returns the type of node for a given command.
   */
  def nodeType(command: String): String =
    commandSet(command).map(_._1).getOrElse("")

  /**
   * Returns an attribute stored for the command.
   */
  def attr(command: String, name: String, default: String): String = commandSet(command).map(_._2) match {
    case Some(set) => set.getOrElse(command, Map[String, String]()).getOrElse(name, default)
    case None => default
  }

  /**
   * Returns if the command's first arg is a node's key.
   */
  def keyed(command: String): Boolean =
    attr(command, "keyed", "true") != "false"

  /**
   * Returns if the command writes its node's value.
   */
  def writes(command: String): Boolean =
    attr(command, "writes", "false") == "true" || overwrites(command)

  /**
   * Returns if the command can overwrite an existing node with the
   * same key, even with a different type.
   */
  def overwrites(command: String): Boolean =
    attr(command, "overwrites", "false") == "true"

  /**
   * Returns the default value for a command that should be given when
   * the node doesn't exist. Each of the types here are referenced by
   * the "default" param in the commands.conf file.
   */
  def default(command: String, args: Seq[String]): Any =
    attr(command, "default", "none") match {
      case "string" => ""
      case "ok"     => SimpleReply()
      case "error"  => ErrorReply("no such key")
      case "nil"    => null
      case "zero"   => 0
      case "neg1"   => -1
      case "neg2"   => -2
      case "seq"    => Seq()
      case "scan"   => Seq("0", "")
      case "nils"   => args.map(_ => null)
      case "zeros"  => args.map(_ => 0)
      case "none"   => ()
    }

  /**
   * Returns whether the given args are within range for a command.
   * Defined in the "args" param in the command.conf file. Can be
   * a single integer for a fixed number of args, or a range defined
   * by the format "X-Y" where X is the min number of args allowed,
   * and Y is the max. Some other forms are supported, such as "many"
   * which means no real upper limit (actually Int.MaxValue), and
   * "pairs" which means args are pairs, and should be an even number.
   */
  def argsInRange(command: String, args: Seq[String]): Boolean = {
    val parts = attr(command, "args", "0").split('-')
    val pairs = parts(0) == "pairs"
    if (parts.size == 1 && !pairs) {
      args.size == parts(0).toInt
    } else {
      val start = if (pairs) 2 else parts(0).toInt
      val stop = if (pairs || parts(1) == "many") Int.MaxValue - 1 else parts(1).toInt
      val step = if (pairs) 2 else 1
      (start to stop by step).contains(args.size)
    }
  }

}

/**
 * Main payload for a command - stores its name, type, key, args.
 */
case class Payload(input: Seq[Any] = Seq(), db: String = "0", destination: Option[ActorRef] = None) {
  val command = if (input.size > 0) input(0).toString.toLowerCase else ""
  val nodeType = if (command != "") Commands.nodeType(command) else ""
  val key = if (nodeType != "" && input.size > 1 && Commands.keyed(command)) input(1).toString else ""
  val args = input.drop(if (key == "") 1 else 2).map(_.toString)
  lazy val argsPaired = (0 to args.size - 2 by 2).map {i => (args(i), args(i + 1))}
  lazy val argsUpper = args.map(_.toUpperCase)
}

/**
 * Payload wrapper for routing it to its correct KeyNode.
 */
case class Routable(payload: Payload) extends ConsistentHashable {
  override def consistentHashKey: Any = payload.key
}

/**
 * Response a Node will return to a ClientNode after command is run.
 * Primarily used in PayloadProcessing.respond below.
 */
case class Response(key: String, value: Any)

/**
 * Trait containing behavior for dealing with a Payload - it contains
 * a payload variable that the class should initially set upon
 * receiving it via actor's receive method. Used by anything that a
 * Payload passes through, such as all Node and Aggregate actors.
 */
trait PayloadProcessing extends Actor {

  /**
   * The current payload - normally will be set when a Payload
   * arrives via the actor's receive method.
   */
  var payload = Payload()

  /**
   * Signature for the partial function CommandRunner that actually
   * handles each command. Every Node must implement the "run" method
   * of this type (this requirement is actually codified in the
   * base Node class). It takes a case statement mapping command
   * names to the code that handles them - typically handled
   * inline, or via a method if more complex. The result will
   * then be sent back to the calling ClientNode, and converted
   * into a Redis response before being sent back to the client
   * socket. One special case is the handler returning Unit,
   * in which case no response is sent back - in this case it's
   * up to the handling code to manually send a response back
   * to the client node. A common example of this is all of the
   * aggregation commands, which need to coordinate with
   * multiple nodes before calculating a response. But the bulk
   * of commands are simple one-liners directly returning a response.
   */
  type CommandRunner = PartialFunction[String, Any]

  // These are just shortcuts to the current payload.
  def args: Seq[String] = payload.args
  def argsPaired: Seq[(String, String)] = payload.argsPaired
  def argsUpper: Seq[String] = payload.argsUpper

  /**
   * Sends an unrouted Payload to one or more KeyNode actors, either by
   * routing by key, or broadcasting to all.
   */
  def route(
      input: Seq[Any] = Seq(),
      destination: Option[ActorRef] = None,
      clientPayload: Option[Payload] = None,
      broadcast: Boolean = false): Unit = {

    // A payload can be pre-constructed (by a ClientNode which does so
    // in order to first validate it), or constructed here with the
    // given set of payload args, which is the common case for Node
    // actors wanting to trigger commands themselves.
    val p = clientPayload match {
      case Some(payload) => payload
      case None => Payload(input, payload.db, destination)
    }

    val keys = context.system.actorSelection("/user/keys")

    if (broadcast) keys ! Broadcast(p) else keys ! Routable(p)

  }

  /**
   * Sends a response (usually the result of a command) back to a
   * payload's destination (usually a ClientNode sending a command).
   */
  def respond(response: Any): Unit =
    if (response != ()) {
      payload.destination.foreach {d => d ! Response(payload.key, response)}
    }

  /**
   * Stops the actor - we define this shortcut to give subclassing traits
   * the chance to override it and inject extra shutdown behavior that
   * concrete actors need not know about.
   */
  def stop: Unit = context stop self

  // Following are a handful of utility functions - they don't really
  // deal directly with general payload processing, but have some
  // minor dependencies on payload args and such, and don't really
  // belong anywhere else in particular, so here they are.

  /**
   * Shortcut for creating Aggregate actors.
   */
  def aggregate(props: Props): Unit =
    context.actorOf(props, s"aggregate-${payload.command}-${randomString()}") ! payload

  /**
   * Utility for selecting a random item.
   */
  def randomItem(iterable: Iterable[String]): String =
    if (iterable.isEmpty) "" else iterable.toSeq(Random.nextInt(iterable.size))

  /**
   * Utility for generating a random string.
   */
  def randomString(length: Int = 5): String = Random.alphanumeric.take(length).mkString

  /**
   * Utility for glob-style filtering.
   */
  def pattern(values: Iterable[String], pattern: String): Iterable[String] = {
    val regex = ("^" + pattern.map {
      case '.'|'('|')'|'+'|'|'|'^'|'$'|'@'|'%'|'\\' => "\\" + _
      case '*' => ".*"
      case '?' => "."
      case c => c
    }.mkString("") + "$").r
    values.filter(regex.pattern.matcher(_).matches)
  }

  /**
   * Utility for scan-style commands, namely SCAN/SSCAN/HSCAN/ZSCAN.
   */
  def scan(values: Iterable[String]): Seq[String] = {
    val count = if (args.size >= 3) args(2).toInt else 10
    val start = if (args.size >= 1) args(0).toInt else 0
    val end = start + count
    val filtered = if (args.size >= 2) pattern(values, args(1)) else values
    val next = if (end < filtered.size) end else 0
    Seq(next.toString) ++ filtered.slice(start, end)
  }

  /**
   * Utility for handling boundary args that wrap around ends of
   * sequences and count backwards when negative.
   */
  def bounds(from: Int, to: Int, size: Int): (Int, Int) =
    (if (from < 0) size + from else from, if (to < 0) size + to else to)

  /**
   * Utility for slicing sequences, for commands such as SLICE,
   * GETRANGE, LRANGE, etc.
   */
  def slice[T](value: Seq[T]): Seq[T] = {
    val (from, to) = bounds(args(0).toInt, args(1).toInt, value.size)
    value.slice(from, to + 1)
  }

}

// Following are some reply classes that get sent from a Node back
// to a ClientNode, in response to a command - they're defined so
// that we can handle them explicitly when building a response
// in the Redis protocol, where they have specific notation.

/**
 * An error response, as per Redis protocol.
 */
case class ErrorReply(message: String = "syntax error", prefix: String = "ERR")

/**
 * A simple response, as per Redis protocol.
 */
case class SimpleReply(message: String = "OK")

// Following are some messages we send *to* nodes.

/**
 * Message that a Node actor sends to itself when it's ready to save
 * its value to disk. See Node.save() for more detail.
 */
case object Persist extends ControlMessage

/**
 * Message that shuts a Node actor down and delete its value from disk.
 */
case object Delete extends ControlMessage

/**
 * Message that tells an actor to shut down, but not delete its value
 * from disk - the KeyNode holding its key still contains a reference
 * to it, and a new actor will be started for it the next time a
 * command is received for its key. See KeyNode for more details.
 */
case object Sleep extends ControlMessage

/**
 * Node is the base actor class that all concrete node types subclass
 * Specifically, there is a concrete Node class for each data type
 * namely: StringNode for strings, HashNode for hashes, etc. There are
 * also some special Node types, such as KeyNode which manages the key
 * space for regular Node actors, and ClientNode which manages a single
 * client connection.
 *
 * The base class here defines the core features of a Node actor,
 * namely:
 *
 *  - Its data value, typed by the type parameter that concrete
 *    subclasses must define.
 *  - Execution of the node's CommandRunner each time a Payload is
 *    received.
 *  - Optionally persisting the node's value to disk (snapshotting)
 *    after a command has been handled (point 2 above), cleaning up
 *    older snapshots, and restoring from a snapshot on startup.
 *
 * Persistence warrants some discussion: we use akka-persistence, but
 * not completely, as event-sourcing is not used, and we rely entirely
 * on its snapshotting feature, only ever keeping a single snapshot.
 * This was basically the easiest way to get persistence working.
 * We always store a reference to the last snapshot's meta-data (the
 * lastSnapshot var) so that we can delete old snapshots whenever a
 * new one is saved. As for saving, this is controlled via the config
 * var curiodb.persist-after which is the number of milliseconds after
 * a command runs that writes the node's value (described as writable
 * in the commands.conf file). When one of these commands runs, we call
 * save, which will schedule a Persist message back to the node itself.
 * This is based on the assumption that there's no guarantee an
 * actor's recieve and scheduler won't both execute at the exact same
 * time, so we have everything run through receive. The persisting
 * var stores whether persisting has been scheduled, to allow extra
 * save calls to do nothing when persisting has already been scheduled.
 */
abstract class Node[T] extends PersistentActor with PayloadProcessing with ActorLogging {

  /**
   * Actual data value for the node.
   */
  var value: T

  /**
   * The most recently saved snapshot. We store it on save and recover,
   * so that we can delete it (and any earlier snapshots) each time we
   * successfully save a new snapshot.
   */
  var lastSnapshot: Option[SnapshotMetadata] = None

  /**
   * Boolean representing whether we've scheduled an internal
   * Persist message, so that it only occurs once at a time
   * according to the milliseconds configured by the
   * curiodb.persist-after setting.
   */
  var persisting: Boolean = false

  /**
   * Stores the milliseconds configured by curiodb.persist-after.
   */
  val persistAfter = context.system.settings.config.getInt("curiodb.persist-after")

  /**
   * Abstract definition of each Node actor's CommandRunner that must
   * be implemented.
   */
  def run: CommandRunner

  def persistenceId: String = self.path.name

  /**
   * As discussed above, saves a snapshot. If curiodb.persist-after is
   * zero or less (which means every change must cause a snapshot to be
   * saved), then just save a snapshot. Otherwise schedule the internal
   * Persist message to save a snapshot, if none has already been
   * scheduled, according to the value of the persisting var.
   */
  def save: Unit = {
    if (persistAfter == 0) {
      saveSnapshot(value)
    } else if (persistAfter > 0 && !persisting) {
      persisting = true
      context.system.scheduler.scheduleOnce(persistAfter milliseconds) {self ! Persist}
    }
  }

  /**
   * Deletes old snapshots after a new one is saved.
   */
  def deleteOldSnapshots(stopping: Boolean = false): Unit =
    if (persistAfter >= 0) {
      lastSnapshot.foreach {meta =>
        val criteria = if (stopping) SnapshotSelectionCriteria()
          else SnapshotSelectionCriteria(meta.sequenceNr, meta.timestamp - 1)
        deleteSnapshots(criteria)
      }
    }

  /**
   * Restore the Node actor's value on startup.
   */
  override def receiveRecover: Receive = {
    case SnapshotOffer(meta, snapshot) =>
      lastSnapshot = Some(meta)
      value = snapshot.asInstanceOf[T]
  }

  /**
   * Main Receive handler - deals with starting/stopping/persisting,
   * and receiving Payload instances, then running the Node actor's
   * CommandRunner.
   */
  def receiveCommand: Receive = {
    case SaveSnapshotSuccess(meta) => lastSnapshot = Some(meta); deleteOldSnapshots()
    case SaveSnapshotFailure(_, e) => log.error(e, "Snapshot write failed")
    case Persist    => persisting = false; saveSnapshot(value)
    case Delete     => deleteOldSnapshots(stopping = true); stop
    case Sleep      => stop
    case p: Payload =>
      payload = p
      respond(Try(run(payload.command)) match {
        case Success(response) => if (Commands.writes(payload.command)) save; response
        case Failure(e) => log.error(e, s"Error running: $payload"); ErrorReply
      })
  }

  override def receive: Receive = LoggingReceive(super.receive)

  /**
   * Handles the RENAME/RENAMENX commands for all node types. Each
   * Node subclass is responsible for converting its value to something
   * that can be used by the relevant command given - this command
   * is typically implemented (and named) differently per node, and
   * knows how to accept fromValue. Renaming is basically a delete,
   * then create a new node, so we first delete ourselves (via the
   * internal _DEL command), then pass the fromValue arg with the given
   * command to  what will be a newly created Node.
   */
  def rename(fromValue: Any, toCommand: String): Unit =
    if (payload.key != args(0)) {
      route(Seq("_del", payload.key))
      route(Seq(toCommand, args(0)) ++ (fromValue match {
        case x: Iterable[Any] => x
        case x => Seq(x)
      }))
    }

  /**
   * Handles the SORT command. Defined here since it can be run against
   * multiple Node types, namely ListNode, SetNode and SortedSetNode.
   */
  def sort(values: Iterable[String]): Any = {
    // TODO: BY/GET support.
    var sorted = if (argsUpper.contains("ALPHA")) values.toSeq.sorted else values.toSeq.sortBy(_.toFloat)
    if (argsUpper.contains("DESC")) sorted = sorted.reverse
    val limit = argsUpper.indexOf("LIMIT")
    if (limit > -1) sorted = sorted.slice(args(limit + 1).toInt, args(limit + 2).toInt)
    val store = argsUpper.indexOf("STORE")
    if (store > -1) {
      route(Seq("_lstore", args(store + 1)) ++ sorted)
      sorted.size
    } else sorted
  }

}

/**
 * String commands. Note that unlike Redis, bitmap and HLL commands
 * have their own data types and corresponding classes, namely
 * BitmapNode and HyperLogLogNode.
 */
class StringNode extends Node[String] {

  var value = ""

  /**
   * Returns a default "0" string if the current value is an empty
   * string, which mimics the behaviour of Redis for integer/float
   * commands (INCR, DECR, etc) on string values.
   */
  def valueOrZero: String = if (value == "") "0" else value

  /**
   * Handles calling the EXPIRE/PEXPIRE commands for the SETEX/PSETEX
   * commands.
   */
  def expire(command: String): Unit = route(Seq(command, payload.key, args(1)))

  def run: CommandRunner = {
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

/**
 * Bitmap commands. For simplicity the bitmap commands have their own
 * Node type which uses a BitSet for its underlying value.
 */
class BitmapNode extends Node[mutable.BitSet] {

  var value = mutable.BitSet()

  def last: Int = value.lastOption.getOrElse(0)

  def bitPos: Int = {
    var x = value
    if (args.size > 1) {
      val (from, to) = bounds(args(1).toInt, if (args.size == 3) args(2).toInt else last, last + 1)
      x = x.range(from, to + 1)
    }
    if (args(0) == "1")
      x.headOption.getOrElse(-1)
    else
      (0 to x.lastOption.getOrElse(-1)).collectFirst({
        case i: Int if !x.contains(i) => i
      }).getOrElse(if (args.size > 1 && value.size > 1) -1 else 0)
  }

  def run: CommandRunner = {
    case "_rename"  => rename(value, "_bstore")
    case "_bstore"  => value.clear; value ++= args.map(_.toInt); last / 8 + (if (value.isEmpty) 0 else 1)
    case "_bget"    => value
    case "bitcount" => value.size
    case "getbit"   => value(args(0).toInt)
    case "setbit"   => val x = run("getbit"); value(args(0).toInt) = args(1) == "1"; x
    case "bitpos"   => bitPos
  }

}

/**
 * HyperLogLog commands. For simplicity the hyperloglog commands have
 * their own Node type which uses a net.agkn.hll.HLL for its underlying
 * value.
 */
class HyperLogLogNode extends Node[HLL] {

  var value = new HLL(
    context.system.settings.config.getInt("curiodb.hyperloglog.register-log"),
    context.system.settings.config.getInt("curiodb.hyperloglog.register-width")
  )

  /**
   * The HLL class only supports longs, so we just use hashcodes of the
   * strings we want to add.
   */
  def add: Int = {
    val x = value.cardinality
    args.foreach {x => value.addRaw(x.hashCode.toLong)}
    if (x == value.cardinality) 0 else 1
  }

  def run: CommandRunner = {
    case "_rename"  => rename(value.toBytes.map(_.toString), "_pfstore")
    case "_pfcount" => value.cardinality.toInt
    case "_pfstore" => value.clear(); value = HLL.fromBytes(args.map(_.toByte).toArray); SimpleReply()
    case "_pfget"   => value
    case "pfadd"    => add
  }

}

/**
 * Hash commands.
 */
class HashNode extends Node[mutable.Map[String, String]] {

  var value = mutable.Map[String, String]()

  /**
   * Shortcut that sets a value, given that the hash key is the first
   * payload arg.
   */
  def set(arg: Any): String = {val x = arg.toString; value(args(0)) = x; x}

  override def run: CommandRunner = {
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

/**
 * ListNode supports blocking commands (BLPOP, BRPOP, etc) where
 * if the list is empty, no immediate response is sent to the client,
 * and when the next command is run that adds to the list, we
 * essentially retry the original blocking command, and if we can
 * perform it (eg pop), we then send the requested value back to
 * the client. This is implemented by storing an ordered set of
 * Payload instances that are blocked, and iterating them each time the
 * next command is run. Timeouts are also supported via the scheduler,
 * which simply removes the blocked Payload from the setm and sends
 * a null response back to the ClientNode.
 */
class ListNode extends Node[mutable.ArrayBuffer[String]] {

  var value = mutable.ArrayBuffer[String]()

  /**
   * Set of blocked Payload instances awaiting a response.
   */
  var blocked = mutable.LinkedHashSet[Payload]()

  /**
   * Called on each of the blocking commands, storing the payload
   * in the blocket set if the list is currently empty, and scheduling
   * its timeout. Otherwise if the list have items, we just run the
   * non-blocking version of the command.
   */
  def block: Any = {
    if (value.isEmpty) {
      blocked += payload
      context.system.scheduler.scheduleOnce(args.last.toInt seconds) {
        blocked -= payload
        respond(null)
      }
      ()
    } else run(payload.command.tail)  // Run the non-blocking version.
  }

  /**
   * Called each time the Node actor's CommandRunner runs - if the
   * Node actor's list has values and we have blocked payloads, iterate
   * through the blocked Payload instances and replay their commands.
   */
  def unblock(result: Any): Any = {
    while (value.size > 0 && blocked.size > 0) {
      // Set the node's current payload to the blocked payload, so
      // that the running command has access to the correct payload.
      payload = blocked.head
      blocked -= payload
      respond(run(payload.command.tail))
    }
    result
  }

  /**
   * LINSERT command that handles BEFORE/AFTER args.
   */
  def insert: Int = {
    val i = value.indexOf(args(1)) + (if (args(0) == "AFTER") 1 else 0)
    if (i >= 0) {
      value.insert(i, args(2))
      value.size
    } else -1
  }

  def run: CommandRunner = ({
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
    case "linsert"    => insert
  }: CommandRunner) andThen unblock

}

/**
 * Set commands.
 */
class SetNode extends Node[mutable.Set[String]] {

  var value = mutable.Set[String]()

  def run: CommandRunner = {
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

/**
 * A SortedSetEntry is stored for each value in a SortedSetNode. It's
 * essentially a score/key pair which is Ordered.
 */
case class SortedSetEntry(score: Int, key: String = "")(implicit ordering: Ordering[(Int, String)])
    extends Ordered[SortedSetEntry] {
  def compare(that: SortedSetEntry): Int =
    ordering.compare((this.score, this.key), (that.score, that.key))
}

/**
 * Sorted sets are implemented slightly differently than in Redis. We
 * still use two data structures, but different ones than Redis does.
 * We use IndexedTreeMap to map keys to scores, and IndexedTreeSet to
 * map scores to keys. Both these are provided by the same third-party
 * library.
 *
 * Like Redis, IndexedTreeMap is a map that maintains key order, but
 * also (unlike Java and Scala collections) supports indexing
 * (for range queries).
 *
 * For scores mapped to keys, unlike Redis' skip list, we use
 * IndexedTreeSet which is an ordered set that also supports indexing.
 * We actually store both score and key (see SortedSetEntry above),
 * which provides ordering by score then by key.
 */
class SortedSetNode extends Node[(IndexedTreeMap[String, Int], IndexedTreeSet[SortedSetEntry])] {

  /**
   * Actual value is a two item tuple, map of keys to scores, and set
   * of score/key entries.
   */
  var value = (new IndexedTreeMap[String, Int](), new IndexedTreeSet[SortedSetEntry]())

  /**
   * Shortcut to mapping of keys to scores.
   */
  def keys: IndexedTreeMap[String, Int] = value._1

  /**
   * Shortcut to set of score/key entries.
   */
  def scores: IndexedTreeSet[SortedSetEntry] = value._2

  /**
   * Adds key/score to both structures.
   */
  def add(score: Int, key: String): Boolean = {
    val exists = remove(key)
    keys.put(key, score)
    scores.add(SortedSetEntry(score, key))
    !exists
  }

  /**
   * Removes key/score from both structures.
   */
  def remove(key: String): Boolean = {
    val exists = keys.containsKey(key)
    if (exists) {
      scores.remove(SortedSetEntry(keys.get(key), key))
      keys.remove(key)
    }
    exists
  }

  /**
   * Increments score for a key by retrieving it, removing it and
   * re-adding it.
   */
  def increment(key: String, by: Int): Int = {
    val score = (if (keys.containsKey(key)) keys.get(key) else 0) + by
    remove(key)
    add(score, key)
    score
  }

  /**
   * Index of key in the score set - we first loook up the score
   * to get the correct entry in the set.
   */
  def rank(key: String, reverse: Boolean = false): Int = {
    val index = scores.entryIndex(SortedSetEntry(keys.get(key), key))
    if (reverse) keys.size - index else index
  }

  /**
   * Supports range commands on the score/key set, for use by the
   * rangeByIndex and rangeByScore methods below. Each of these is
   * responsible for determining the from/to pair SortedSetEntry
   * instances that defines the range. Also handles the WITHSCORES arg.
   */
  def range(from: SortedSetEntry, to: SortedSetEntry, reverse: Boolean): Seq[String] = {
    if (from.score > to.score) return Seq()
    var result = scores.subSet(from, true, to, true).toSeq
    result = limit[SortedSetEntry](if (reverse) result.reverse else result)
    if (argsUpper.contains("WITHSCORES"))
      result.flatMap(x => Seq(x.key, x.score.toString))
    else
      result.map(_.key)
  }

  /**
   * Queries the the score/key set for a range by index.
   */
  def rangeByIndex(from: String, to: String, reverse: Boolean = false): Seq[String] = {
    var (fromIndex, toIndex) = bounds(from.toInt, to.toInt, keys.size)
    if (reverse) {
      fromIndex = keys.size - fromIndex - 1
      toIndex = keys.size - toIndex - 1
    }
    range(scores.exact(fromIndex), scores.exact(toIndex), reverse)
  }

  /**
   * Queries the the score/key set for a range by score. As per Redis,
   * supports the infinite/inclusive/exclusive syntax.
   */
  def rangeByScore(from: String, to: String, reverse: Boolean = false): Seq[String] = {
    def parse(arg: String, dir: Int) = arg match {
      case "-inf" => if (scores.isEmpty) 0 else scores.first().score
      case "+inf" => if (scores.isEmpty) 0 else scores.last().score
      case arg if arg.startsWith("(") => arg.toInt + dir
      case _ => arg.toInt
    }
    range(SortedSetEntry(parse(from, 1)), SortedSetEntry(parse(to, -1) + 1), reverse)
  }

  /**
   * Queries the the key/map for a range by key. As per Redis,
   * supports the infinite/inclusive/exclusive syntax.
   */
  def rangeByKey(from: String, to: String, reverse: Boolean = false): Seq[String] = {
    def parse(arg: String) = arg match {
      // For negatively infinite, an empty key should always be first.
      case "-" => ""
      // For infinite, the greatest key plus a char should always be last.
      case "+" => if (keys.size == 0) "" else keys.lastKey() + "x"
      case arg if "[(".indexOf(arg.head) > -1 => arg.tail
    }
    val (fromKey, toKey) = (parse(from), parse(to))
    if (fromKey > toKey) return Seq()
    val result = keys.subMap(fromKey, from.head == '[', toKey, to.head == '[').toSeq
    limit[(String, Int)](if (reverse) result.reverse else result).map(_._1)
  }

  /**
   * Optionally handles the LIMIT arg in range commands.
   */
  def limit[T](values: Seq[T]): Seq[T] = {
    val i = argsUpper.indexOf("LIMIT")
    if (i > 1)
      values.slice(args(i + 1).toInt, args(i + 1).toInt + args(i + 2).toInt)
    else
      values
  }

  def run: CommandRunner = {
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

/**
 * Message sent from PubSubServer/KeyNode to a PubSubClient/ClientNode
 * so that it can manage its own channel/pattern subscriptions. See
 * PubSubServer for more detail.
 */
case class PubSubEvent(event: String, channelOrPattern: String)

/**
 * PubSubServer is exclusively part of KeyNode, but defined separately
 * here for clarity. A KeyNode is responsible for managing the keyspace
 * for a subset of nodes, and therefore the same logic applies to pubsub
 * channels. A PubSubServer (KeyNode) stores channel names mapped to
 * ActorRef values for ClientNode actors (which have corresponding
 * PubSubClient traits, similar to the PubSubServer/KeyNode
 * relationship), which represent all client connections, pubsub or
 * otherwise).
 *
 * A significant shortcoming in this design is handling for pattern
 * subscriptions. The problem is that a pattern may match channels
 * that are split across different KeyNode instances. To work around
 * this initially, we actually store *every* pattern subscription
 * on *every* KeyNode. Patterns are stored in the same way as channels,
 * with patterns mapped to ActorRef values for ClientNode actors.
 */
trait PubSubServer extends PayloadProcessing {

  /**
   * Client subscriptions to channels.
   */
  val channels = mutable.Map[String, mutable.Set[ActorRef]]()

  /**
   * Client subscriptions to patterns.
   */
  val patterns = mutable.Map[String, mutable.Set[ActorRef]]()

  /**
   * Handles subscribe and unsubscribe to both channels and patterns.
   * Responsible for omitting PubSubEvent messages back to the
   * ClientNode when a change in subscription occurs.
   */
  def subscribeOrUnsubscribe: Unit = {
    val pattern = payload.command.startsWith("_p")
    val subscriptions = if (pattern) patterns else channels
    val key = if (pattern) args(0) else payload.key
    val subscriber = payload.destination.get
    val subscribing = payload.command.drop(if (pattern) 2 else 1) == "subscribe"
    val updated = if (subscribing)
      subscriptions.getOrElseUpdate(key, mutable.Set[ActorRef]()).add(subscriber)
    else
      !subscriptions.get(key).filter(_.remove(subscriber)).isEmpty
    if (!subscribing && updated && subscriptions(key).isEmpty) subscriptions -= key
    if (updated) subscriber ! PubSubEvent(payload.command.tail, key)
  }

  /**
   * Sends a message that has been receieved (published) from a client,
   * to all matching subscriptions - either channels, or patterns.
   */
  def publish: Int = {
    channels.get(payload.key).map({subscribers =>
      val message = Response(payload.key, Seq("message", payload.key, args(0)))
      subscribers.foreach(_ ! message)
      subscribers.size
    }).sum + patterns.filterKeys(!pattern(Seq(payload.key), _).isEmpty).map({entry =>
      val message = Response(payload.key, Seq("pmessage", entry._1, payload.key, args(0)))
      entry._2.foreach(_ ! message)
      entry._2.size
    }).sum
  }

  def runPubSub: CommandRunner = {
    case "_numsub"       => channels.get(payload.key).map(_.size).sum
    case "_numpat"       => patterns.values.map(_.size).sum
    case "_channels"     => pattern(channels.keys, args(0))
    case "_subscribe"    => subscribeOrUnsubscribe
    case "_unsubscribe"  => subscribeOrUnsubscribe
    case "_psubscribe"   => subscribeOrUnsubscribe
    case "_punsubscribe" => subscribeOrUnsubscribe
    case "publish"       => publish
  }

}

/**
 * A KeyNode manages a subset of keys, and stores these by mapping
 * DB names to keys to nodes, where nodes are represented by a
 * NodeEntry.
 */
@SerialVersionUID(1L)
class NodeEntry(
    val nodeType: String,
    @transient var node: Option[ActorRef] = None,
    @transient var expiry: Option[(Long, Cancellable)] = None,
    @transient var sleep: Option[Cancellable] = None)
  extends Serializable

/**
 * A KeyNode is a special type of Node in the system. It does not
 * represent any key/value provided by a client, but instead is
 * responsible for managing the keyspace for a subset of keys in its
 * value field, which contains a map of DB names, mapped to keys,
 * mapped to nodes, where each node is represented by a NodeEntry.
 * KeyNode takes on the role of a Node, as there are a variety of
 * commands that logically belong to it.
 *
 * When a ClientNode (the other special type of node, responsble
 * for handling a single client) receives a command for a particular
 * key, the command is first routed to the KeyNode responsible for that
 * key. The KeyNode maps each key to an ActorRef for the key's actual
 * node (StringNode, ListNode, etc), having first created the Node
 * actor at some point (this actually occurs when the key/node doesn't
 * exist and, a command is run that doesn't have a default defined, and
 * writes, as per in commands.conf), and forwards each command onto the
 * Node when received. The routing strategy used is configurable via
 * Akka (actor.deployment./keys.router in application.conf),
 * defaulting to the consistent-hashing-pool router.
 *
 * A KeyNode also handles many commands itself that deal with key
 * management, such as deleting, persisting, and expiring keys.
 * Each NodeEntry instance stores a Option(Long/Cancellable) pair
 * which if expiring, contains the timestamp of when the expiry will
 * occur, and the Cancellable task that will actually run, deleting
 * the node. Deletion of a node (either via expiry or the DEL command)
 * simply sends a message to the Node actor which when received,
 * shuts down the actor, and then removes the key and NodeEntry from
 * the keyspace map.
 *
 * Lastly worthy of discussion is a feature that Redis does not
 * provide, virtual memory, which simply allows a Node to persist
 * its value to disk, and shut down after a period of time (defined by
 * the curiodb.sleep-after millisecond value in reference.conf). The
 * difference between this occuring and a Node being deleted, is that
 * the key and NodeEntry is kept in the keyspace map. This is also why
 * the ActorRef for each Node in a NodeEntry is an Option - a value of
 * None indicates a sleeping Node. When a command is run against a key
 * mapped to a sleeping Node, a new Node actor is created, which will
 * read its previous value from disk. The idea here is to allow more
 * data to be stored in the system than can fit in memory.
 */
class KeyNode extends Node[mutable.Map[String, mutable.Map[String, NodeEntry]]] with PubSubServer {

  /**
   * Alias for keys mapped to each NodeEntry.
   */
  type DB = mutable.Map[String, NodeEntry]

  /**
   * The actual value persisted for a KeyNode is a map of db names
   * to DB instances.
   */
  var value = mutable.Map[String, DB]()

  /**
   * Error message sent to ClientNode when a command is issued against
   * an existing key that contains a different type of node than the
   * type that the command belongs to.
   */
  val wrongType = ErrorReply("Operation against a key holding the wrong kind of value", prefix = "WRONGTYPE")
  val sleepAfter = context.system.settings.config.getInt("curiodb.sleep-after")
  val sleepEnabled = sleepAfter > 0

  /**
   * Shortcut for grabbing the String/NodeEntry map (aka DB) for the
   * given DB name.
   */
  def dbFor(name: String): DB =
    value.getOrElseUpdate(name, mutable.Map[String, NodeEntry]())

  /**
   * Shortcut for grabbing the String/NodeEntry map (aka DB) for the
   * current payload.
   */
  def db: DB = dbFor(payload.db)

  /**
   * Cancels expiry on a node when the PERSIST command is run.
   */
  def persist: Int = db(payload.key).expiry match {
    case Some((_, cancellable)) =>
      cancellable.cancel()
      db(payload.key).expiry = None
      1
    case None => 0
  }

  /**
   * Initiates expiry on a node when any of the relevant commands are
   * run, namely EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT.
   */
  def expire(when: Long): Int = {
    persist
    val expires = (when - System.currentTimeMillis).toInt milliseconds
    val cancellable = context.system.scheduler.scheduleOnce(expires) {
      self ! Payload(Seq("_del", payload.key), db = payload.db)
    }
    db(payload.key).expiry = Some((when, cancellable))
    1
  }

  /**
   * Called every time a command is run, only if sleeping is enabled
   * (via the curiodb.sleep-after config value), and initiates the
   * actor's sleeping process, whereby its value is persisted to disk
   * and then shut down. The key's NodeEntry has its ActorRef then
   * set to None to signify a sleeping Node. When a command is next
   * run against the key, the actor for the Node is then recreated,
   * with its value restored from disk.
   */
  def sleep: Unit = {
    val when = sleepAfter milliseconds
    val key = payload.key
    val entry = db(key)
    entry.sleep.foreach(_.cancel())
    entry.sleep = Some(context.system.scheduler.scheduleOnce(when) {
      db.get(key).foreach {entry =>
        entry.node.foreach(_ ! Sleep)
        entry.node = None
      }
    })
  }

  /**
   * Retrieves the milliseconds remaining until expiry occurs for a key
   * when the TTL/PTTL commands are run.
   */
  def ttl: Long = db(payload.key).expiry match {
    case Some((when, _)) => when - System.currentTimeMillis
    case None => -1
  }

  /**
   * Handles the bridge between the external SORT and internal _SORT
   * commands. This is essentially a work around given that we only
   * allow external (client facing) commands to map to a single type
   * of Node.
   */
  def sort: Any = db(payload.key).nodeType match {
    case "list" | "set" | "sortedset" =>
      val sortArgs = Seq("_sort", payload.key) ++ payload.args
      node ! Payload(sortArgs, db = payload.db, destination = payload.destination)
    case _ => wrongType
  }

  /**
   * Validates that the key and command for the current Payload can be
   * run. This boils down to ensuring the command belongs to the type
   * of Node mapped to the key, and that the Node must or musn't exist,
   * given the particular command. Optionally returns an error, or a
   * default value if the Node doesn't exist and a default is defined
   * (as per commands.conf). Otherwise if validation passes, None is
   * returned, indicating the command should be sent to the key's
   * Node.
   */
  def validate: Option[Any] = {
    val exists      = db.contains(payload.key)
    val nodeType    = if (exists) db(payload.key).nodeType else ""
    val invalidType = (nodeType != "" && payload.nodeType != nodeType &&
      payload.nodeType != "keys" && !Commands.overwrites(payload.command))
    val cantExist   = payload.command == "lpushx" || payload.command == "rpushx"
    val mustExist   = payload.command == "setnx"
    val default     = Commands.default(payload.command, payload.args)
    if (invalidType)
      Some(wrongType)
    else if ((exists && cantExist) || (!exists && mustExist))
      Some(0)
    else if (!exists && default != ())
      Some(default)
    else
      None
  }

  /**
   * Shortcut for retrieving the ActorRef for the current payload.
   * Creates a new actor if the key is sleeping or doesn't exist.
   */
  def node: ActorRef = {
    if (payload.nodeType == "keys")
      self
    else
      db.get(payload.key).flatMap(_.node) match {
        case Some(node) => node
        case None => create(payload.db, payload.key, payload.nodeType).get
      }
  }

  /**
   * Creates a new actor for the given DB name, key and node type.
   * The recovery arg is set to true when the KeyNode first starts
   * and restores its keyspace from disk, otherwise (the default)
   * we persists the keyspace to disk.
   */
  def create(db: String, key: String, nodeType: String, recovery: Boolean = false): Option[ActorRef] = {
    val node = if (recovery && sleepEnabled) None else Some(context.actorOf(nodeType match {
      case "string"      => Props[StringNode]
      case "bitmap"      => Props[BitmapNode]
      case "hyperloglog" => Props[HyperLogLogNode]
      case "hash"        => Props[HashNode]
      case "list"        => Props[ListNode]
      case "set"         => Props[SetNode]
      case "sortedset"   => Props[SortedSetNode]
    }, s"$db-$nodeType-$key"))
    dbFor(db)(key) = new NodeEntry(nodeType, node)
    if (!recovery) save
    node
  }

  /**
   * Deletes a Node.
   */
  def delete(key: String, dbName: Option[String] = None): Boolean =
    dbFor(dbName.getOrElse(payload.db)).remove(key) match {
      case Some(entry) => entry.node.foreach(_ ! Delete); true
      case None => false
    }

  override def run: CommandRunner = ({
    case "_del"          => (payload.key +: args).map(key => delete(key))
    case "_keys"         => pattern(db.keys, args(0))
    case "_randomkey"    => randomItem(db.keys)
    case "_flushdb"      => db.keys.map(key => delete(key)); SimpleReply()
    case "_flushall"     => value.foreach(db => db._2.keys.map(key => delete(key, Some(db._1)))); SimpleReply()
    case "exists"        => args.map(db.contains)
    case "ttl"           => ttl / 1000
    case "pttl"          => ttl
    case "expire"        => expire(System.currentTimeMillis + (args(0).toInt * 1000))
    case "pexpire"       => expire(System.currentTimeMillis + args(0).toInt)
    case "expireat"      => expire(args(0).toLong / 1000)
    case "pexpireat"     => expire(args(0).toLong)
    case "type"          => if (db.contains(payload.key)) db(payload.key).nodeType else null
    case "renamenx"      => val x = db.contains(payload.key); if (x) {run("rename")}; x
    case "rename"        => db(payload.key).node.foreach(_ ! Payload(Seq("_rename", payload.key, args(0)), db = payload.db)); SimpleReply()
    case "persist"       => persist
    case "sort"          => sort
  }: CommandRunner) orElse runPubSub

  /**
   * We override the KeyNode actor's Receive so as to perform validation,
   * prior to the Node parent class Receive running, which wil call
   * CommandRunner for the KeyNode.
   */
  override def receiveCommand: Receive = ({
    case Routable(p) => payload = p; validate match {
      case Some(errorOrDefault) => respond(errorOrDefault)
      case None =>
        val overwrite = !db.get(payload.key).filter(_.nodeType != payload.nodeType).isEmpty
        if (Commands.overwrites(payload.command) && overwrite) delete(payload.key)
        node ! payload
        if (payload.command match {
          case "_subscribe" | "_unsubscribe" | "publish" => false
          case _ => sleepEnabled
        }) sleep
    }
  }: Receive) orElse super.receiveCommand

  /**
   * Restores the keyspace from disk on startup, creating each Node
   * actor.
   */
  override def receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot) =>
      snapshot.asInstanceOf[mutable.Map[String, DB]].foreach {db =>
        db._2.foreach {item => create(db._1, item._1, item._2.nodeType, recovery = true)}
      }
  }

}

/**
 * AggregateCommands is exclusively part of ClientNode, but defined
 * separately here for clarity. A ClientNode is responsible for
 * managing a single client connection, and handles certain commands
 * that don't go through the normal ClientNode -> KeyNode -> Node flow.
 * While some of these don't deal with Node actors at all, the bulk
 * are commands that must aggregate values from multiple Node actors,
 * which are all defined here. See the base Aggregate class for more
 * detail.
 */
trait AggregateCommands extends PayloadProcessing {

  /**
   * CommandRunner for AggregateCommands, which is given a distinct
   * name, so that ClientNode can compose together multiple
   * CommandRunner methods to form its own.
   */
  def runAggregate: CommandRunner = {
    case "mset"         => argsPaired.foreach {args => route(Seq("set", args._1, args._2))}; SimpleReply()
    case "msetnx"       => aggregate(Props[AggregateMSetNX])
    case "mget"         => aggregate(Props[AggregateMGet])
    case "bitop"        => aggregate(Props[AggregateBitOp])
    case "dbsize"       => aggregate(Props[AggregateDBSize])
    case "del"          => aggregate(Props[AggregateDel])
    case "keys"         => aggregate(Props[AggregateKeys])
    case "flushdb"      => aggregate(Props[AggregateFlushDB])
    case "flushall"     => aggregate(Props[AggregateFlushAll])
    case "pfcount"      => aggregate(Props[AggregateHyperLogLogCount])
    case "pfmerge"      => aggregate(Props[AggregateHyperLogLogMerge])
    case "randomkey"    => aggregate(Props[AggregateRandomKey])
    case "scan"         => aggregate(Props[AggregateScan])
    case "sdiff"        => aggregate(Props[AggregateSet])
    case "sinter"       => aggregate(Props[AggregateSet])
    case "sunion"       => aggregate(Props[AggregateSet])
    case "sdiffstore"   => aggregate(Props[AggregateSetStore])
    case "sinterstore"  => aggregate(Props[AggregateSetStore])
    case "sunionstore"  => aggregate(Props[AggregateSetStore])
    case "zinterstore"  => aggregate(Props[AggregateSortedSetStore])
    case "zunionstore"  => aggregate(Props[AggregateSortedSetStore])
  }

}

/**
 * PubSubClient is exclusively part of ClientNode, but defined
 * separately here for clarity. A ClientNode is responsible for
 * managing a single client connection, and PubSubClient is
 * required to store a set of channels and patterns its subscribed
 * to, similar to the way PubSubServer maps these to ClientNode
 * ActorRef instances.
*/
trait PubSubClient extends PayloadProcessing {

  /**
   * Channels subscribed to.
   */
  var channels = mutable.Set[String]()

  /**
   * Patterns subscribed to.
   */
  var patterns = mutable.Set[String]()

  /**
   * Handles all commands that subscribe or unsubsubscribe,
   * namely SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE.
   */
  def subscribeOrUnsubscribe: Unit = {
    val pattern = payload.command.head == 'p'
    val subscribed = if (pattern) patterns else channels
    val xs = if (args.isEmpty) subscribed.toSeq else args
    xs.foreach {x => route(Seq("_" + payload.command, x), destination = payload.destination, broadcast = pattern)}
  }


  /**
   * Here we override the stop method used by PayloadProcessing, which
   * allows us to inform the KeyNode actors holding subscriptions to
   * our channels and patterns that we're unsubscribing.
   */
  override def stop: Unit = {
    channels.foreach {x => route(Seq("_unsubscribe", x), destination = Some(self))}
    patterns.foreach {x => route(Seq("_punsubscribe", x), destination = Some(self), broadcast = true)}
    super.stop
  }

  /**
   * CommandRunner for PubSubClient, which is given a distinct
   * name, so that ClientNode can compose together multiple
   * CommandRunner methods to form its own.
   */
  def runPubSub: CommandRunner = {
    case "subscribe"    => subscribeOrUnsubscribe
    case "unsubscribe"  => subscribeOrUnsubscribe
    case "psubscribe"   => subscribeOrUnsubscribe
    case "punsubscribe" => subscribeOrUnsubscribe
    case "pubsub"       => args(0) match {
      case "channels" => aggregate(Props[AggregatePubSubChannels])
      case "numsub"   => if (args.size == 1) Seq() else aggregate(Props[AggregatePubSubNumSub])
      case "numpat"   => route(Seq("_numpat", randomString()), destination = payload.destination)
    }
  }

  /**
   * Receive for PubSubClient, which is given a distinct
   * name, so that ClientNode can compose together multiple
   * Receive methods to form its own. Here we provide handling for
   * PubSubEvent messages, which allow us to inform the client
   * of the number of subscriptions it holds when subscribing or
   * unsubscribing.
   */
  def receivePubSub: Receive = {
    case PubSubEvent(event, channelOrPattern) =>
      val subscriptions = if (event.head == 'p') patterns else channels
      val subscribing = event.stripPrefix("p") == "subscribe"
      val subscribed = subscribing && subscriptions.add(channelOrPattern)
      val unsubscribed = !subscribing && subscriptions.remove(channelOrPattern)
      if (subscribed || unsubscribed) {
        self ! Response(channelOrPattern, Seq(event, channelOrPattern, subscriptions.size.toString))
      }
  }

}

/**
 * A ClientNode is a special type of Node in the system. It does not
 * represent any key/value provided by a client, but instead is
 * responsible for managing the lifecycle of a single client
 * connection. Its main role in doing this is to receive messages,
 * parse the Redis protocol into a Payload which is sent onto a
 * KeyNode or Aggrgate actor, and await a reply which it then
 * converts back into the Redis protocol before sending it to the
 * client. ClientNode takes on the role of a Node, as there are a
 * variety of commands that logically belong to it.
*/
class ClientNode extends Node[Null] with PubSubClient with AggregateCommands {

  var value = null

  /**
   * Stores incoming data from the client socket, until a complete
   * Redis protocol packet arrives.
   */
  val buffer = new StringBuilder()

  /**
   * ActorRef for the client socket - we store it once a complete
   * Redis protocol packet arrives, so that we can send it the
   * command's response when it arrives back.
   */
  var client: Option[ActorRef] = None

  /**
   * Stores the current DB name - default, or one provided by the
   * SELECT command.
   */
  var db = payload.db

  /**
   * End of line marker used in parsing/writing Redis protocol.
   */
  val end = "\r\n"

  def run: CommandRunner = ({
    case "select"       => db = args(0); SimpleReply()
    case "echo"         => args(0)
    case "ping"         => SimpleReply("PONG")
    case "time"         => val x = System.nanoTime; Seq(x / 1000000000, x % 1000000)
    case "shutdown"     => context.system.terminate(); SimpleReply()
    case "quit"         => respond(SimpleReply()); self ! Delete
  }: CommandRunner) orElse runPubSub orElse runAggregate

  /**
   * Performs initial Payload validation before sending anywhere,
   * as per the Payload command's definition in commands.conf, namely
   * that the command belongs to a type of Node, it contains a key if
   * required, and the range of arguments provided match the command.
   */
  def validate: Option[ErrorReply] = {
    if (payload.nodeType == "")
      Some(ErrorReply(s"unknown command '${payload.command}'"))
    else if ((payload.key == "" && Commands.keyed(payload.command))
        || !Commands.argsInRange(payload.command, payload.args))
      Some(ErrorReply(s"wrong number of arguments for '${payload.command}' command"))
    else
      None
  }

  /**
   * Parses the input buffer for a complete Redis protocol packet.
   * If a complete packet is parsed, the buffer is cleared and its
   * contents are returned.
   */
  def parseBuffer: Option[Seq[String]] = {

    var pos = 0

    def next(length: Int = 0): String = {
      val to = if (length <= 0) buffer.indexOf(end, pos) else pos + length
      val part = buffer.slice(pos, to)
      if (part.size != to - pos) throw new Exception()
      pos = to + end.size
      part.stripLineEnd
    }

    def parts: Seq[String] = {
      val part = next()
      part.head match {
        case '-'|'+'|':' => Seq(part.tail)
        case '$'         => Seq(next(part.tail.toInt))
        case '*'         => (1 to part.tail.toInt).map(_ => parts.head)
        case _           => part.split(' ')
      }
    }

    Try(parts) match {
      case Success(output) => buffer.delete(0, pos); Some(output)
      case Failure(_)      => None
    }

  }

  /**
   * Converts a response for a command into a Redis protocol string.
   */
  def writeResponse(response: Any): String = response match {
    case x: Iterable[Any]        => s"*${x.size}${end}${x.map(writeResponse).mkString}"
    case x: Boolean              => writeResponse(if (x) 1 else 0)
    case x: Number               => s":$x$end"
    case ErrorReply(msg, prefix) => s"-$prefix $msg$end"
    case SimpleReply(msg)        => s"+$msg$end"
    case null                    => s"$$-1$end"
    case x                       => s"$$${x.toString.size}$end$x$end"
  }

  /**
   * Handles both incoming messages from the client socket actor in
   * which case a reference to the client actor socket is stored,
   * and responses from nodes which are then sent back to the client
   * socket actor.
   */
  override def receiveCommand: Receive = ({

    case Tcp.Received(data) =>
      var parsed: Option[Seq[String]] = None
      buffer.append(data.utf8String)
      while ({parsed = parseBuffer; parsed.isDefined}) {
        payload = Payload(parsed.get, db = db, destination = Some(self))
        client = Some(sender())
        validate match {
          case Some(error) => respond(error)
          case None =>
            if (payload.nodeType == "client")
              self ! payload
            else
              route(clientPayload = Option(payload))
        }
      }

    case Tcp.PeerClosed => stop

    case Response(_, response) => client.get ! Tcp.Write(ByteString(writeResponse(response)))

  }: Receive) orElse receivePubSub orElse super.receiveCommand

}

/**
 * Aggregate is the base actor class for aggregate commands. An
 * aggregate command is one that requires data for multiple keys,
 * and therefore must retrieve data from multiple Node actors - this
 * means that normal ClientNode -> KeyNode -> Node flow for a command
 * does not suffice. Each of these command generally have a
 * corresponding Aggregate subclass.
 *
 * The flow of an aggregate command is one where a ClientNode creates
 * a temporary Aggregate actor that lives for the lifecycle of the
 * command being responded to - upon receiving a command, the Aggregate
 * actor breaks the command into the individual key/command/args
 * required per key, and sends these on the normal KeyNode -> Node
 * flow, with the Aggregate actor itself being the Payload destination
 * for the response, rather than a CLientNode. The Aggregate actor
 * knows how many responses it requires (usually given by the number of
 * keys/nodes it deals with), and once all nodes have responded, it
 * then constructs the actual Payload response to send back to the
 * ClientNode.
 *
 * The construction of each Aggregate subclass takes a type parameter
 * specifying the response type it expects back from each Node, as well
 * as a command it will send to each Node it messages.
 *
 * Various aspects of the aggregation flow can be controlled by
 * overriding methods.
 */
abstract class Aggregate[T](val command: String) extends Actor with PayloadProcessing with ActorLogging {

  /**
   * Typically the initial keys mapped to responses sent back from each
   * Node actor. The keys are used so that we can preserve the original
   * order of the keys when constructing the final response. In some
   * cases where keys are not applicable, an integer is used to maintain
   * order.
   */
  var responses = mutable.Map[String, T]()

  /**
   * Ordered set of keys dealt with by the aggregation, defaulting to
   * the original Payload args of the command received.
   */
  def keys: Seq[String] = args

  /**
   * Returns responses ordered by their original key order.
   */
  def ordered: Seq[T] = keys.map(responses(_))

  /**
   * Constructs the final response to send back to the ClientNode.
   */
  def complete: Any = ordered

  /**
   * Starts the aggregation process by sending a Payload containing
   * the Aggregate subclass instance's command, for each key in the
   * originating Payload.
   */
  def begin = keys.foreach {key => route(Seq(command, key), destination = Some(self))}

  /**
   * Starts the aggregation process when the original Payload is first
   * received, and receives each Node response afterwards, until all
   * responses have arrived, at which point the final response for the
   * ClientNode is constructed and sent back, and the Aggregate actor
   * is shut down.
   */
  def receive: Receive = LoggingReceive {
    case p: Payload => payload = p; begin
    case Response(key, value) =>
      val keyOrIndex = if (responses.contains(key)) (responses.size + 1).toString else key
      responses(keyOrIndex) = value.asInstanceOf[T]
      if (responses.size == keys.size) {
        respond(Try(complete) match {
          case Success(response) => response
          case Failure(e) => log.error(e, s"Error running: $payload"); ErrorReply
        })
        stop
      }
  }

}

/**
 * Aggregate for the MGET command. Probably the simplest Aggregate as
 * it literally sends GET to each key, sending a list of responses
 * back to the ClientNode.
 */
class AggregateMGet extends Aggregate[String]("get")

/**
 * Base Aggregate for all of the set operation commands, namely
 * SDIFF/SINTER/SUNION/SDIFFSTORE/SINTERSTORE/SUNIONSTORE. It
 * simply defines the set operation based on the command name, that
 * will be used in subclasses to reduce the results to a single set.
 */
abstract class AggregateSetReducer[T](command: String) extends Aggregate[T](command) {
  type S = mutable.Set[String]
  lazy val reducer: (S, S) => S = payload.command.tail match {
    case x if x.startsWith("diff")  => (_ &~ _)
    case x if x.startsWith("inter") => (_ & _)
    case x if x.startsWith("union") => (_ | _)
  }
}

/**
 * Base Aggregate for all of the non-storing set commands, namely
 * SDIFF/SINTER/SUNION. All it does is define the command used for
 * retrieving all members for each key, namely SMEMBERS.
 */
abstract class BaseAggregateSet extends AggregateSetReducer[mutable.Set[String]]("smembers")

/**
 * Aggregate for all of the non-storing set commands, namely
 * SDIFF/SINTER/SUNION. It glues together the reducing operation with
 * the completion process.
 */
class AggregateSet extends BaseAggregateSet {
  override def complete: Any = ordered.reduce(reducer)
}

/**
 * Aggregate for all of the storing set commands, namely
 * SDIFFSTORE/SINTERSTORE/SUNIONSTORE. It overrides the completion
 * process to store the reduced results in the appropriate Node,
 * and abort sending a response which will be handled by the final
 * Node being written to.
 */
class AggregateSetStore extends BaseAggregateSet {
  override def complete: Unit =
    route(Seq("_sstore", payload.key) ++ ordered.reduce(reducer), destination = payload.destination)
}

/**
 * Aggregate for all of the sorted set commands, namely
 * ZINTERSTORE/ZUNIONSTORE. It is very different from its
 * AggregateSetStore counterpart, given the AGGREGATE/WEIGHTS
 * args it supports.
 */
class AggregateSortedSetStore extends AggregateSetReducer[IndexedTreeMap[String, Int]]("_zget") {

  /**
   * Position of the AGGREGATE arg in the original Payload.
   */
  lazy val aggregatePos = argsUpper.indexOf("AGGREGATE")

  /**
   * Value of the AGGREGATE arg in the original Payload.
   */
  lazy val aggregateName = if (aggregatePos == -1) "SUM" else argsUpper(aggregatePos + 1)

  /**
   * Postition of the WEIGHT arg in the original Payload.
   */
  lazy val weightPos = argsUpper.indexOf("WEIGHTS")

  /**
   * The actual operation that will be performed given the AGGREGATE
   * arg in the original Payload.
   */
  lazy val aggregate: (Int, Int) => Int = aggregateName match {
    case "SUM" => (_ + _)
    case "MIN" => min _
    case "MAX" => max _
  }

  /**
   * Weight value to use, defaults to 1 if WEIGHT arg not defined.
   */
  def weight(i: Int): Int = if (weightPos == -1) 1 else args(weightPos + i + 1).toInt

  /**
   * Sorted Set aggregate commands define a required arg that
   * specifies the number of keys in the Payload - here we use
   * that to pull out the keys.
   */
  override def keys: Seq[String] = args.slice(1, args(0).toInt + 1)

  /**
   * Reduces results based on the AGGREGATE/WEIGHT args in the original
   * Payload, storing the reduced results in the appropriate Node,
   * and aborting sending a response which will be handled by the final
   * Node being written to.
   */
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
      }
      i += 1
      out
    }).entrySet.toSeq.flatMap(e => Seq(e.getValue.toString, e.getKey))
    route(Seq("_zstore", payload.key) ++ result, destination = payload.destination)
  }

}

/**
 * Aggregate for the BITOP command. Conceptually similar to
 * AggregateSetStore where the results are reduced then stored,
 * defering the final response to the Node being written to.
 */
class AggregateBitOp extends Aggregate[mutable.BitSet]("_bget") {
  override def keys: Seq[String] = args.drop(2)
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

/**
 * Aggregate for the PFCOUNT command. Simpy runs HLL count on the Node
 * for each key given, and sums the results.
 */
class AggregateHyperLogLogCount extends Aggregate[Long]("_pfcount") {
  override def complete: Long = responses.values.sum
}

/**
 * Aggregate for the PFMERGE command. Reduces each HLL with a union
 * operation, storing the final result in the Node for the given key.
 */
class AggregateHyperLogLogMerge extends Aggregate[HLL]("_pfget") {
  override def complete: Unit = {
    val result = ordered.reduce({(x, y) => x.union(y); x}).toBytes.map(_.toString)
    route(Seq("_pfstore", payload.key) ++ result, destination = payload.destination)
  }
}

/**
 * Base Aggregate for all commands that need to communicate with *all*
 * KeyNode actors, namely PUBSUB with the CHANNELS subcommand, and each
 * of the commands that deal with the keyspace itself, namely
 * KEYS/SCAN/DBSIZE/RANDOMKEY/DEL/EXISTS/FLUSHDB/FLUSHALL.
 *
 * As these commands don't deal with keys, define keys to means an
 * incremental integer, one for each KeyNode actor in the system.
 */
abstract class AggregateBroadcast[T](command: String) extends Aggregate[T](command) {

  /**
   * These are the args we'll use in the Payload that is broadcast to
   * all KeyNode actors, since keys is overtaken in meaning.
   */
  def broadcastArgs: Seq[String] = payload.args

  /**
   * For keys we use a range of integers, one for each KeyNode actor in
   * the system.
   */
  override def keys: Seq[String] = (1 to context.system.settings.config.getInt("curiodb.keynodes")).map(_.toString)

  /**
   * Constructs the broadcast Payload for each KeyNode actor.
   */
  override def begin: Unit = route(command +: broadcastArgs, destination = Some(self), broadcast = true)

}

/**
 * Aggregate for the PUBSUB CHANNELS command/subcommand. Simply builds a
 * list of channels returned.
 */
class AggregatePubSubChannels extends AggregateBroadcast[Iterable[String]]("_channels") {
  override def broadcastArgs: Seq[String] = Seq(if (args.size == 2) args(1) else "*")
  override def complete: Iterable[String] = responses.values.reduce(_ ++ _)
}

/**
 * Aggregate for the PUBSUB NUMSUB command/subcommand. This is a normal
 * Aggregate subclass that simply returns a list of responses.
 */
class AggregatePubSubNumSub extends Aggregate[Int]("_numsub") {

  /**
   * First arg is the NUMSUB subcommand, not a key.
   */
  override def keys: Seq[String] = args.drop(1)

  /**
   * Pair keys with responses.
   */
  override def complete: Seq[String] = keys.flatMap(x => Seq(x, responses(x).toString))

}

/**
 * Base Aggregate for all commands that need to read lists of keys
 * from each KeyNode actor, namely KEYS/SCAN/DBSIZE.
 */
abstract class BaseAggregateKeys extends AggregateBroadcast[Iterable[String]]("_keys") {
  def reduced: Iterable[String] = responses.values.reduce(_ ++ _)
}

/**
 * Aggregate for the KEYS command. Simply combines all keys returned
 */
class AggregateKeys extends BaseAggregateKeys {
  override def complete: Iterable[String] = reduced
}

/**
 * Aggregate for the SCAN command. Applies scan behavior in the same
 * way as the SSCAN/HSCAN/ZSCAN commands.
 */
class AggregateScan extends BaseAggregateKeys {

  /**
   * We need all keys returned so that we can locally apply the scan,
   * so here we specify the wildcard arg for each KeyNode actor's
   * Payload.
   */
  override def broadcastArgs: Seq[String] = Seq("*")

  /**
   * Applies scan to the keys returned.
   */
  override def complete: Seq[String] = scan(reduced)

}

// TODO: add an internal command for getting size from keynode.
class AggregateDBSize extends BaseAggregateKeys {
  override def broadcastArgs: Seq[String] = Seq("*")
  override def complete: Int = reduced.size
}

/**
 * Aggregate for the RANDOMKEY command. To avoid pulling down all keys,
 * each KeyNode has an internal _RANDOMKEY command, and we then draw
 * a random one of these.
 */
class AggregateRandomKey extends AggregateBroadcast[String]("_randomkey") {
  override def complete: String = randomItem(responses.values.filter(_ != ""))
}

/**
 * Base Aggregate for commands that deal with boolean responses from
 * each KeyNode actor, namely DEL/EXISTS/MSETNX.
 */
abstract class BaseAggregateBool(command: String) extends AggregateBroadcast[Iterable[Boolean]](command) {

  /**
   * Returns all the true responses.
   */
  def trues: Iterable[Boolean] = responses.values.flatten.filter(_ == true)

}

/**
 * Aggregate for the DEL command. There's an unintuitive performance
 * consideration here - rather than sending a message per key, we
 * broadcast all keys to all KeyNode actors, and allow them to ignore
 * keys they don't manage. This means more data travelling through the
 * system, but limits the number of messages being sent, which greatly
 * improves performance for large sets of keys.
 */
class AggregateDel extends BaseAggregateBool("_del") {

  /**
   * Payload args are actually keys, even though we're broadcasting.
   */
  override def broadcastArgs: Seq[String] = payload.args

  /**
   * Each key that actually belong to a KeyNode will return true.
   */
  override def complete: Int = trues.size

}

/**
 * Aggregate for the MSETNX command. We first query the KeyNode actors
 * for each key existing, and only send values to store in Node actors
 * if none of the keys exist, as per the MSETNX command's behavior.
 */
class AggregateMSetNX extends BaseAggregateBool("exists") {

  /**
   * Every odd arg is a key, and every even arg is a value.
   */
  override def keys: Seq[String] = payload.argsPaired.map(_._1)

  override def complete: Boolean = {
    if (trues.isEmpty) payload.argsPaired.foreach {args => route(Seq("set", args._1, args._2))}
    trues.isEmpty
  }

}

/**
 * Base Aggregate for commands that don't need data for a reply, namely
 * FLUSHDB/FLUSHALL.
 */
abstract class AggregateSimpleReply(command: String) extends AggregateBroadcast[String](command) {
  override def complete: SimpleReply = SimpleReply()
}

/**
 * Aggregate for the FLUSHDB command. It simply sends off the
 * corresponding internal command to all KeyNode actors.
 */
class AggregateFlushDB extends AggregateSimpleReply("_flushdb")

/**
 * Aggregate for the FLUSHALL command. It simply sends off the
 * corresponding internal command to all KeyNode actors.
 */
class AggregateFlushAll extends AggregateSimpleReply("_flushall")

/**
 * Actor for the TCP server that registers a ClientNode for each
 * client.
 */
class Server(listen: URI) extends Actor {
  IO(Tcp)(context.system) ! Tcp.Bind(self, new InetSocketAddress(listen.getHost, listen.getPort))
  def receive: Receive = LoggingReceive {
    case Tcp.Connected(_, _) => sender() ! Tcp.Register(context.actorOf(Props[ClientNode]))
  }
}

/**
 * Main entry point for the system. It simply configures Akka
 * clustering, and starts the TCP server once all expected KeyNode
 * actors have started.
 *
 * Currently the number of nodes (instances of the program, not Node
 * actors) in the cluster is fixed in size, given the config value
 * curiodb.nodes, eg:
 *
 * {{{
 * curoidb.nodes = {
 *   node1: "tcp://127.0.0.1:9001"
 *   node2: "tcp://127.0.0.1:9002"
 *   node3: "tcp://127.0.0.1:9003"
 * }
 * }}}
 *
 * We then use this value to configure the various akka.cluster
 * config values. One of the main future goals is to explore
 * Akka's cluster sharding package, which should allow for more
 * dynamic topologies.
 */
object CurioDB {
  def main(args: Array[String]): Unit = {

    val sysName   = "curiodb"
    val config    = ConfigFactory.load()
    val listen    = new URI(config.getString("curiodb.listen"))
    val node      = if (args.isEmpty) config.getString("curiodb.node") else args(0)
    val nodes     = config.getObject("curiodb.nodes").map(n => (n._1 -> new URI(n._2.unwrapped.toString)))
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
    Await.result(system.whenTerminated, Duration.Inf)

  }
}
