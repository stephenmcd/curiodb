/**
 * Utils for loading and validation of command definitions, the
 * main command message that represents the life-cycle of a running
 * command in the system, and other related message classes.
 */

package curiodb

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.Broadcast
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random

/**
 * Loads all of the command definitions from commands.conf into a
 * structure (maps of maps of maps) that we can use to query the
 * various bits of behavior for a single command.
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
   * Returns the CommandSet for a given command name.
   */
  def commandSet(commandName: String): Option[(String, CommandSet)] =
    commands.find(_._2.contains(commandName))

  /**
   * Returns the type of node for a given command name.
   */
  def nodeType(commandName: String): String =
    commandSet(commandName).map(_._1).getOrElse("")

  /**
   * Returns an attribute stored for the command.
   */
  def attribute(commandName: String, attributeName: String, default: String): String = {
    commandSet(commandName).map(_._2) match {
      case Some(set) => set.getOrElse(commandName, Map[String, String]()).getOrElse(attributeName, default)
      case None      => default
    }
  }

  /**
   * Returns if the command's first arg is a node's key.
   */
  def keyed(commandName: String): Boolean =
    attribute(commandName, "keyed", "true") != "false"

  /**
   * Returns if the command is considered "keyed" for routing purposes,
   * but doesn't actually store a value in a Node, eg PubSub channels
   * and Lua script hashes.
   */
  def pseudoKeyed(commandName: String): Boolean =
    attribute(commandName, "pseudoKeyed", "false") == "true"

  /**
   * Returns if the command writes its node's value.
   */
  def writes(commandName: String): Boolean =
    attribute(commandName, "writes", "false") == "true" || overwrites(commandName)

  /**
   * Returns if the command can overwrite an existing node with the
   * same key, even with a different type.
   */
  def overwrites(commandName: String): Boolean =
    attribute(commandName, "overwrites", "false") == "true"

  /**
   * Returns the default value for a command that should be given when
   * the node doesn't exist. Each of the types here are referenced by
   * the "default" param in the commands.conf file.
   */
  def default(commandName: String, args: Seq[String]): Any =
    attribute(commandName, "default", "none") match {
      case "string" => ""
      case "ok"     => SimpleReply()
      case "error"  => ErrorReply("no such key")
      case "nil"    => null
      case "zero"   => 0
      case "neg1"   => -1
      case "neg2"   => -2
      case "seq"    => Seq()
      case "SCAN"   => Seq("0", "")
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
  def argsInRange(commandName: String, args: Seq[String]): Boolean = {
    val parts = attribute(commandName, "args", "0").split('-')
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
case class Command(input: Seq[Any] = Seq(), db: String = "0", destination: Option[ActorRef] = None) {
  val name = if (input.size > 0) input(0).toString.toUpperCase else ""
  val nodeType = if (name != "") Commands.nodeType(name) else ""
  val key = if (nodeType != "" && input.size > 1 && Commands.keyed(name)) input(1).toString else ""
  val args = input.drop(if (key == "") 1 else 2).map(_.toString)
  lazy val argsPaired = (0 to args.size - 2 by 2).map {i => (args(i), args(i + 1))}
  lazy val argsUpper = args.map(_.toUpperCase)
}

/**
 * Command wrapper for routing it to its correct KeyNode.
 */
case class Routable(command: Command) extends ConsistentHashable {
  override def consistentHashKey: Any = command.key
}

/**
 * Response a Node will return to a ClientNode after a command is run.
 * Primarily used in CommandProcessing.
 */
case class Response(key: String, value: Any)

/**
 * Actor trait containing behavior for dealing with a Command - it
 * contains a command variable that the class should initially set upon
 * receiving it via the actor's receive method. Used by anything that a
 * Command passes through, such as all Node and Aggregate actors.
 */
trait CommandProcessing extends Actor {

  /**
   * The current command - normally will be set when a Command
   * arrives via the actor's receive method.
   */
  var command = Command()

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

  // These are just shortcuts to the current command.
  def args: Seq[String] = command.args
  def argsPaired: Seq[(String, String)] = command.argsPaired
  def argsUpper: Seq[String] = command.argsUpper

  /**
   * Sends an unrouted Command to one or more KeyNode actors, either by
   * routing by key, or broadcasting to all.
   */
  def route(
      input: Seq[Any] = Seq(),
      destination: Option[ActorRef] = None,
      clientCommand: Option[Command] = None,
      broadcast: Boolean = false): Unit = {

    // A Command can be pre-constructed (by a ClientNode which does so
    // in order to first validate it), or constructed here with the
    // given set of Command args, which is the common case for Node
    // actors wanting to trigger commands themselves.
    val p = clientCommand match {
      case Some(command) => command
      case None => Command(input, command.db, destination)
    }

    val keys = context.system.actorSelection("/user/keys")

    if (broadcast) keys ! Broadcast(p) else keys ! Routable(p)

  }

  /**
   * Sends a Response (usually the result of a command) back to a
   * the command's destination (usually a ClientNode sending a
   * Command).
   */
  def respond(response: Any): Unit =
    if (response != ()) {
      command.destination.foreach {d => d ! Response(command.key, response)}
    }

  /**
   * Stops the actor - we define this shortcut to give subclassing traits
   * the chance to override it and inject extra shutdown behavior that
   * concrete actors need not know about.
   */
  def stop(): Unit = context stop self

  // Following are a handful of utility functions - they don't really
  // deal directly with general Command processing, but have some
  // minor dependencies on Command args and such, and don't really
  // belong anywhere else in particular, so here they are.

  /**
   * Shortcut for creating Aggregate actors.
   */
  def aggregate(props: Props): Unit =
    context.actorOf(props, s"aggregate-${command.name}-${randomString()}") ! command

  /**
   * Utility for selecting a random item.
   */
  def randomItem(iterable: Iterable[String]): String =
    if (iterable.isEmpty) "" else iterable.toSeq(Random.nextInt(iterable.size))

  /**
   * Utility for dropping extraneous zeros from floats when converting to
   * strings, for consistency with Redis' INCRBYFLOAT/HINCRBYFLOAT commands
   * and sorted set scores.
   */
  def numberToString(n: Any): String = {
    val s = n.toString
    if (s.takeRight(2) == ".0") s.dropRight(2) else s
  }

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
// to a ClientNode, in response to a Command - they're defined so
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
