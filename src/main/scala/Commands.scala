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
import scala.util.{Random, Success, Failure, Try}

/**
 * Loads all of the command attributes from commands.conf into a
 * structure (command names mapped to maps of attribute names/values)
 * that Command instances can use to look up their configured
 * attributes.
 */
object Attributes {

  /**
   * Command names mapped to maps of attribute names/values.
   */
  val attributes = mutable.Map[String, mutable.Map[String, String]]()

  ConfigFactory.load("commands.conf").getConfig("commands").entrySet.foreach {entry =>
    val parts = entry.getKey.replace("\"", "").split('.')
    val (kind, commandName, attributeName) = (parts(0), parts(1), parts(2))
    val command = attributes.getOrElseUpdate(commandName, mutable.Map[String, String]("kind" -> kind))
    command(attributeName) = entry.getValue.unwrapped.toString
  }

  /**
   * Looks up an attribute for a command.
   */
  def get(commandName: String, attributeName: String): String =
    attributes(commandName).getOrElse(attributeName, "")

}

/**
 * Main payload for a command - stores its name, key, and args, and
 * contains utility methods for looking up attributes configured via
 * commands.conf.
 */
case class Command(input: Seq[Any] = Seq(), client: Option[ActorRef] = None, db: String = "0") {

  /**
   * Command's name. The default state of a command on a Node when it
   * starts is an empty Command that contains no input. In that case,
   * we just set name to an empty string, which only requires checking
   * on attribute lookup.
   */
  val name = input.headOption.getOrElse("").toString.toUpperCase

  /**
   * Command's key. This is always considered to be the second item in
   * the input, even though it might not strictly be the key of a Node,
   * such as script sha values, and pubsub channels. Treating these as
   * keys allows a consistent approach for hashing, as required by
   * KeyNode routing.
   */
  val key = input.drop(1).headOption.getOrElse("").toString

  /**
   * Command's arguments, namely all input after the command's name,
   * and its key when it's a real key for a Node (defined by the
   * "keyed" attribute).
   */
  val args = input.map(_.toString).drop(if (name == "") 0 else if (keyed) 2 else 1)

  /**
   * Attribute lookup used in all the methods below.
   */
  def attribute(attributeName: String): String =
    if (name == "") "" else Attributes.get(name, attributeName)

  /**
   * Checks the position of an uppercase arg, since many commands
   * use keywords within arguments, like the SORT command and
   * many of the sorted set commands.
   */
  def indexOf(arg: String): Int = args.map(_.toUpperCase).indexOf(arg)

  /**
   * The type of node the command is for, eg string, list, hash, etc.
   */
  lazy val kind: String = attribute("kind")

  /**
   * Does the command write to a Node actor's value.
   */
  lazy val writes: Boolean = attribute("writes") == "true" || overwrites

  /**
   * Is the command able to overwrite an existing Node entirely,
   * which specifically refers to deleting it if it's of a different
   * type.
   */
  lazy val overwrites: Boolean = attribute("overwrites") == "true"

  /**
   * Does the command operate on a single key, provided by the next
   * arg after its name. The default is true since most commands
   * are modelled this way.
   */
  lazy val keyed: Boolean = attribute("keyed") != "false"

  /**
   * Returns the default value the command should respond with
   * when its key doesn't exist.
   */
  lazy val default: Any = attribute("default") match {
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
    case _        => ()
  }

  /**
   * Validates that the arguments are in range for the command.
   */
  lazy val argsInRange: Boolean = {
    val parts: Seq[String] = attribute("args") match {
      case "" => Seq("0")
      case x  => x.split('-')
    }
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

  /**
   * Sends a Response (usually the result of a command) back to a
   * the command's destination (usually a ClientNode sending a
   * Command).
   */
  def respond(response: Any): Unit =
    if (response != ()) {
      client.foreach {client => client ! Response(key, response)}
    }

  override def toString: String = input.mkString(" ").replace("\n", " ")

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
 * Command passes through, such as all ClientNode, KeyNode, Node, and
 * Aggregate actors.
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

  /**
   * Shortcut to the args of the current command.
   */
  def args: Seq[String] = command.args

  /**
   * Sends an unrouted Command to one or more KeyNode actors, either by
   * routing by key, or broadcasting to all.
   */
  def route(
      input: Seq[Any] = Seq(),
      client: Option[ActorRef] = None,
      clientCommand: Option[Command] = None,
      broadcast: Boolean = false): Unit = {

    // A Command can be pre-constructed (by a ClientNode which does so
    // in order to first validate it), or constructed here with the
    // given set of Command args, which is the common case for Node
    // actors wanting to trigger commands themselves.
    val c = clientCommand match {
      case Some(command) => command
      case None => Command(input, client, command.db)
    }

    if (c.kind == "client") {
      // For client commands, just send the command back to the
      // ClientNode actor.
      self ! c
    } else {
      val keys = context.system.actorSelection("/user/keys")
      keys ! (if (broadcast) Broadcast(c) else Routable(c))
    }

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

// Following are some reply classes that get sent from a Node back to a
// ClientNode, in response to a Command - they're defined so that we
// can handle them explicitly when building a response in the different
// ClientNode subclasses - for example the Redis protocol, where errors
// have specific notation.

/**
 * An error response, as per Redis protocol.
 */
case class ErrorReply(message: String = "syntax error", prefix: String = "ERR")

/**
 * A simple response, as per Redis protocol.
 */
case class SimpleReply(message: String = "OK")
