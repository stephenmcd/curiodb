/**
 * A Payload is the main message representing the life-cycle of a
 * command being handled.
 */

package curiodb

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.Broadcast
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import scala.util.Random

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
 * Primarily used in PayloadProcessing.
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
