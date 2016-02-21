/**
 * Traits for adding PubSub behavior to Node actors.
 */

package curiodb

import akka.actor.{ActorRef, Props}
import scala.collection.mutable

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
trait PubSubServer extends CommandProcessing {

  /**
   * Client subscriptions to channels.
   */
  lazy val channels = mutable.Map[String, mutable.Set[ActorRef]]()

  /**
   * Client subscriptions to patterns.
   */
  lazy val patterns = mutable.Map[String, mutable.Set[ActorRef]]()

  /**
   * Handles subscribe and unsubscribe to both channels and patterns.
   * Responsible for omitting PubSubEvent messages back to the
   * ClientNode when a change in subscription occurs.
   */
  def subscribeOrUnsubscribe(): Unit = {
    val pattern = command.name.startsWith("_P")
    val subscriptions = if (pattern) patterns else channels
    val key = if (pattern) args(0) else command.key
    val subscriber = command.client.get
    val subscribing = command.name.drop(if (pattern) 2 else 1) == "SUBSCRIBE"
    val updated = if (subscribing)
      subscriptions.getOrElseUpdate(key, mutable.Set[ActorRef]()).add(subscriber)
    else
      !subscriptions.get(key).filter(_.remove(subscriber)).isEmpty
    if (!subscribing && updated && subscriptions(key).isEmpty) subscriptions -= key
    if (updated) subscriber ! PubSubEvent(command.name.tail, key)
  }

  /**
   * Sends a message that has been receieved (published) from a client,
   * to all matching subscriptions - either channels, or patterns.
   */
  def publish(): Int = {
    channels.get(command.key).map({subscribers =>
      val message = Response(Seq("message", command.key, args(1)), "")
      subscribers.foreach(_ ! message)
      subscribers.size
    }).sum + patterns.filterKeys(!pattern(Seq(command.key), _).isEmpty).map({entry =>
      val message = Response(Seq("pmessage", entry._1, command.key, args(1)), "")
      entry._2.foreach(_ ! message)
      entry._2.size
    }).sum
  }

  def runPubSub: CommandRunner = {
    case "_NUMSUB"       => channels.get(command.key).map(_.size).sum
    case "_NUMPAT"       => patterns.values.map(_.size).sum
    case "_CHANNELS"     => pattern(channels.keys, args(0))
    case "_SUBSCRIBE"    => subscribeOrUnsubscribe()
    case "_UNSUBSCRIBE"  => subscribeOrUnsubscribe()
    case "_PSUBSCRIBE"   => subscribeOrUnsubscribe()
    case "_PUNSUBSCRIBE" => subscribeOrUnsubscribe()
    case "PUBLISH"       => publish()
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
trait PubSubClient extends CommandProcessing {

  /**
   * Channels subscribed to.
   */
  lazy val channels = mutable.Set[String]()

  /**
   * Patterns subscribed to.
   */
  lazy val patterns = mutable.Set[String]()

  /**
   * Handles all commands that subscribe or unsubsubscribe,
   * namely SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE.
   */
  def subscribeOrUnsubscribe(): Unit = {
    val pattern = command.name.head == 'P'
    val subscribed = if (pattern) patterns else channels
    val xs = if (args.isEmpty) subscribed.toSeq else args
    xs.foreach {x => route(command.copy(Seq("_" + command.name, x)))}
  }

  /**
   * Here we override the stop method used by CommandProcessing, which
   * allows us to inform the KeyNode actors holding subscriptions to
   * our channels and patterns that we're unsubscribing.
   */
  override def stop(): Unit = {
    channels.foreach {x => route(Seq("_UNSUBSCRIBE", x), client = Some(self))}
    patterns.foreach {x => route(Seq("_PUNSUBSCRIBE", x), client = Some(self))}
    super.stop()
  }

  /**
   * CommandRunner for PubSubClient, which is given a distinct
   * name, so that ClientNode can compose together multiple
   * CommandRunner methods to form its own.
   */
  def runPubSub: CommandRunner = {
    case "SUBSCRIBE"    => subscribeOrUnsubscribe()
    case "UNSUBSCRIBE"  => subscribeOrUnsubscribe()
    case "PSUBSCRIBE"   => subscribeOrUnsubscribe()
    case "PUNSUBSCRIBE" => subscribeOrUnsubscribe()
    case "PUBSUB"       => args(0).toUpperCase match {
      case "CHANNELS" => aggregate(Props[AggregatePubSubChannels])
      case "NUMSUB"   => if (args.size == 1) Seq() else aggregate(Props[AggregatePubSubNumSub])
      case "NUMPAT"   => route(Seq("_NUMPAT", command.id), client = command.client)
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
      val subscriptions = if (event.head == 'P') patterns else channels
      val subscribing = event.stripPrefix("P") == "SUBSCRIBE"
      val subscribed = subscribing && subscriptions.add(channelOrPattern)
      val unsubscribed = !subscribing && subscriptions.remove(channelOrPattern)
      if (subscribed || unsubscribed) {
        self ! Response(Seq(event, channelOrPattern, subscriptions.size.toString), "")
      }
  }

}
