/**
 * Base Node class, as well as system (non-data) nodes.
 */

package curiodb

import akka.actor.{ActorLogging, ActorRef, Cancellable, Props}
import akka.dispatch.ControlMessage
import akka.event.LoggingReceive
import akka.persistence._
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure, Try}

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
 *  - Execution of the node's CommandRunner each time a Command is
 *    received.
 *  - Optionally persisting the node's value to disk (snapshotting)
 *    after a command has been handled (second point above), cleaning
 *    up older snapshots, and restoring from a snapshot on startup.
 *
 * Persistence warrants some discussion: we use akka-persistence, but
 * not completely, as event-sourcing is not used, and we rely entirely
 * on its snapshotting feature, only ever keeping a single snapshot.
 * This was basically the easiest way to get persistence working.
 * We always store a reference to the last snapshot's meta-data (the
 * lastSnapshot var) so that we can delete old snapshots whenever a
 * new one is saved. As for saving, this is controlled via the config
 * var curiodb.persist-after which is the duration after which a
 * command runs that writes the node's value (described as writable in
 * the commands.conf file). When one of these commands runs, we call
 * save, which will schedule a Persist message back to the node itself.
 * This is based on the assumption that there's no guarantee an
 * actor's recieve and scheduler won't both execute at the exact same
 * time, so we have everything run through receive. The persisting
 * var stores whether persisting has been scheduled, to allow extra
 * save calls to do nothing when persisting has already been scheduled.
 */
abstract class Node[T] extends PersistentActor with CommandProcessing with ActorLogging {

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
   * according to the duration configured by the curiodb.persist-after
   * setting.
   */
  var persisting: Boolean = false

  /**
   * Stores the duration configured by curiodb.persist-after.
   */
  val persistAfter = context.system.settings.config.getDuration("curiodb.persist-after").toMillis.toInt

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
   * and receiving Command instances, then running the Node actor's
   * CommandRunner.
   */
  def receiveCommand: Receive = {
    case SaveSnapshotSuccess(meta) => lastSnapshot = Some(meta); deleteOldSnapshots()
    case SaveSnapshotFailure(_, e) => log.error(e, "Snapshot write failed")
    case Persist    => persisting = false; saveSnapshot(value)
    case Delete     => deleteOldSnapshots(stopping = true); stop
    case Sleep      => stop
    case c: Command =>
      command = c
      respond(Try(run(command.name)) match {
        case Success(response) => if (Commands.writes(command.name)) save; response
        case Failure(e) => log.error(e, s"Error running: $command"); ErrorReply("Unknown error")
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
    if (command.key != args(0)) {
      route(Seq("_DEL", command.key))
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
      route(Seq("_LSTORE", args(store + 1)) ++ sorted)
      sorted.size
    } else sorted
  }

}

// Following are some messages we send to nodes.

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
 * the curiodb.sleep-after duration value in reference.conf). The
 * difference between this occuring and a Node being deleted, is that
 * the key and NodeEntry is kept in the keyspace map. This is also why
 * the ActorRef for each Node in a NodeEntry is an Option - a value of
 * None indicates a sleeping Node. When a command is run against a key
 * mapped to a sleeping Node, a new Node actor is created, which will
 * read its previous value from disk. The idea here is to allow more
 * data to be stored in the system than can fit in memory.
 */
class KeyNode extends Node[mutable.Map[String, mutable.Map[String, NodeEntry]]] with PubSubServer with ScriptingServer {

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
  val sleepAfter = context.system.settings.config.getDuration("curiodb.sleep-after").toMillis.toInt
  val sleepEnabled = sleepAfter > 0

  /**
   * Shortcut for grabbing the String/NodeEntry map (aka DB) for the
   * given DB name.
   */
  def dbFor(name: String): DB =
    value.getOrElseUpdate(name, mutable.Map[String, NodeEntry]())

  /**
   * Shortcut for grabbing the String/NodeEntry map (aka DB) for the
   * current Command.
   */
  def db: DB = dbFor(command.db)

  /**
   * Cancels expiry on a node when the PERSIST command is run.
   */
  def persist: Int = db(command.key).expiry match {
    case Some((_, cancellable)) =>
      cancellable.cancel()
      db(command.key).expiry = None
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
      self ! Command(Seq("_DEL", command.key), db = command.db)
    }
    db(command.key).expiry = Some((when, cancellable))
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
    val key = command.key
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
  def ttl: Long = db(command.key).expiry match {
    case Some((when, _)) => when - System.currentTimeMillis
    case None => -1
  }

  /**
   * Handles the bridge between the external SORT and internal _SORT
   * commands. This is essentially a work around given that we only
   * allow external (client facing) commands to map to a single type
   * of Node.
   */
  def sort: Any = {
      val entry = db(command.key)
      entry.nodeType match {
        case "list" | "set" | "sortedset" =>
          val sortArgs = Seq("_SORT", command.key) ++ command.args
          entry.node.get ! Command(sortArgs, db = command.db, destination = command.destination)
        case _ => wrongType
      }
  }

  /**
   * Validates that the key and command for the current Command can be
   * run. This boils down to ensuring the command belongs to the type
   * of Node mapped to the key, and that the Node must or musn't exist,
   * given the particular command. Optionally returns an error, or a
   * default value if the Node doesn't exist and a default is defined
   * (as per commands.conf). Otherwise if validation passes, None is
   * returned, indicating the command should be sent to the key's
   * Node.
   */
  def validate: Option[Any] = {
    val exists      = db.contains(command.key)
    val nodeType    = if (exists) db(command.key).nodeType else ""
    val invalidType = (nodeType != "" && command.nodeType != nodeType &&
      command.nodeType != "keys" && !Commands.overwrites(command.name))
    val mustExist   = command.name == "LPUSHX" || command.name == "RPUSHX"
    val cantExist   = command.name == "SETNX"
    val default     = Commands.default(command.name, command.args)
    if (invalidType)
      Some(wrongType)
    else if ((exists && cantExist) || (!exists && mustExist) || (!exists && !cantExist && default != ()))
      Some(if (default != ()) default else 0)
    else
      None
  }

  /**
   * Shortcut for retrieving the ActorRef for the current Command.
   * Creates a new actor if the key is sleeping or doesn't exist.
   */
  def node: ActorRef = {
    if (command.nodeType == "keys")
      self
    else
      db.get(command.key).flatMap(_.node) match {
        case Some(node) => node
        case None => create(command.db, command.key, command.nodeType).get
      }
  }

  /**
   * Creates a new Node actor for the given DB name, key and node type.
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
    dbFor(dbName.getOrElse(command.db)).remove(key) match {
      case Some(entry) => entry.node.foreach(_ ! Delete); true
      case None => false
    }

  override def run: CommandRunner = ({
    case "_DEL"          => (command.key +: args).map(key => delete(key))
    case "_KEYS"         => pattern(db.keys, args(0))
    case "_RANDOMKEY"    => randomItem(db.keys)
    case "_FLUSHDB"      => db.keys.map(key => delete(key)); SimpleReply()
    case "_FLUSHALL"     => value.foreach(db => db._2.keys.map(key => delete(key, Some(db._1)))); SimpleReply()
    case "EXISTS"        => args.map(db.contains)
    case "TTL"           => ttl / 1000
    case "PTTL"          => ttl
    case "EXPIRE"        => expire(System.currentTimeMillis + (args(0).toInt * 1000))
    case "PEXPIRE"       => expire(System.currentTimeMillis + args(0).toInt)
    case "EXPIREAT"      => expire(args(0).toLong / 1000)
    case "PEXPIREAT"     => expire(args(0).toLong)
    case "TYPE"          => if (db.contains(command.key)) db(command.key).nodeType else null
    case "RENAMENX"      => val x = db.contains(command.key); if (x) {run("RENAME")}; x
    case "RENAME"        => db(command.key).node.foreach(_ ! Command(Seq("_RENAME", command.key, args(0)), db = command.db)); SimpleReply()
    case "PERSIST"       => persist
    case "SORT"          => sort
  }: CommandRunner) orElse runPubSub orElse runScripting

  /**
   * We override the KeyNode actor's Receive so as to perform validation,
   * prior to the Node parent class Receive running, which wil call
   * CommandRunner for the KeyNode.
   */
  override def receiveCommand: Receive = ({
    case Routable(p) => command = p; validate match {
      case Some(errorOrDefault) => respond(errorOrDefault)
      case None =>
        val overwrite = !db.get(command.key).filter(_.nodeType != command.nodeType).isEmpty
        if (Commands.overwrites(command.name) && overwrite) delete(command.key)
        node ! command
        if (command.name match {
          // TODO: This list of commands should be represented somewhere
          // else, possibly as a new attribute in commands.conf. It might
          // be called "pseudokeyed" (replacing their use of "keyed"), as
          // these are the commands that leverage key distribution but
          // don't have a node that can actually sleep.
          case "_SUBSCRIBE" | "_UNSUBSCRIBE" | "PUBLISH" | "_NUMSUB" | "_NUMPAT" |
               "EVALSHA" | "_SCRIPTLOAD" | "_SCRIPTFLUSH" => false
          case _ => sleepEnabled && Commands.keyed(command.name)
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
 * A ClientNode is a special type of Node in the system. It does not
 * represent any key/value provided by a client, but instead is
 * responsible for managing the life-cycle of a single client
 * connection.
 *
 * ClientNode is subclassed according to each external protocol
 * supported, such as the Redis protocol over TCP, JSON over HTTP, and
 * also the Lua pcall/call scripting API. Each subclass is responsible
 * for converting its input into a Command payload, and converting
 * Response payloads back to the relevant protocol deal with.
 *
 * ClientNode is a Node subclass, as it also handles certain commands
 * itself, such as utiilities that don't need to be routed via KeyNode
 * actors.
 */
abstract class ClientNode extends Node[Null] with PubSubClient with AggregateCommands with ScriptingClient {

  var value = null

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
  var db = command.db

  def run: CommandRunner = ({
    case "SELECT"       => db = args(0); SimpleReply()
    case "ECHO"         => args(0)
    case "PING"         => SimpleReply("PONG")
    case "TIME"         => val x = System.nanoTime; Seq(x / 1000000000, x % 1000000)
    case "SHUTDOWN"     => context.system.terminate(); SimpleReply()
    case "QUIT"         => respond(SimpleReply()); self ! Delete
  }: CommandRunner) orElse runPubSub orElse runAggregate orElse runScripting

  /**
   * Performs initial Command validation before sending it anywhere,
   * as per the command's definition in commands.conf, namely that the
   * command belongs to a type of Node, it contains a key if required,
   * and the number of arguments fall within the configured range.
   */
  def validate: Option[ErrorReply] = {
    if (command.nodeType == "")
      Some(ErrorReply(s"unknown command '${command.name}'"))
    else if ((command.key == "" && Commands.keyed(command.name))
        || !Commands.argsInRange(command.name, command.args))
      Some(ErrorReply(s"wrong number of arguments for '${command.name}' command"))
    else
      None
  }

  /**
   * Constructs a Command payload for the given input, validates it,
   * and either routes it to a KeyNode or sends it back to itself in
   * the case of client commands.
   */
  def sendCommand(input: Seq[String]): Unit = {
    command = Command(input, db = db, destination = Some(self))
    client = Some(sender())
    validate match {
      case Some(error) => respond(error)
      case None =>
        if (command.nodeType == "client")
          self ! command
        else
          route(clientCommand = Option(command))
    }
  }

  override def receiveCommand: Receive = receivePubSub orElse super.receiveCommand

}