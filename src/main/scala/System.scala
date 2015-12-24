/**
 * Base Node class, as well as system (non-data) nodes.
 */

package curiodb

import akka.actor.{ActorLogging, ActorRef, Cancellable, Props, Stash}
import akka.dispatch.ControlMessage
import akka.event.LoggingReceive
import akka.persistence._
import java.util.UUID.randomUUID
import scala.collection.JavaConversions._
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
 * not completely, as event sourcing is not used, and we rely entirely
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
   * MVCC map of transaction values for the Node mapped to client IDs,
   * including the committed "main" value, which is identified with
   * an empty string as its key. LinkedHashMap is used so that we can
   * access the most recently written value when transaction isolation
   * is configued as read-uncommitted.
   */
  var values = mutable.LinkedHashMap[String, T]("" -> emptyValue)

  /**
   * Returns the default value for the type of Node, which subclasses
   * must override. Used to set the initial value when a Node is
   * first created, and also in the edge case of resetting a
   * transaction's value if within a transaction, the Node is marked
   * deleted, and then recreated.
   */
  def emptyValue: T

  /**
   * Value getter according to the configured transaction isolation
   * level:
   *
   * - repeatable:  should always return the value of the current transaction
   * - uncommitted: always returns the newest written value, even if uncommitted
   * - committed:   always return the commit value, even inside a transaction
   *
   * Note that there is no "serializable" level since there is no notion of
   * a range query. You can read more info on isolation levels here:
   * https://en.wikipedia.org/wiki/Isolation_(database_systems)
   */
  def value: T = values(isolationLevel match {
    case "repeatable" if inTransaction => command.clientId // transaction value
    case "uncommitted"                 => values.keys.last // most recent value
    case _                             => ""               // committed value
  })

  /**
   * Value setter - writes the current value, either to the main
   * key, or a client ID if in a transaction.
   */
  def value_=(value: T): Unit =
    values(if (inTransaction) command.clientId else "") = value

  /**
   * Clones the value for use in a new transaction. Node subclasses
   * that work on mutable values must override, typically by calling
   * value.clone, otherwise we just default to the current value.
   */
  def cloneValue: T = value

  /**
   * Checks if the current Command is in a transaction.
   */
  def inTransaction: Boolean = values.contains(command.clientId)

  /**
   * Transaction isolation level: "repeatable" reads, read "committed",
   * or read "uncommitted".
   */
  val isolationLevel = context.system.settings.config.getString("curiodb.transactions.isolation")

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
   * Stores the duration which controls the minimum delay between
   * receiving a command that writes, and persisting the actual value
   * written.
   */
  val persistAfter = durationSetting("curiodb.persist-after")

  /**
   * Stores the duration which controls the amount of time we allow
   * a transaction to exist for. This ensures that if for some reason
   * the transaction does not complete, it can start handling write
   * commands again.
   */
  val transactionTimeout = durationSetting("curiodb.transactions.timeout")

  /**
   * Abstract definition of each Node actor's CommandRunner that must
   * be implemented. It's the partial function that matches a Command
   * name to the code that runs it.
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
  def save(): Unit = {
    if (isolationLevel == "uncommitted") {
      // Update insertion order.
      val key = if (inTransaction) command.clientId else ""
      values(key) = values.remove(key).get
    }
    if (!inTransaction) {
      if (persistAfter == 0) {
        saveSnapshot(value)
      } else if (persistAfter > 0 && !persisting) {
        persisting = true
        context.system.scheduler.scheduleOnce(persistAfter milliseconds) {self ! Persist}
      }
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
    case Persist    => persisting = false; saveSnapshot(values(""))
    case Delete     => deleteOldSnapshots(stopping = true); stop()
    case Sleep      => stop()
    case c: Command =>
      command = c
      // If the KeyNode for this Node marked the command as creating
      // the Node inside a transaction, set up the transaction.
      if (command.createdInTransaction) {
        beginTransaction()
        // Remove the main value, as it conceptually shouldn't exist
        // since this Node doesn't have a committed value yet. More
        // specifically though, we use the absence of a committed value
        // as a flag for shutting down the actor if the transaction is
        // aborted and no committed value exists.
        values.remove("")
      }
      val response = Try((run orElse runForAnyNode)(command.name)) match {
        case Success(response) => if (command.writes) save(); response
        case Failure(e) => log.error(e, s"Error running: $command"); ErrorReply("Unknown error")
      }
      command.respond(response)
  }

  /**
   * Moves the current value into a transaction for the current
   * command's client ID. This can occur at the start of a transaction
   * (eg _MULTI command), or if the Node actor is created inside a
   * transaction, in which case we use emptyValue, which handles the
   * edge case of a Node existing, going into a transaction, being
   * marked deleted, then being recreated.
   */
  def beginTransaction(): Any = {
    val clientId = command.clientId
    if (!inTransaction) context.system.scheduler.scheduleOnce(transactionTimeout milliseconds) {closeTransaction(clientId)}
    values(clientId) = if (inTransaction) emptyValue else cloneValue
    TransactionAck()
  }

  /**
   * Removes the transaction value and writes it back into the main
   * value. This could be called after the transaction has timed out,
   * so we only move the value if we're still in a transaction.
   */
  def commitTransaction(): Any =
    closeTransaction(committing = true) match {
      case Some(v) => value = v; TransactionAck()
      case None    => ()
    }

  /**
   * Removes and returns the transaction value. Expected to run twice
   * per transaction - once when commitTransaction is called when
   * _EXEC is received, and once when the transaction times out.
   * We also check for the lack of a committed value, which indicates
   * that the actor was created in a transaction
   */
  def closeTransaction(clientId: String = command.clientId, committing: Boolean = false): Option[T] = {
    val removed = values.remove(clientId)
    // Transaction was aborted when the Node was created in a
    // transaction, so shut down. We also check that we're actually
    // removing the transaction value, since this will run twice and
    // calling stop twice raises NullPointerExecption in Akka.
    if (!committing && values.isEmpty) stop()
    removed
  }

  /**
   * Alternate Receive handler for all Node actors - these are internal
   * commands can run for any Node type, and are not validated in any
   * way, nor are they configured in commands.conf. Currently used for
   * transaction begin (_MULTI) and commit (_EXEC).
   *
   * When _MULTI is received and a transaction is beginning, we store
   * the current value in the transaction map against the transaction's
   * client ID. Subsequent commands within the transaction will then
   * read from and write to the transaction map, until it's committed
   * via the _EXEC command, at which point we copy the value from the
   * transaction map back to the main value, and remove it from the
   * transaction map.
   *
   * The transaction may also timeout which is scheduled when _MULTI is
   * received. If this occurs and then _EXEC is received, we don't send
   * acknowledgement back to the ClientNode, so it can then deal with
   * its own timeout.
   */
  def runForAnyNode: CommandRunner = {
    case "_MULTI"   => beginTransaction()
    case "_EXEC"    => commitTransaction()
    case "_DISCARD" => closeTransaction(); ()
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
   * command to what will be a newly created Node.
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
    var sorted = if (command.indexOf("ALPHA") > -1) values.toSeq.sorted else values.toSeq.sortBy(_.toFloat)
    if (command.indexOf("DESC") > -1) sorted = sorted.reverse
    val limit = command.indexOf("LIMIT")
    if (limit > -1) sorted = sorted.slice(args(limit + 1).toInt, args(limit + 2).toInt)
    val store = command.indexOf("STORE")
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
 * command is received for its key. See KeyNode for more detail.
 */
case object Sleep extends ControlMessage

/**
 * Message that a Node or KeyNode actor sends back to a ClientNode
 * actor to acknowledge the number of keys it has dealt with after
 * receiving _MULTI or _EXEC commands.
 * See ClientNode.awaitTransactionAcks for more detail.
 */
case class TransactionAck(keyCount: Int = 1)

/**
 * A KeyNode manages a subset of keys, and stores these by mapping
 * DB names to keys to nodes, where nodes are represented by a
 * NodeEntry.
 */
@SerialVersionUID(1L)
case class NodeEntry(
    val kind: String,
    @transient var node: Option[ActorRef] = None,
    @transient var expiry: Option[(Long, Cancellable)] = None,
    @transient var sleep: Option[Cancellable] = None,
    @transient var deletedInTransaction: Boolean = false)
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
 * difference between this occurring and a Node being deleted, is that
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
  override def emptyValue = mutable.Map[String, DB]()

  /**
   * Deep clone which drills down into each DB, and copies all its
   * entries.
   */
  override def cloneValue = value.map {case (dbName, entries) =>
    dbName -> entries.map {case (key, entry) => key -> entry.copy()}
  }

  /**
   * Error message sent to ClientNode when a command is issued against
   * an existing key that contains a different type of node than the
   * type that the command belongs to.
   */
  val wrongType = ErrorReply("Operation against a key holding the wrong kind of value", prefix = "WRONGTYPE")

  /**
   * Milliseconds after which a Node should persist its value to disk,
   * and shut down, (aka sleep).
   */
  val sleepAfter = durationSetting("curiodb.sleep-after")

  /**
   * Milliseconds after which a Node automatically expires itself.
   */
  val expireAfter = durationSetting("curiodb.expire-after")

  /**
   * Current keys in transaction mapped to client IDs.
   * See transactionAcknowledge/transactionClose for more detail.
   */
  var transactionKeys = mutable.Map[String, String]()

  /**
   * Shortcut for grabbing the String/NodeEntry map (aka DB) for the
   * given DB name, or the current command.
   */
  def db(name: String = command.db): DB =
    value.getOrElseUpdate(name, mutable.Map[String, NodeEntry]())

  /**
   * Cancels expiry on a node when the PERSIST command is run.
   */
  def persist(): Int = db()(command.key).expiry match {
    case Some((_, cancellable)) => cancellable.cancel(); db()(command.key).expiry = None; 1
    case None => 0
  }

  /**
   * Initiates expiry on a node when any of the relevant commands are
   * run, namely EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT, or when the
   * "expire-after" setting is configured.
   */
  def expire(when: Long): Int = {
    persist
    val expires = (when - System.currentTimeMillis).toInt milliseconds
    val cancellable = context.system.scheduler.scheduleOnce(expires) {
      self ! command.copy(Seq("_DEL", command.key), client = None)
    }
    db()(command.key).expiry = Some((when, cancellable))
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
  def sleep(): Unit = {
    val when = sleepAfter milliseconds
    val key = command.key
    val entry = db()(key)
    entry.sleep.foreach(_.cancel())
    entry.sleep = Some(context.system.scheduler.scheduleOnce(when) {
      db().get(key).foreach {entry =>
        entry.node.foreach(_ ! Sleep)
        entry.node = None
      }
    })
  }

  /**
   * Retrieves the milliseconds remaining until expiry occurs for a key
   * when the TTL/PTTL commands are run.
   */
  def ttl: Long = db()(command.key).expiry match {
    case Some((when, _)) => when - System.currentTimeMillis
    case None => -1
  }

  /**
   * Handles the bridge between the external SORT and internal _SORT
   * commands. This is essentially a work around given that we only
   * allow external (client facing) commands to map to a single type
   * of Node.
   */
  def sort(): Any = {
    val entry = db()(command.key)
    entry.kind match {
      case "list" | "set" | "sortedset" =>
        entry.node.get ! command.copy(Seq("_SORT", command.key) ++ command.args)
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
   * Node. Also validates that for write commands, the keys for the
   * command aren't part of an existing transaction - if this occurs,
   * the commands gets stashed until the transaction has completed.
   */
  def validate: Option[Any] = {
    val _exists     = exists(command.key)
    val kind        = if (_exists) db()(command.key).kind else ""
    val invalidKind = (kind != "" && command.kind != kind &&
      command.kind != "keys" && !command.overwrites)
    val mustExist   = command.name == "LPUSHX" || command.name == "RPUSHX"
    val cantExist   = command.name == "SETNX"
    val wait        = (command.writes &&
      !command.keys.filter(transactionKeys.getOrElse(_, command.clientId) != command.clientId).isEmpty)
    if (invalidKind) {
      Some(wrongType)
    } else if ((_exists && cantExist) || (!_exists && mustExist) || (!_exists && !cantExist && command.default != () && command.keyed)) {
      Some(if (command.default != ()) command.default else 0)
    } else if (wait) {
      stash(); Some(())
    } else {
      None
    }
  }

  /**
   * Resets the current command to a copy of itself with the
   * createdInTransaction flag set to true. The Node actor
   * that receives this command will then use it to set up
   * its own transaction handling.
   */
  def commandCreatedInTransaction(): Unit =
    command = command.copy(createdInTransaction = true)

  /**
   * Shortcut for retrieving the ActorRef for the current Command.
   * There are actually 5 states handled here:
   *
   * 1) No key, Command belongs to a KeyNode.
   * 2) Key doesn't exist.
   * 3) Key exists and Node actor is not running (it's asleep).
   * 4) Key exists, Node is running.
   * 5) Key exists, Node is running, but marked deleted in transaction.
   */
  def node(): ActorRef = {
    if (command.kind == "keys")
      self  // 1) Command belongs to a KeyNode, just return self.
    else
      db().get(command.key).flatMap({entry =>
        // 5) The Node actor is running, but it's makred deleted in
        // a transaction, and we're conceptually recreating it even
        // though it has a running Node actor. In this case we reset
        // the deletedInTransaction flag, and mark the command as
        // creating the Node in a transaction, so that the Node can
        // use its emptyValue when setting the transaction up again.
        // This code reads a bit weirdly, since we're hijacking the
        // flatMap to modify some state.
        if (entry.deletedInTransaction) {
          entry.deletedInTransaction = false
          commandCreatedInTransaction()
        }
        entry.node
      }) match {
        // 4) and 5) Just return the running Node.
        case Some(node) => node
        // 2) and 3) The flatMap above ensures None is matched even
        // if the key exists, but contains no Node running, eg it's
        // asleep. In either case we create a new Node actor.
        case None => create(command.db, command.key, command.kind).get
      }
  }

  /**
   * Creates a new Node actor for the given DB name, key and node type.
   * The recovery arg is set to true when the KeyNode first starts
   * and restores its keyspace from disk, otherwise (the default)
   * we persists the keyspace to disk.
   */
  def create(dbName: String, key: String, kind: String, recovery: Boolean = false): Option[ActorRef] = {
    val node = if (recovery && sleepAfter > 0) None else Some(context.actorOf(kind match {
      case "string"      => Props[StringNode]
      case "bitmap"      => Props[BitmapNode]
      case "hyperloglog" => Props[HyperLogLogNode]
      case "hash"        => Props[HashNode]
      case "list"        => Props[ListNode]
      case "set"         => Props[SetNode]
      case "sortedset"   => Props[SortedSetNode]
    }, s"$dbName-$kind-$key"))
    db(dbName)(key) = new NodeEntry(kind, node)
    if (!recovery) save()
    if (transactionKeys.getOrElse(key, "") == command.clientId) commandCreatedInTransaction()
    node
  }

  /**
   * Deletes a Node.
   */
  def delete(key: String, dbName: Option[String] = None): Boolean = {
    val _db = db(dbName.getOrElse(command.db))
    _db.get(key) match {
      case Some(entry) if inTransaction =>
        val deleted = !entry.deletedInTransaction
        entry.deletedInTransaction = true
        deleted
      case Some(entry) => _db.remove(key); entry.node.foreach(_ ! Delete); true
      case None        => false
    }
  }

  /**
   * Does the key exist - rather than using db.contains, we also
   * need to account for keys marked as deleted in a transaction.
   */
  def exists(key: String): Boolean = db().get(key) match {
    case Some(entry) => !entry.deletedInTransaction
    case None        => false
  }

  /**
   * Called by beginTransaction and commitTransaction when _MULTI and
   * _EXEC are received respectively, and handled both forwarding on
   * these commands, and returning TransactionAck responses indicating
   * the number of *invalid* keys received, so that the ClientNode
   * managing the transaction can account for all keys sent. See
   * ClientNode.awaitTransactionAcks for more detail about this flow.
   */
  def forwardTransactionToNodes(): Any = {
    val keys = command.keys
    val nodes = keys.flatMap(db().get(_).flatMap(_.node))
    val keyCount = keys.size - nodes.size
    if (!nodes.isEmpty) {
      val c = command.copy(Seq(command.name))
      nodes.foreach(_ ! c)
    }
    if (keyCount > 0) TransactionAck(keyCount) else ()
  }

  /**
   * Overridden so that we can add the keys for this transaction to
   * transactionKeys, and run forwardTransactionToNodes.
   */
  override def beginTransaction(): Any = {
    if (!inTransaction) command.keys.foreach(key => transactionKeys(key) = command.clientId)
    super.beginTransaction()
    forwardTransactionToNodes()
  }

  /**
   * Overridden so that we can migrate the DB in the transaction map,
   * specifically handling the case of keys marked as deleted, as this
   * is the stage where they're permananely deleted as they would be
   * outside of a transaction.
   */
  override def commitTransaction(): Any =
    closeTransaction(committing = true) match {
      case Some(dbs) =>
        dbs.foreach {case (dbName, entries) =>
          entries.foreach {case (key, entry) =>
            if (entry.deletedInTransaction) {
              db(dbName).remove(key)
              entry.node.foreach(_ ! Delete)
            } else {
              db(dbName)(key) = entry
            }
          }
        }
        forwardTransactionToNodes()
      case None => ()
    }

  /**
   * Overridden so that we can clean up transactionKeys, and unstash
   * any write commands that were previously stashed due to conflicting
   * with keys in an existing transaction.
   */
  override def closeTransaction(clientId: String = command.clientId, committing: Boolean = false) = {
    if (inTransaction) {
      unstashAll()
      transactionKeys = transactionKeys.filter {case (_, cId) => cId != clientId}
    }
    super.closeTransaction(clientId, committing)
  }

  override def run: CommandRunner = ({
    case "_DISCARD"      => forwardTransactionToNodes(); closeTransaction(); ()
    case "_DEL"          => args.map(delete(_))
    case "_KEYS"         => pattern(db().keys, args(0))
    case "_RANDOMKEY"    => randomItem(db().keys)
    case "_FLUSHDB"      => db().keys.map(key => delete(key)); SimpleReply()
    case "_FLUSHALL"     => value.foreach {case (dbName, entries) => entries.keys.map({key => delete(key, Some(dbName))})}; SimpleReply()
    case "EXISTS"        => args.map(exists)
    case "TTL"           => ttl / 1000
    case "PTTL"          => ttl
    case "EXPIRE"        => expire(System.currentTimeMillis + (args(0).toInt * 1000))
    case "PEXPIRE"       => expire(System.currentTimeMillis + args(0).toInt)
    case "EXPIREAT"      => expire(args(0).toLong / 1000)
    case "PEXPIREAT"     => expire(args(0).toLong)
    case "TYPE"          => if (exists(command.key)) db()(command.key).kind else null  // TODO: flatMap+getOrElse
    case "RENAMENX"      => val x = exists(args(0)); if (x) {run("RENAME")}; x
    case "RENAME"        => db()(command.key).node.foreach(_ ! command.copy(Seq("_RENAME" +: command.keys), client = None)); SimpleReply()
    case "PERSIST"       => persist()
    case "SORT"          => sort()
  }: CommandRunner) orElse runPubSub orElse runScripting

  /**
   * We override the KeyNode actor's Receive so as to perform validation,
   * prior to the Node parent class Receive running, which wil call
   * CommandRunner for the KeyNode.
   */
  override def receiveCommand: Receive = ({
    case Routable(c) => command = c; validate match {
      case Some(errorOrDefault) => command.respond(errorOrDefault)
      case None =>
        if (command.overwrites && !db().get(command.key).filter(_.kind != command.kind).isEmpty) delete(command.key)
        node() ! command
        if (command.keyed) {
          if (sleepAfter > 0) sleep()
          if (expireAfter > 0) expire(System.currentTimeMillis + expireAfter)
        }
    }
  }: Receive) orElse super.receiveCommand

  /**
   * Restores the keyspace from disk on startup, creating each Node
   * actor.
   */
  override def receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot) =>
      snapshot.asInstanceOf[mutable.Map[String, DB]].foreach {case (dbName, entries) =>
        entries.foreach {case (key, entry) => create(dbName, key, entry.kind, recovery = true)}
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

  lazy val id = randomUUID.toString

  override def emptyValue = null

  /**
   * ActorRef we send final responses back to.
   */
  var client: Option[ActorRef] = None

  /**
   * The current DB name - default, or one provided by "SELECT".
   */
  var db = command.db

  /**
   * List of disabled commands.
   */
  val disabled = context.system.settings.config.getStringList("curiodb.commands.disabled").map(_.toUpperCase).toSet

  def run: CommandRunner = ({
    case "SELECT"         => db = args(0); SimpleReply()
    case "ECHO"           => args(0)
    case "PING"           => SimpleReply("PONG")
    case "TIME"           => val x = System.nanoTime; Seq(x / 1000000000, x % 1000000)
    case "SHUTDOWN"       => context.system.terminate(); SimpleReply()
    case "QUIT"           => respond(SimpleReply()); self ! Delete
    case "MULTI" | "EXEC" => ()
  }: CommandRunner) orElse runPubSub orElse runAggregate orElse runScripting

  /**
   * Performs initial Command validation before sending it anywhere,
   * as per the command's definition in commands.conf, namely that the
   * command belongs to a type of Node, it contains a key if required,
   * and the number of arguments fall within the configured range.
   */
  def validate: Option[ErrorReply] = {
    if (command.kind == "") {
      Some(ErrorReply(s"unknown command '${command.name}'"))
    } else if (disabled.contains(command.name)) {
      Some(ErrorReply(s"command '${command.name}' is disabled"))
    } else if ((command.key == "" && command.keyed) || !command.argsInRange) {
      Some(ErrorReply(s"wrong number of arguments for '${command.name}' command"))
    } else {
      None
    }
  }

  /**
   * Ordered map of commands that have been received - used to
   * order responses received when multiple commands are in play,
   * eg MULTI/EXEC.
   */
  var commands = mutable.LinkedHashMap[String, Command]()

  /**
   * Map of responses that have been received inside a transaction.
   */
  var responses = mutable.Map[String, Response]()

  /**
   * Flags a validation error occurring for a command inside MULTI/EXEC
   * which is then used to abort when EXEC is called.
   */
  var transactionError = false

  /**
   * Should transactions abort if any of their commands receives
   * an error response.
   */
  val transactionAbortOnError = context.system.settings.config.getString("curiodb.transactions.on-error") == "rollback"

  /**
   * Flag used to mark the ClientNode as being in "streaming" mode,
   * namely when a PubSub channel is subscribed to, and command
   * timeouts should be bypassed.
   */
  var streaming = false

  /**
   * Cleanup performed whenever MULTI/EXEC is aborted, either
   * explicitly via DISCARD or implicitly via an error.
   */
  def abortTransaction(response: Any): Unit = {
    transactionError = false
    responses.clear
    commands.clear
    respond(response)
  }

  /**
   * Determines if a transaction is triggered via MULTI.
   */
  def multi: Boolean = commands.size > 0 && commands.head._2.name == "MULTI"

  /**
   * Routes the given Command instance, initializing its timeout event.
   */
  def routeWithTimeout(command: Command) = {
    streaming = command.streaming
    if (!streaming && commandTimeout > 0) {
      val id = command.id
      context.system.scheduler.scheduleOnce(commandTimeout milliseconds) {
        commands.remove(id) match {
          case Some(c) => respond(ErrorReply(s"Timeout on $c"))
          case None    =>
        }
      }
    }
    route(command)
  }

  /**
   * Internal API for subclasses to send a command once they've
   * received its full input. Here we constructs a Command payload for
   * the given input, validate it, and typically route it to the
   * relevant actor, other than in the case of transactionKeys. In this
   * case, we either buffer the commands if MULTI/EXEC is the reason
   * for the transaction (Lua scripts also run a transaction), and then
   * when committing (EXEC/EVAL/EVALSHA), we begin the transaction
   * phase - see awaitTransactionAcks for more detail.
   */
  def sendCommand(input: Seq[String], originalClientId: Option[String] = None): Unit = {
    command = Command(input, Some(self), db, originalClientId.getOrElse(id))
    client = Some(sender())
    validate match {
      case Some(error) =>
        respond(error)
        // If we're in a transaction, note the error so we can abort
        // when EXEC is run.
        if (multi) transactionError = true
      case None =>
        commands(command.id) = command
        command.name match {
          case "DISCARD" =>
            // Aborting transactions.
            abortTransaction(SimpleReply())
          case "EXEC" if transactionError =>
            // Error occurred on initial validation of a command,
            // so abort transactions.
            abortTransaction(ErrorReply("Transaction discarded because of previous errors.", "EXECABORT"))
          case "EXEC" if !multi =>
            // EXEC without MULTI.
            abortTransaction(ErrorReply("EXEC without MULTI"))
          case "EXEC" if commands.size == 2 =>
            // MULTI and EXEC with no commands, return empty list.
            abortTransaction(Seq())
          case "EXEC" | "EVAL" | "EVALSHA" =>
            // Committing a transaction, start the first step.
            awaitTransactionAcks("_MULTI", {
              context.become(awaitTransactionResponses)
              commands.values.foreach(routeWithTimeout)
            })
          case _ if multi =>
            // Since SELECT modifies subsequent commands as they're
            // constructed, we need to run it immediately. Running it
            // again within a transaction (which happens) does nothing.
            if (command.name == "SELECT") run(command.name)
            // Adding commands to a transaction, send SimpleReply.
            respond(SimpleReply(if (commands.size == 1) "OK" else "QUEUED"))
          case _ =>
            // No transaction, send the command.
            routeWithTimeout(commands.head._2)
        }
    }
  }

  /**
   * Handles transaction management, specifically broadcasting either
   * _MULTI or _EXEC to KeyNode (and indirectly, Node) actors, and then
   * running the next stage on completion of receiving all expected
   * replies. Following are the three stages of a transaction:
   *
   * 1) Send _MULTI and await all replies confirming we can begin
   * 2) Send actual commands and await responses from Node actors
   * 3) Send _EXEC and await all replies confirming we have committed
   *
   * The awaitTransactionAcks method therefore handles steps 1 and 3
   * (step 2 is handled by awaitTransactionResponses). In each case,
   * we perform the same brodcast optimization found throughout
   * Aggregation.scala, where we send all keys to all KeyNode actors,
   * to reduce the number of messages sent, at the cost of redundant
   * keys. When we broadcast all keys for a awaitTransactionAcks step,
   * we need all KeyNode and relevant Node actors to confirm they are
   * ready. This is managed by initially calculating the number of keys
   * being transmitted (total keys * total KeyNode actors), and
   * awaiting TransactionAck replies which each contain a keyCount.
   * KeyNode actors will return a keyCount for the number of *invalid*
   * keys it recevies, while passing on _MULTI and _EXEC to the Node
   * actors it correctly received keys for - those Node actors should
   * then return a keyCount of 1 in their TransactionAck replies.
   *
   * Once the ClientNode has received TransactionAck.keyCount values
   * matching the total number of keys, it proceeds with the next
   * step. For _MULTI, this means sending the actual queued commands
   * via awaitTransactionResponses. For _EXEC, this means sending the
   * collected Response messages back to the external client.
   */
  def awaitTransactionAcks(commandName: String, onComplete: => Unit): Unit = {
    val keys = commands.values.flatMap(_.keys).toSet
    if (keys.isEmpty) {
      // If there are no keys involved, there won't be any
      // TransactionAck messages to receive, so go straight to the
      // next step.
      onComplete
    } else {
      val timeout = context.system.scheduler.scheduleOnce(commandTimeout milliseconds) {
        abortTransaction(ErrorReply(s"Transaction timeout"))
        context.unbecome
      }
      var keyCount = keys.size * context.system.settings.config.getInt("curiodb.keynodes")
      context.become({
        case Response(TransactionAck(kc), _) =>
          keyCount -= kc
          if (keyCount == 0) {context.unbecome; timeout.cancel(); onComplete}
      })
      route(commandName +: keys.toSeq, client = Some(self))
    }
  }

  /**
   * Sends the final response once a transaction has ended, which in
   * the case of MULTI/EXEC, is a sequence of all the collected
   * responses, ordered by their corresponding commands.
   */
  def endTransaction() = {
    respond(if (multi) {
      commands.keys.slice(1, commands.size - 1).toSeq.map(responses(_).value)
    } else responses.values.head.value)
    commands.clear
    responses.clear
    context.become(receiveCommand)
  }

  /**
   * Middle phase of a transaction where we receive and collect
   * Response messages for queued commands - once they're all received,
   * we construct the final response, switch state back to the default
   * receiveCommand, and send the final response back to self to be
   * received as a regular Response. Here we also check for error
   * replies if we're configured to abort transactions on errors, and
   * if any are received, we abort the transaction with _DISCARD, and
   * set all responses other than the error to null, so that clients
   * can determine which command caused the error.
   */
  def awaitTransactionResponses(): Receive = ({
    case response @ Response(_: ErrorReply, _) if transactionAbortOnError =>
      val keys = commands.values.flatMap(_.keys).toSet
      route("_DISCARD" +: keys.toSeq)
      // Set all responses to null, and then put the error response
      // against its ID - this way when the sequence of ordered
      // responses is sent back as one response, clients can determine
      // which command caused the error.
      commands.keys.foreach(id => responses(id) = Response(null, id))
      responses(response.id) = response
      endTransaction()
    case response: Response =>
      responses(response.id) = response
      // We've received all responses when there's the same number as
      // commands, omitting MULTI and EXEC.
      if (!multi || responses.size == commands.size - 2)
      awaitTransactionAcks("_EXEC", endTransaction)
  }: Receive) orElse receiveCommand  // ClientNode may still receive commands.

  /**
   * Sends a final response back to the original actor that provided the
   * input for the Command.
   */
  def respond(response: Any): Unit = client.get ! formatResponse(response)

  /**
   * Hook for subclasses to override and convert a response value
   * into an appropriate format.
   */
  def formatResponse(response: Any): Any = response

  /**
   * Receives final Response, which is then formatted per ClientNode
   * subclass, and sent back to the original actor that provided the
   * input for the Command.
   */
  override def receiveCommand: Receive = ({
    case response: Response =>
      commands.remove(response.id) match {
        case None if !streaming => log.error(s"Client received response after timeout $response")
        case _                  => respond(response.value)
      }
  }: Receive) orElse receivePubSub orElse super.receiveCommand

}
