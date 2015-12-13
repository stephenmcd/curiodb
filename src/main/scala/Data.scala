/**
 * All Node subclasses representing each data type.
 */

package curiodb

import com.dictiography.collections.{IndexedTreeMap, IndexedTreeSet}
import net.agkn.hll.HLL
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global

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
  def expire(commandName: String): Unit = route(Seq(commandName, command.key, args(1)))

  def run: CommandRunner = {
    case "_RENAME"     => rename(value, "SET")
    case "GET"         => value
    case "SET"         => value = args(0); SimpleReply()
    case "SETNX"       => run("SET"); true
    case "GETSET"      => val x = value; value = args(0); x
    case "APPEND"      => value += args(0); value
    case "GETRANGE"    => slice(value).mkString
    case "SETRANGE"    => value.patch(args(0).toInt, args(1), 1)
    case "STRLEN"      => value.size
    case "INCR"        => value = (valueOrZero.toInt + 1).toString; value.toInt
    case "INCRBY"      => value = (valueOrZero.toInt + args(0).toInt).toString; value.toInt
    case "INCRBYFLOAT" => value = numberToString(valueOrZero.toFloat + args(0).toFloat); value
    case "DECR"        => value = (valueOrZero.toInt - 1).toString; value.toInt
    case "DECRBY"      => value = (valueOrZero.toInt - args(0).toInt).toString; value.toInt
    case "SETEX"       => val x = run("SET"); expire("EXPIRE"); x
    case "PSETEX"      => val x = run("SET"); expire("PEXPIRE"); x
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
    case "_RENAME"  => rename(value, "_BSTORE")
    case "_BSTORE"  => value.clear; value ++= args.map(_.toInt); last / 8 + (if (value.isEmpty) 0 else 1)
    case "_BGET"    => value
    case "BITCOUNT" => value.size
    case "GETBIT"   => value(args(0).toInt)
    case "SETBIT"   => val x = run("GETBIT"); value(args(0).toInt) = args(1) == "1"; x
    case "BITPOS"   => bitPos
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
    case "_RENAME"  => rename(value.toBytes.map(_.toString), "_PFSTORE")
    case "_PFCOUNT" => value.cardinality.toInt
    case "_PFSTORE" => value.clear(); value = HLL.fromBytes(args.map(_.toByte).toArray); SimpleReply()
    case "_PFGET"   => value
    case "PFADD"    => add
  }

}

/**
 * Hash commands.
 */
class HashNode extends Node[mutable.Map[String, String]] {

  var value = mutable.Map[String, String]()

  /**
   * Shortcut that sets a value, given that the hash key is the first
   * command arg.
   */
  def set(arg: Any): String = {val x = numberToString(arg); value(args(0)) = x; x}

  override def run: CommandRunner = {
    case "_RENAME"      => rename(run("HGETALL"), "_HSTORE")
    case "_HSTORE"      => value.clear; run("HMSET")
    case "HKEYS"        => value.keys
    case "HEXISTS"      => value.contains(args(0))
    case "HSCAN"        => scan(value.keys)
    case "HGET"         => value.getOrElse(args(0), null)
    case "HSETNX"       => if (!value.contains(args(0))) run("HSET") else false
    case "HGETALL"      => value.flatMap(x => Seq(x._1, x._2))
    case "HVALS"        => value.values
    case "HDEL"         => val x = run("HEXISTS"); value -= args(0); x
    case "HLEN"         => value.size
    case "HMGET"        => args.map(value.getOrElse(_, null))
    case "HMSET"        => argsPaired.foreach {args => value(args._1) = args._2}; SimpleReply()
    case "HINCRBY"      => set(value.getOrElse(args(0), "0").toInt + args(1).toInt).toInt
    case "HINCRBYFLOAT" => set(value.getOrElse(args(0), "0").toFloat + args(1).toFloat)
    case "HSET"         => val x = !value.contains(args(0)); set(args(1)); x
  }

}

/**
 * ListNode supports blocking commands (BLPOP, BRPOP, etc) where
 * if the list is empty, no immediate response is sent to the client,
 * and when the next command is run that adds to the list, we
 * essentially retry the original blocking command, and if we can
 * perform it (eg pop), we then send the requested value back to
 * the client. This is implemented by storing an ordered set of
 * Command instances that are blocked, and iterating them each time the
 * next Command is received and processed. Timeouts are also supported
 * via the scheduler, which simply removes the blocked Command from the
 * set, and sends a null Response back to the ClientNode.
 */
class ListNode extends Node[mutable.ListBuffer[String]] {

  var value = mutable.ListBuffer[String]()

  /**
   * Set of blocked Command instances awaiting a response.
   */
  var blocked = mutable.LinkedHashSet[Command]()

  /**
   * Called on each of the blocking commands, storing the received
   * Command in the blocked set if the list is currently empty, and
   * scheduling its timeout. Otherwise if the list have items, we just
   * run the non-blocking version of the command.
   */
  def block: Any = {
    if (value.isEmpty) {
      blocked += command
      context.system.scheduler.scheduleOnce(args.last.toInt seconds) {
        blocked -= command
        respond(null)
      }
      ()
    } else run(command.name.tail)  // Run the non-blocking version.
  }

  /**
   * Called each time the Node actor's CommandRunner runs - if the
   * Node actor's list has values and we have blocked commands, iterate
   * through the blocked Command instances and replay their commands.
   */
  def unblock(result: Any): Any = {
    while (value.nonEmpty && blocked.nonEmpty) {
      // Set the node's current Command to the blocked Command, so
      // that the run method has access to the correct Command.
      command = blocked.head
      blocked -= command
      respond(run(command.name.tail))
    }
    result
  }

  /**
   * LINSERT command that handles BEFORE/AFTER args.
   */
  def insert: Int = {
    val i = value.indexOf(args(1)) + (if (args.head == "AFTER") 1 else 0)
    if (i >= 0) {
      value.insert(i, args(2))
      value.size
    } else -1
  }

  /**
   * LREM command - generates a new list, filtering out the given
   * item for the given count, in either direction in linear time.
   */
  def remove: Int = {
    val count = args(0).toInt
      val item = args(1)
    var result = 0
    val iter = if (count >= 0) value.clone.iterator else value.clone.reverseIterator
    value.clear
    iter.foreach {x =>
      if (x != item || (count != 0 && result == count.abs)) {
        if (count >= 0)
          value.append(x)
        else
          value.prepend(x)
      } else result += 1
    }
    result
  }

  /**
   * LPOP/RPOP/BLPOP/BRPOP - handles returning nil on empty lists,
   * and also returning the key/item pair for blocking pops.
   */
  def pop(index: Int): Any = {
    val item = if (value.isEmpty) null else value.remove(index)
    command.name match {
      case "BLPOP" | "BRPOP" => Seq(command.key, item)
      case _                 => item
    }
  }

  def run: CommandRunner = ({
    case "_RENAME"    => rename(value, "_LSTORE")
    case "_LSTORE"    => value.clear; run("RPUSH")
    case "_SORT"      => sort(value)
    case "LPUSH"      => args.reverse ++=: value; run("LLEN")
    case "RPUSH"      => value ++= args; run("LLEN")
    case "LPUSHX"     => run("LPUSH")
    case "RPUSHX"     => run("RPUSH")
    case "LPOP"       => pop(0)
    case "RPOP"       => pop(value.size - 1)
    case "LSET"       => value(args.head.toInt) = args(1); SimpleReply()
    case "LINDEX"     => val x = args.head.toInt; if (x >= 0 && x < value.size) value(x) else null
    case "LREM"       => remove
    case "LRANGE"     => slice(value)
    case "LTRIM"      => value = slice(value).asInstanceOf[mutable.ListBuffer[String]]; SimpleReply()
    case "LLEN"       => value.size
    case "BLPOP"      => block
    case "BRPOP"      => block
    case "BRPOPLPUSH" => block
    case "RPOPLPUSH"  => val x = run("RPOP"); if (x != null) {route("LPUSH" +: args :+ x.toString)}; x
    case "LINSERT"    => insert
  }: CommandRunner) andThen unblock

}

/**
 * Set commands.
 */
class SetNode extends Node[mutable.Set[String]] {

  var value = mutable.Set[String]()

  def run: CommandRunner = {
    case "_RENAME"     => rename(value, "_SSTORE")
    case "_SSTORE"     => value.clear; run("SADD")
    case "_SORT"       => sort(value)
    case "SADD"        => val x = (args.toSet &~ value).size; value ++= args; x
    case "SREM"        => val x = (args.toSet & value).size; value --= args; x
    case "SCARD"       => value.size
    case "SISMEMBER"   => value.contains(args(0))
    case "SMEMBERS"    => value
    case "SRANDMEMBER" => randomItem(value)
    case "SPOP"        => val x = run("SRANDMEMBER"); value -= x.toString; x
    case "SSCAN"       => scan(value)
    case "SMOVE"       => val x = value.remove(args(1)); if (x) {route("SADD" +: args)}; x
  }

}

/**
 * A SortedSetEntry is stored for each value in a SortedSetNode. It's
 * essentially a score/key pair which is Ordered.
 */
case class SortedSetEntry(score: Float, key: String = "")(implicit ordering: Ordering[(Float, String)])
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
class SortedSetNode extends Node[(IndexedTreeMap[String, Float], IndexedTreeSet[SortedSetEntry])] {

  /**
   * Actual value is a two item tuple, map of keys to scores, and set
   * of score/key entries.
   */
  var value = (new IndexedTreeMap[String, Float](), new IndexedTreeSet[SortedSetEntry]())

  /**
   * Shortcut to mapping of keys to scores.
   */
  def keys: IndexedTreeMap[String, Float] = value._1

  /**
   * Shortcut to set of score/key entries.
   */
  def scores: IndexedTreeSet[SortedSetEntry] = value._2

  /**
   * Adds key/score to both structures.
   */
  def add(score: Float, key: String): Boolean = {
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
  def increment(by: Float, key: String): String = {
    val score = (if (keys.containsKey(key)) keys.get(key) else 0) + by
    remove(key)
    add(score, key)
    numberToString(score)
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
      result.flatMap(x => Seq(x.key, numberToString(x.score)))
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
    def parse(arg: String, dir: Float) = arg match {
      case "-inf" => if (scores.isEmpty) 0 else scores.first().score
      case "+inf" => if (scores.isEmpty) 0 else scores.last().score
      case arg if arg.startsWith("(") => arg.toFloat + dir
      case _ => arg.toFloat
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
    limit[(String, Float)](if (reverse) result.reverse else result).map(_._1)
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
    case "_RENAME"          => rename(scores.flatMap(x => Seq(x.score, x.key)), "_ZSTORE")
    case "_ZSTORE"          => keys.clear; scores.clear; run("ZADD")
    case "_ZGET"            => keys
    case "_SORT"            => sort(keys.keys)
    case "ZADD"             => argsPaired.map(arg => add(arg._1.toFloat, arg._2)).filter(x => x).size
    case "ZCARD"            => keys.size
    case "ZCOUNT"           => rangeByScore(args(0), args(1)).size
    case "ZINCRBY"          => increment(args(0).toFloat, args(1))
    case "ZLEXCOUNT"        => rangeByKey(args(0), args(1)).size
    case "ZRANGE"           => rangeByIndex(args(0), args(1))
    case "ZRANGEBYLEX"      => rangeByKey(args(0), args(1))
    case "ZRANGEBYSCORE"    => rangeByScore(args(0), args(1))
    case "ZRANK"            => rank(args(0))
    case "ZREM"             => remove(args(0))
    case "ZREMRANGEBYLEX"   => rangeByKey(args(0), args(1)).map(remove).filter(x => x).size
    case "ZREMRANGEBYRANK"  => rangeByIndex(args(0), args(1)).map(remove).filter(x => x).size
    case "ZREMRANGEBYSCORE" => rangeByScore(args(0), args(1)).map(remove).filter(x => x).size
    case "ZREVRANGE"        => rangeByIndex(args(1), args(0), reverse = true)
    case "ZREVRANGEBYLEX"   => rangeByKey(args(1), args(0), reverse = true)
    case "ZREVRANGEBYSCORE" => rangeByScore(args(1), args(0), reverse = true)
    case "ZREVRANK"         => rank(args(0), reverse = true)
    case "ZSCAN"            => scan(keys.keys)
    case "ZSCORE"           => numberToString(keys.get(args(0)))
  }

}

