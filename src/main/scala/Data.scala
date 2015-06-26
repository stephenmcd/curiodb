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
