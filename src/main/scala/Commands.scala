/**
 * Utils for loading and validation of command definitions.
 */

package curiodb

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.collection.mutable

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
