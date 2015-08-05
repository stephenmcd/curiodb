/**
 * Traits for adding Lua scripting to Node actors.
 */

package curiodb

import akka.actor.{Actor, ActorContext, ActorLogging, Props}
import akka.pattern.ask
import akka.util.Timeout
import java.io.ByteArrayInputStream
import java.security.MessageDigest
import org.luaj.vm2.{LuaValue, Varargs => LuaArgs, LuaTable, LuaNumber, LuaClosure, Prototype => LuaScript, LuaError}
import org.luaj.vm2.lib.{VarArgFunction, OneArgFunction}
import org.luaj.vm2.lib.jse.{JsePlatform, CoerceLuaToJava, CoerceJavaToLua}
import org.luaj.vm2.compiler.LuaC
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure, Try}

/**
 * Utilities for converting between Lua and JVM values. Handles manual
 * conversion between Seq and LuaValue, as well as conversion between
 * ErrorReply/SimpleReply and Lua tables (or message tables).
 */
object Coerce {

  /**
   * Constructs a message table, which is just a Lua table with a
   * specifically named single key, that is used to represent
   * SimpleReply and ErrorReply in Lua.
   */
  def toMessageTable(key: String, message: String): LuaTable = {
    val table = new LuaTable()
    table.set(key, LuaValue.valueOf(message))
    table
  }

  /**
   * Converts a JVM value to a LuaValue. We need to handle special
   * cases for SimpleReply/ErrorReply (to construct message tables),
   * and Seq -> LuaTable since LuaJ doesn't recognize Seq.
   */
  def toLua(value: Any): LuaValue = value match {
    case SimpleReply(message)   => toMessageTable("ok", message)
    case ErrorReply(message, _) => toMessageTable("err", message)
    case x: Seq[Any]            => LuaValue.listOf(x.map(toLua).toArray)
    case x                      => CoerceJavaToLua.coerce(x)
  }

  /**
   * Converts a LuaValue to a JVM value. We need to handle special
   * cases for assuming a table with a single ok/err field represents
   * SimpleReply/ErrorReply, and LuaTable -> Seq since LuaJ doesn't
   * recognize Seq.
   */
  def fromLua(value: LuaValue): Any = value match {
    case x: LuaTable if x.get("ok").isstring  => SimpleReply(x.get("ok").tojstring)
    case x: LuaTable if x.get("err").isstring => ErrorReply(x.get("err").tojstring)
    case x: LuaTable  => (for (i <- 1 to x.length) yield fromLua(x.get(i))).toSeq
    case x: LuaNumber => CoerceLuaToJava.coerce(x, classOf[Int])  // Mimic Redis casting floats to ints.
    case x            => CoerceLuaToJava.coerce(x, classOf[Any])
  }

}

/**
 * To implement call/pcall as synchronous functions, we need to use
 * Akka's ask pattern. Since each of the Node types only support
 * sending messages forwards (using tell), LuaClientNode is used as a
 * temporary actor that coordinates a command being run with the ask
 * pattern. It's also a ClientNode as it needs to construct Command
 * payloads from a sequence of args, in its case, those provided by the
 * pcall/call functions within a Lua script, and needs to be able to
 * perform the same commands a ClientNode can, such as SELECT/TIME/etc.
 *
 * The ask flow is initiated when the LuaClientNode receives the
 * CallArgs payload, constructed from the pcall/call function args in
 * CallFunction below.
 */
class LuaClientNode extends ClientNode {

  override def receiveCommand: Receive = ({
    case CallArgs(args)     => sendCommand(args)
    case response: Response => client.get ! response; stop
  }: Receive) orElse super.receiveCommand

}

/**
 * Args given to pcall/call functions inside a Lua script, that will be
 * used to construct a Command payload from a LuaClientNode actor.
 */
case class CallArgs(args: Seq[String])

/**
 * Lua API for pcall/call. When called, it takes the args provided,
 * constructs a CallArgs payload from them, creates a temporary
 * LuaClientNode actor and sends them to it using the ask pattern.
 * The raiseErrors arg marks the different behavior when a runtime Lua
 * error occurs via pcall/call - specifically whether a LuaError is
 * raised (as with call), or a message table containing the error is
 * returned (as with pcall).
 */
class CallFunction(context: ActorContext, raiseErrors: Boolean = false) extends VarArgFunction {

  override def invoke(luaArgs: LuaArgs): LuaValue = {
    val args = (for (i <- 1 to luaArgs.narg) yield luaArgs.tojstring(i)).toSeq
    val node = context.actorOf(Props[LuaClientNode])
    val timeout_ = 2 seconds
    implicit val timeout: Timeout = timeout_
    Await.result(node ? CallArgs(args), timeout_).asInstanceOf[Response].value match {
      case ErrorReply(message, _) if raiseErrors => throw new LuaError(message)
      case result => Coerce.toLua(result)
    }
  }

}

/**
 * Lua API for the status_reply/error_reply functions. It just returns
 * a message table with the given key and message.
 */
class ReplyFunction(key: String) extends OneArgFunction {
  override def call(message: LuaValue): LuaValue = Coerce.toMessageTable(key, message.tojstring)
}

/**
 * Provides backward compatibility with table.getn from Lua 5.0.
 */
class TableGetnFunction extends OneArgFunction {
  override def call(table: LuaValue): LuaValue = table.len
}

/**
 * Scripts stored via the SCRIPT LOAD command are stored in KeyNode
 * actors, and as such, scripts can be run from both KeyNode and
 * ClientNode actors (mixed in with the ScriptingServer and
 * ScriptingClient traits), via the EVALSHA and EVAL commands
 * respectively. Given this, a temporary actor is required to run the
 * script, as it may make synchronous Lua calls to pcall/call, which
 * may result in a command running against the same KeyNode that's
 * running the script - this would fail since the running Lua script
 * would block the command from being run. So - ScriptRunner is merely
 * a temporary actor that runs a Lua script, which is initiated by
 * receiving the original Command payload it can then to respond to.
 */
class ScriptRunner(compiled: LuaScript) extends CommandProcessing with ActorLogging {

  def receive: Receive = {
    case c: Command =>
      command = c

      // Build the initial Lua environment, and provide some
      // compatibility tweaks - LuaJ seems to omit a global unpack
      // function, and we also support Lua 5.0 features like math.mod
      // and table.getn, which are removed from newer Lua versions.
      // http://lua-users.org/wiki/CompatibilityWithLuaFive
      val globals = JsePlatform.standardGlobals()
      globals.set("unpack", globals.get("table").get("unpack"))
      globals.get("math").set("mod", globals.get("math").get("fmod"))
      globals.get("table").set("getn", new TableGetnFunction())

      // Add the KEYS/ARGV Lua variables.
      val offset = if (command.name == "EVAL") 2 else 1
      val keyCount = if (offset <= args.size) args(offset - 1).toInt else 0
      val keys = args.slice(offset, offset + keyCount)
      val argv = args.slice(offset + keyCount, args.size)
      globals.set("KEYS", Coerce.toLua(keys))
      globals.set("ARGV", Coerce.toLua(argv))

      // Add the API. We add it to both the "redis" and "curiodb" names.
      val api = LuaValue.tableOf()
      api.set("pcall", new CallFunction(context))
      api.set("call", new CallFunction(context, raiseErrors = true))
      api.set("status_reply", new ReplyFunction("ok"))
      api.set("error_reply", new ReplyFunction("err"))
      globals.set("curiodb", api)
      globals.set("redis", api)

      // Run the script and return its result back to the ClientNode.
      respond(Try((new LuaClosure(compiled, globals)).call()) match {
        case Success(result) => Coerce.fromLua(result)
        case Failure(e)      => log.debug("Lua runtime error", e.getMessage); ErrorReply(e.getMessage)
      })
      stop

  }

}

/**
 * Base trait for both KeyNode and ClientNode actors that provides the
 * methods for compiling and running Lua scripts.
 */
trait Scripting extends CommandProcessing with ActorLogging {

  /**
   * Runs a compiled Lua script by constructing a temporary
   * ScriptRunner actor, and sending it the orignal command
   * received so that it can eventually reply to it directly.
   */
  def runScript(compiled: LuaScript): Unit =
    context.actorOf(Props(classOf[ScriptRunner], compiled)) ! command

  /**
   * Compiles a Lua script given by a command, and runs a success
   * function when successful - storing it in the case of LOAD SCRIPT
   * on a KeyNode, or running it in the case of EVAL on a CLientNode.
   */
  def compileScript(onSuccess: LuaScript => Any): Any =
    Try(LuaC.instance.compile(new ByteArrayInputStream((args(0)).getBytes), "")) match {
      case Success(compiled) => onSuccess(compiled)
      case Failure(e)        =>
        log.debug("Lua compile error", e.getMessage)
        ErrorReply(e.getMessage.replace("[string \"\"]:", "Error compiling script, line "))
    }

}

/**
 * KeyNode mixin that stores compiled Lua scripts via the LOAD SCRIPT
 * command, and runs them via the EVALSHA command. In the same way
 * PubSub channels leverage the routing implemented for keys, the SHA1
 * of scripts mimic keys too for distribution.
 */
trait ScriptingServer extends Scripting {

  /**
   * Compiled scripts stored on a KeyNode via the LOAD SCRIPT command.
   */
  lazy val scripts = mutable.Map[String, LuaScript]()

  /**
   * CommandRunner for ScriptingServer, which is given a distinct
   * name, so that KeyNode can compose together multiple
   * CommandRunner methods to form its own.
   */
  def runScripting: CommandRunner = {
    case "_SCRIPTEXISTS" => args.filter(scripts.contains)
    case "_SCRIPTFLUSH"  => scripts.clear
    case "_SCRIPTLOAD"   => compileScript {compiled => scripts(command.key) = compiled; command.key}
    case "EVALSHA"       =>
      scripts.get(command.key) match {
        case Some(compiled) => runScript(compiled)
        case None           => ErrorReply("No matching script. Please use EVAL.", "NOSCRIPT")
      }
  }

}

/**
 * ClientNode mixin that can run scripts directly, or forward the
 * various LOAD subcommands onto the relevant KeyNode actor.
 */
trait ScriptingClient extends Scripting {

  lazy val digest = MessageDigest.getInstance("SHA-1")

  /**
   * Constructs the SHA1 digest of a given script - this happens on
   * the ClientNode so that we can leverage the routing normally
   * used for keys when sending the script to a KeyNode to be stored.
   */
  def sha1 = digest.digest(args(1).getBytes).map("%02x".format(_)).mkString

  /**
   * CommandRunner for ScriptingClient, which is given a distinct
   * name, so that ClientNode can compose together multiple
   * CommandRunner methods to form its own.
   */
  def runScripting: CommandRunner = {
    case "EVAL"     => compileScript {compiled => runScript(compiled)}
    case "SCRIPT"   =>
      args(0).toUpperCase match {
        case "EXISTS" => aggregate(Props[AggregateScriptExists])
        case "FLUSH"  => route(Seq("_SCRIPTFLUSH"), broadcast = true); SimpleReply()
        case "LOAD"   => route(Seq("_SCRIPTLOAD", sha1, args(1)), destination = command.destination)
      }
  }

}