/**
 * TCP, HTTP, and WebSocket servers, and main entry point to the program.
 *
 * All the servers are modelled similarly - they each have a single
 * server actor that takes incoming requests, and constructs subclassed
 * ClientNode actors for each request. The ClientNode actors are
 * responsible for constructing a Command payload, routing it, and
 * converting a Response payload back into the appropriate format.
 */

package curiodb

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.event.LoggingReceive
import akka.io.{IO, Tcp}
import akka.routing.FromConfig
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import java.net.{InetSocketAddress, URI}
import java.io.File
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure, Try}
import spray.can.Http
import spray.can.server.UHttp
import spray.can.websocket.frame.{TextFrame}
import spray.can.websocket.WebSocketServerWorker
import spray.http._
import spray.json._
import spray.json.DefaultJsonProtocol._

/**
 * Base ClientNode for HTTP and WebSocket ClientNode actors - provides
 * methods for converting to/from JSON, and cleanup on disconnect.
 */
abstract class JsonClientNode extends ClientNode {

  val MISSING_ARG = "Missing valid JSON object with 'args' key"

  /**
   * Converts a response to JSON - we need to deal with each type
   * specifically so that spray-json knows what to do with it.
   * ErrorReply is omitted, since different actions are taken when
   * errors are returned.
   */
  def toJson(response: Any): JsValue = response match {
    case x: Iterable[Any] => x.map(toJson).toJson
    case x: Boolean       => toJson(if (x) 1 else 0)
    case x: Int           => x.toJson
    case x: Long          => x.toJson
    case x: String        => x.toJson
    case SimpleReply(msg) => msg.toJson
    case null             => JsNull
  }

  /**
   * Parses a JSON arg list from a HttpRequest or WebSocket TextFrame.
   */
  def fromJson(entity: String): Option[Seq[String]] =
    Try(entity.parseJson.asJsObject.getFields("args")(0).convertTo[Seq[String]]) match {
      case Success(input) => Some(input)
      case Failure(_)     => None
    }

  /**
   * Constructs the final JSON object from a Response payload.
   */
  def jsonResult(response: Any): String =
    Map("result" -> toJson(response)).toJson.toString + "\n"

  // Triggers cleanup for PubSub etc.
  override def receiveCommand: Receive = ({
    case _: Http.ConnectionClosed => stop
  }: Receive) orElse super.receiveCommand

}

/**
 * ClientNode that manages a single HTTP request - it extracts the JSON
 * args list from it and constructs a Command payload from them, and
 * then waits to receive back a Response payload, which it converts
 * back to JSON before returning it as a HTTP response.
 *
 * In the case of SUBSCRIBE/PSUBSCRIBE commands, state is changed into
 * a chunked mode which holds the connection open and can send multiple
 * PubSub messages back as chunked responses.
 */
class HttpClientNode extends JsonClientNode {

  /**
   * Flag marking the client as in chunked mode for PubSub.
   */
  var chunked: Boolean = false

  /**
   * Flag marking chunked mode for PubSub has started.
   */
  var chunkedStarted: Boolean = false

  /**
   * Shortcut for a 400 HttpResponse with an error message.
   */
  def errorResponse(entity: String): HttpResponse =
    HttpResponse(status = StatusCodes.BadRequest, entity = entity + "\n")

  /**
   * Shortcut for a 200 HttpResponse with a JSON entity.
   */
  def jsonResponse(json: String): HttpResponse =
    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), json))

  /**
   * Handles parsing a JSON arg list from a POST HttpRequest, and
   * constructing a Command payload from it, and receiving the Response
   * payload back for the command, which it then converts back to JSON
   * and sends it back to the client connection.
   */
  override def receiveCommand: Receive = ({

    case HttpRequest(HttpMethods.POST, Uri.Path("/"), _, entity, _) =>
      fromJson(entity.asString) match {
        case Some(input) => sendCommand(input); if (command.name.endsWith("SUBSCRIBE")) chunked = true
        case None        => sender() ! errorResponse(MISSING_ARG)
      }

    // Fallback for any other HttpRequest, just return 404.
    case _: HttpRequest => sender() ! HttpResponse(status = StatusCodes.NotFound)

  }: Receive) orElse super.receiveCommand

  /**
   * Handles the various types of responses that can be returned, eg
   * HTTP 200/400, and chunked start/message for PubSub.
   */
  override def formatResponse(response: Any): Any = response match {
    case ErrorReply(msg, _)                => errorResponse(msg)
    case _ if (chunked && !chunkedStarted) => chunkedStarted = true; ChunkedResponseStart(jsonResponse(jsonResult(response)))
    case _ if (chunked)                    => MessageChunk(jsonResult(response))
    case _                                 => jsonResponse(jsonResult(response))
  }

}

/**
 * Actor for the HTTP server that registers creation of a
 * HttpClientNode for each connection made.
 */
class HttpServer(listen: URI) extends Actor {

  IO(UHttp)(context.system) ! Http.Bind(self, interface = listen.getHost, port = listen.getPort)

  def receive: Receive = LoggingReceive {
    case _: Http.Connected => sender() ! Http.Register(context.actorOf(Props[HttpClientNode]))
  }

}

class WebSocketClientNode(val serverConnection: ActorRef) extends JsonClientNode with WebSocketServerWorker {

  /**
   * Shortcut for a JSON object with an error value.
   */
  def errorResult(error: String): String =
    Map("error" -> error).toJson.toString

  /**
   * Setup required for spray-websocket.
   */
  override def receive: Receive = handshaking orElse businessLogic

  /**
   * Main Receive method - handles incoming TextFrame payloads, which
   * all use JSON strings.
   */
  override def businessLogic: Receive = ({
    case TextFrame(data) =>
      fromJson(data.decodeString("UTF-8")) match {
        case Some(input) => sendCommand(input)
        case None        => sender() ! errorResult(MISSING_ARG)
      }
  }: Receive) orElse receiveCommand

  /**
   * Converts responses to JSON embedded in TextFrame payloads.
   */
  override def formatResponse(response: Any): Any = TextFrame(response match {
    case ErrorReply(msg, _) => errorResult(msg)
    case _                  => jsonResult(response)
  })

}

/**
 * Actor for the WebSocket server that registers creation of a
 * WebSocketClientNode for each connection made.
 */
class WebSocketServer(listen: URI) extends Actor {

  IO(UHttp)(context.system) ! Http.Bind(self, interface = listen.getHost, port = listen.getPort)

  def receive: Receive = LoggingReceive {
    case _: Http.Connected =>
      val serverConnection = sender()
      serverConnection ! Http.Register(context.actorOf(Props(classOf[WebSocketClientNode], serverConnection)))
  }

}

/**
 * ClientNode that manages a single TCP connection - it buffers
 * data received in the Redis protocol, until it contains a complete
 * packet it can construct a Command payload with.
 */
class TcpClientNode extends ClientNode {

  /**
   * Stores incoming data from the client socket, until a complete
   * Redis protocol packet arrives.
   */
  val buffer = new StringBuilder()

  /**
   * End of line marker used in parsing/writing Redis protocol.
   */
  val end = "\r\n"

  /**
   * Parses the input buffer for a complete Redis protocol packet.
   * If a complete packet is parsed, the buffer is cleared and its
   * contents are returned.
   */
  def fromRedis(): Option[Seq[String]] = {

    var pos = 0

    def next(length: Int = 0): String = {
      val to = if (length <= 0) buffer.indexOf(end, pos) else pos + length
      val part = buffer.slice(pos, to)
      if (part.size != to - pos) throw new Exception()
      pos = to + end.size
      part.stripLineEnd
    }

    def parts: Seq[String] = {
      val part = next()
      part.head match {
        case '-'|'+'|':' => Seq(part.tail)
        case '$'         => Seq(next(part.tail.toInt))
        case '*'         => (1 to part.tail.toInt).map(_ => parts.head)
        case _           => part.split(' ')
      }
    }

    Try(parts) match {
      case Success(output) => buffer.delete(0, pos); Some(output)
      case Failure(_)      => None
    }

  }

  /**
   * Handles buffering incoming TCP data until a complete Redis
   * protocol packet has formed, and constructing a Command payload
   * from it.
   */
  override def receiveCommand: Receive = ({

    case Tcp.Received(data) =>
      var parsed: Option[Seq[String]] = None
      buffer.append(data.utf8String)
      while ({parsed = fromRedis(); parsed.isDefined})
        sendCommand(parsed.get)

    // Triggers cleanup for PubSub etc.
    case Tcp.PeerClosed => stop

  }: Receive) orElse super.receiveCommand

  /**
   * Converts a response for a command into a Redis protocol string.
   */
  override def formatResponse(response: Any): Any = {
    def toRedis(response: Any): String = response match {
      case x: Iterable[Any]        => s"*${x.size}${end}${x.map(toRedis).mkString}"
      case x: Boolean              => toRedis(if (x) 1 else 0)
      case x: Int                  => s":$x$end"
      case ErrorReply(msg, prefix) => s"-$prefix $msg$end"
      case SimpleReply(msg)        => s"+$msg$end"
      case null                    => s"$$-1$end"
      case x                       => s"$$${x.toString.size}$end$x$end"
    }
    Tcp.Write(ByteString(toRedis(response)))
  }

}

/**
 * Actor for the TCP server that registers creation of a TcpClientNode
 * for each connection made.
 */
class TcpServer(listen: URI) extends Actor {

  IO(Tcp)(context.system) ! Tcp.Bind(self, new InetSocketAddress(listen.getHost, listen.getPort))

  def receive: Receive = LoggingReceive {
    case _: Tcp.Connected => sender() ! Tcp.Register(context.actorOf(Props[TcpClientNode]))
  }

}

/**
 * Entry point for the system. It configures Akka clustering, and
 * starts the TCP and HTTP servers once the cluster has formed.
 *
 * Currently the number of nodes (instances of the program, not Node
 * actors) in the cluster is fixed in size, given the config value
 * curiodb.nodes, eg:
 *
 * {{{
 * curoidb.nodes = {
 *   node1: "tcp://127.0.0.1:9001"
 *   node2: "tcp://127.0.0.1:9002"
 *   node3: "tcp://127.0.0.1:9003"
 * }
 * }}}
 *
 * We then use this value to configure the various akka.cluster
 * config values. One of the main future goals is to explore
 * Akka's cluster sharding package, which should allow for more
 * dynamic topologies.
 */
object CurioDB {
  def main(args: Array[String]): Unit = {

    val sysName   = "curiodb"
    val options   = args.map(x => x.split("=")).map(x => x(0).dropWhile(_ == '-') -> x(1)).toMap
    val config    = ConfigFactory.load(ConfigFactory.parseFile(new File(options.getOrElse("config", ""))))
    val node      = config.getString("curiodb.node")
    val nodes     = config.getObject("curiodb.nodes").map(n => (n._1 -> new URI(n._2.unwrapped.toString)))
    val keyNodes  = nodes.size * config.getInt("akka.actor.deployment./keys.cluster.max-nr-of-instances-per-node")
    val seedNodes = nodes.values.map(u => s""" "akka.${u.getScheme}://${sysName}@${u.getHost}:${u.getPort}" """)

    val system = ActorSystem(sysName, ConfigFactory.parseString(s"""
      curiodb.keynodes = ${keyNodes}
      curiodb.node = ${node}
      akka.cluster.seed-nodes = [${seedNodes.mkString(",")}]
      akka.cluster.min-nr-of-members = ${nodes.size}
      akka.remote.netty.tcp.hostname = "${nodes(node).getHost}"
      akka.remote.netty.tcp.port = ${nodes(node).getPort}
      akka.actor.deployment./keys.nr-of-instances = ${keyNodes}
    """).withFallback(config))

    // Once the cluster is formed, create all KeyNode actors, and
    // start the TCP and HTTP servers (if configured).
    Cluster(system).registerOnMemberUp {

      println("All cluster nodes are up!")
      system.actorOf(Props[KeyNode].withRouter(FromConfig()), name = "keys")

      config.getStringList("curiodb.listen").map(new URI(_)).foreach {uri =>
        val scheme = uri.getScheme.toLowerCase
        system.actorOf(Props(scheme match {
          case "http" => classOf[HttpServer]
          case "tcp"  => classOf[TcpServer]
          case "ws"   => classOf[WebSocketServer]
        }, uri), s"$scheme-server")
        println(s"$scheme-server listening on ${uri}")
      }

    }

    Await.result(system.whenTerminated, Duration.Inf)

  }
}
