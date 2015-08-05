/**
 * TCP and HTTP servers, and main entry point to the program.
 *
 * Both the TCP and HTTP servers are modelled similarly - they
 * each have a single server actor that takes incoming requests,
 * and constructs ClientNode actors for each request. The ClientNode
 * actors are responsible for constructing a Command payload, routing
 * it, and converting a Response payload back into the appropriate
 * format.
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
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure, Try}
import spray.can.Http
import spray.http._
import spray.json._
import spray.json.DefaultJsonProtocol._

/**
 * ClientNode that manages a single HTTP request - it receives the
 * parsed command arguments from the HttpServer actor, and constructs
 * a Command payload from them, and then waits to receive back a
 * Response payload that it converts back to JSON before sending it
 * back as a HTTP response.
 */
class HttpClientNode(input: Seq[String], connection: ActorRef) extends ClientNode {

  sendCommand(input)

  /**
   * Converts a response to JSON - we need to deal with each type
   * specifically so that spray-json knows what to do with it.
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
   * Handles receiving the Response payload back for a command, which
   * it then converts back to JSON and returns it as a HttpResponse.
   */
  override def receiveCommand: Receive = ({
    case Response(_, response) =>
      connection ! (response match {
        case ErrorReply(msg, _) => HttpResponse(status = StatusCodes.BadRequest, entity = msg + "\n")
        case _ => HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`),
                                                   Map("result" -> toJson(response)).toJson.toString))
      })
      stop
  }: Receive) orElse super.receiveCommand

}

/**
 * Bridge with spray-can that receives an incoming HTTP request and
 * extracts the JSON args list from it, which it then passes onto a
 * a HttpClientNode actor to run as a command.
 */
class HttpServer(listen: URI) extends Actor {

  IO(Http)(context.system) ! Http.Bind(self, interface = listen.getHost, port = listen.getPort)

  def receive: Receive = LoggingReceive {

    case _: Http.Connected => sender() ! Http.Register(self)

    case HttpRequest(HttpMethods.POST, Uri.Path("/"), _, entity, _) =>
      Try(entity.asString.parseJson.asJsObject.getFields("args")(0).convertTo[Seq[String]]) match {
        case Success(input) => context.actorOf(Props(classOf[HttpClientNode], input, sender()))
        case Failure(_) =>
          val error = "Missing valid JSON object with 'args' key\n"
          sender() ! HttpResponse(status = StatusCodes.BadRequest, entity = error)
      }

    case _: HttpRequest =>  sender() ! HttpResponse(status = StatusCodes.NotFound)

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
  def fromRedis: Option[Seq[String]] = {

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
   * Converts a response for a command into a Redis protocol string.
   */
  def toRedis(response: Any): String = response match {
    case x: Iterable[Any]        => s"*${x.size}${end}${x.map(toRedis).mkString}"
    case x: Boolean              => toRedis(if (x) 1 else 0)
    case x: Int                  => s":$x$end"
    case ErrorReply(msg, prefix) => s"-$prefix $msg$end"
    case SimpleReply(msg)        => s"+$msg$end"
    case null                    => s"$$-1$end"
    case x                       => s"$$${x.toString.size}$end$x$end"
  }

  /**
   * Handles buffering incoming TCP data until a complete Redis
   * protocol packet has formed, and constructing a Command payload
   * from it, and receiving the Response payload back for the command,
   * which it then converts back to the Redis protocol and sends it
   * back to the client socket.
   */
  override def receiveCommand: Receive = ({

    case Tcp.Received(data) =>
      var parsed: Option[Seq[String]] = None
      buffer.append(data.utf8String)
      while ({parsed = fromRedis; parsed.isDefined})
        sendCommand(parsed.get)

    case Tcp.PeerClosed => stop

    case Response(_, response) => client.get ! Tcp.Write(ByteString(toRedis(response)))

  }: Receive) orElse receivePubSub orElse super.receiveCommand

}

/**
 * Actor for the TCP server that registers creation of a TcpClientNode
 * for each connection made.
 */
class TcpServer(listen: URI) extends Actor {

  IO(Tcp)(context.system) ! Tcp.Bind(self, new InetSocketAddress(listen.getHost, listen.getPort))

  def receive: Receive = LoggingReceive {
    case Tcp.Connected(_, _) => sender() ! Tcp.Register(context.actorOf(Props[TcpClientNode]))
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
    val config    = ConfigFactory.load()
    val node      = if (args.isEmpty) config.getString("curiodb.node") else args(0)
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
        uri.getScheme.toLowerCase match {
          case "http" => system.actorOf(Props(new HttpServer(uri)), "http-server")
          case "tcp"  => system.actorOf(Props(new TcpServer(uri)), "tcp-server")
        }
        println(s"Listening on ${uri}")
      }

    }

    Await.result(system.whenTerminated, Duration.Inf)

  }
}
