/**
 * TCP server and main entry point to the program.
 */

package curiodb

import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.Cluster
import akka.event.LoggingReceive
import akka.io.{IO, Tcp}
import akka.routing.FromConfig
import com.typesafe.config.ConfigFactory
import java.net.{InetSocketAddress, URI}
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Actor for the TCP server that registers a ClientNode for each
 * client.
 */
class Server(listen: URI) extends Actor {
  IO(Tcp)(context.system) ! Tcp.Bind(self, new InetSocketAddress(listen.getHost, listen.getPort))
  def receive: Receive = LoggingReceive {
    case Tcp.Connected(_, _) => sender() ! Tcp.Register(context.actorOf(Props[ClientNode]))
  }
}

/**
 * Main entry point for the system. It simply configures Akka
 * clustering, and starts the TCP server once all expected KeyNode
 * actors have started.
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
    val listen    = new URI(config.getString("curiodb.listen"))
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

    Cluster(system).registerOnMemberUp {
      println("All nodes are up!")
      system.actorOf(Props[KeyNode].withRouter(FromConfig()), name = "keys")
    }

    system.actorOf(Props(new Server(listen)), "server")
    Await.result(system.whenTerminated, Duration.Inf)

  }
}
