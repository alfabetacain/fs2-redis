package dk.alfabetacain.scadis

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Supervisor
import cats.syntax.all._
import com.comcast.ip4s.Host
import com.comcast.ip4s.SocketAddress
import dk.alfabetacain.scadis.Cluster.parseSlot
import dk.alfabetacain.scadis.Util.toBS
import dk.alfabetacain.scadis.codec.Codec
import dk.alfabetacain.scadis.parser.Value
import fs2.io.net.Network
import org.typelevel.log4cats.LoggerFactory

trait Cluster[F[_]] {
  def refresh(background: Boolean): F[Unit]
  def getConnectionFromAddress(address: SocketAddress[Host]): F[Connection[F]]
  def getConnectionFromHash(hash: Int): F[Connection[F]]
  def getMasterConnections(): F[List[Connection[F]]]
}

class ClusterImpl[F[_]: Async: Parallel: LoggerFactory](
    supervisor: Supervisor[F],
    connections: Ref[F, Map[SocketAddress[Host], F[Connection[F]]]],
    mainConnection: Connection[F],
    mainAddress: SocketAddress[Host],
) extends Cluster[F] {

  override def refresh(background: Boolean): F[Unit] = {
    val refreshFiber = for {
      actualValue <-
        for {
          res <- mainConnection.send(
            NonEmptyList.of(
              toBS("CLUSTER"),
              toBS("SLOTS")
            )
          )
          asArray <- Async[F].fromOption(
            Option(res).collect { case arr: Value.RArray => arr },
            new IllegalStateException(s"CLUSTER SLOTS output is not an array")
          )
          parsed <- Async[F].fromOption(
            asArray.value.map { elem =>
              Option(elem).collect { case arr: Value.RArray => arr }.flatMap(parseSlot)
            }.sequence,
            new IllegalStateException(s"Could not parse slots")
          )
          _ <- parsed.map(_.master).toSet.toList.map[F[Connection[F]]] { master =>
            getConnectionFromAddress(
              master.host.flatMap(host => SocketAddress.fromString(s"$host:${master.port}")).getOrElse(mainAddress)
            )
          }.parSequence
        } yield res
    } yield ()
    if (background) {
      supervisor.supervise(refreshFiber).void
    } else {
      refreshFiber
    }
  }

  override def getConnectionFromAddress(address: SocketAddress[Host]): F[Connection[F]] = {

    for {
      deferred <- Deferred[F, Connection[F]]
      result <- connections.modify { map =>
        map.get(address) match {
          case None => (map + (address -> deferred.get), Left(deferred))
          case Some(value) =>
            (map, Right(value))
        }
      }
      connection <- result match {
        case Left(_) =>
          for {
            _ <- supervisor.supervise(Connection.make(
              Network[F].client(address),
              true,
            ).use { client =>
              for {
                _ <- deferred.complete(client)
                _ <- Async[F].never[Unit]
              } yield ()
            })
            client <- deferred.get
          } yield client
        case Right(value) => value
      }
    } yield connection
  }

  override def getConnectionFromHash(hash: Int): F[Connection[F]] = {
    ???
  }

  override def getMasterConnections(): F[List[Connection[F]]] = {
    ???
  }

}

object Cluster {

  final case class Node(host: Option[Host], port: Int, nodeId: String)
  final case class Slot(start: Int, end: Int, master: Node, replicas: List[Node])

  private[scadis] def parseNode(input: Value.RArray): Option[Node] = {
    input.value match {
      case hostEntry :: portEntry :: node :: _ =>
        val host = Option(hostEntry).collect { case bs: Value.RBulkString => bs }
          .flatMap(bs => Codec.utf8Codec.decode(bs).toOption)
          .flatMap(host => Host.fromString(host))
        for {
          port <-
            Option(portEntry).collect { case l: Value.RLong => l.value.toInt }
          nodeIdEntry <-
            Option(node).collect { case bs: Value.RBulkString => bs }
          nodeId <-
            Codec.utf8Codec.decode(nodeIdEntry).toOption
        } yield Node(host, port.toInt, nodeId)

      case _ => None
    }
  }

  private[scadis] def parseSlot(input: Value.RArray): Option[Slot] = {
    input.value match {
      case startEntry :: endEntry :: masterEntry :: _ =>
        for {
          start       <- Option(startEntry).collect { case l: Value.RLong => l.value.toInt }
          end         <- Option(endEntry).collect { case l: Value.RLong => l.value.toInt }
          masterEntry <- Option(masterEntry).collect { case arr: Value.RArray => arr }
          master      <- parseNode(masterEntry)
        } yield Slot(start.toInt, end.toInt, master, List.empty)

      case _ => None
    }
  }

  def make[F[_]: Async: Parallel: LoggerFactory](mainAddress: SocketAddress[Host]) = {
    for {
      supervisor     <- Supervisor[F]
      connections    <- Resource.eval(Ref.of[F, Map[SocketAddress[Host], F[Connection[F]]]](Map.empty))
      mainConnection <- Connection.make[F](Network[F].client(mainAddress), true)
      cluster = new ClusterImpl[F](supervisor, connections, mainConnection, mainAddress)
      _ <- Resource.eval(cluster.refresh(false))
    } yield cluster
  }
}
