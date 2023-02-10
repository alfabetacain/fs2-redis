package dk.alfabetacain.scadis

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.comcast.ip4s.Host
import com.comcast.ip4s.SocketAddress
import dk.alfabetacain.scadis.Client.KillClientFilter
import dk.alfabetacain.scadis.Util.expect
import dk.alfabetacain.scadis.Util.toBS
import dk.alfabetacain.scadis.codec.Codec
import dk.alfabetacain.scadis.codec.Decoder
import dk.alfabetacain.scadis.codec.Encoder
import dk.alfabetacain.scadis.parser.Value
import org.typelevel.log4cats.LoggerFactory

import scala.util.Try

trait ClusterClient[F[_], I, O] {
  def get(key: I): F[Option[O]]
  def set(key: I, value: I): F[Boolean]
  def ping(): F[String]
  def clientId(): F[Long]
  def killClient(filters: NonEmptyList[KillClientFilter], killMe: Boolean): F[Long]
  def raw(arguments: NonEmptyList[Value.RBulkString]): F[Value]
}

private[scadis] class ClusterClientImpl[F[_]: Async: Parallel: LoggerFactory, I, O](
    mainAddress: SocketAddress[Host],
    encoder: Encoder[I],
    decoder: Decoder[O],
    config: ClusterClient.Config,
    cluster: Cluster[F],
) extends ClusterClient[F, I, O] {

  override def raw(
      arguments: NonEmptyList[Value.RBulkString],
  ): F[Value] = rawToAddress(arguments)

  private def rawToAddress(
      arguments: NonEmptyList[Value.RBulkString],
      address: Option[SocketAddress[Host]] = None
  ): F[Value] = {
    val usedAddress = address.getOrElse(mainAddress)
    for {
      someClient <- cluster.getConnectionFromAddress(usedAddress)
      sendResult <- someClient.send(arguments)
      withRedirects <- sendResult match {
        case ClusterClient.MovedMatch(moved) =>
          val newAddressString = s"${moved.host.getOrElse(usedAddress.host.toString())}:${moved.port}"
          val newAddressOpt =
            SocketAddress.fromString(newAddressString)
          Async[F].fromOption(
            newAddressOpt,
            new IllegalStateException(
              s"Could not parse socket address from moved instruction. Instruction = $moved, attempted parsing '$newAddressString'"
            )
          )
            .flatMap(newAddress => rawToAddress(arguments, Some(newAddress)))

        case _ =>
          Async[F].pure(sendResult)
      }
    } yield withRedirects
  }

  private def encode(input: I): Value.RBulkString = encoder.encode(input)

  override def get(key: I): F[Option[O]] = {
    expect(
      raw(NonEmptyList.of(toBS("GET"), encode(key))),
      {
        case Value.RNull               => Option.empty
        case result: Value.RBulkString => decoder.decode(result).toOption
      }
    )
  }

  override def set(key: I, value: I): F[Boolean] = {
    expect(
      raw(NonEmptyList.of(toBS("SET"), encode(key), encode(value))),
      {
        case simple: Value.RString => simple.value == "OK"
      }
    )
  }

  override def ping(): F[String] = {
    expect(
      raw(NonEmptyList.of(toBS("PING"))),
      {
        case simple: Value.RString => simple.value
      }
    )
  }

  override def clientId(): F[Long] = {
    expect(
      raw(NonEmptyList.of(toBS("CLIENT"), toBS("ID"))),
      {
        case number: Value.RLong => number.value
      }
    )
  }

  override def killClient(filters: NonEmptyList[KillClientFilter], killMe: Boolean): F[Long] = {
    val encodedFilters = filters.map {
      case KillClientFilter.Id(value) => toBS(value.toString())
    }
    expect(
      raw(NonEmptyList.of(toBS("CLIENT"), toBS("KILL"), toBS("ID")) ++ encodedFilters.toList ++ List(
        toBS("SKIPME"),
        toBS(if (killMe) "yes" else "no")
      )),
      {
        case simple: Value.RString => if (simple.value == "OK") 1 else 0
        case number: Value.RLong   => number.value
      }
    )
  }
}

object ClusterClient {

  final case class Config(autoReconnect: Boolean)

  private[scadis] final case class Moved(hash: Int, host: Option[String], port: Int)

  private[scadis] object MovedMatch {
    private val movedRegex = """^MOVED (\d+) ([^:]*):(\d+)$""".r

    def unapply(input: Value): Option[Moved] = {
      input match {
        case err: Value.RError =>
          err.value match {
            case movedRegex(hashOpt, hostOpt, portOpt) =>
              for {
                hash <- Try(hashOpt.toInt).toOption
                host = Option(hostOpt).filter(_.nonEmpty)
                port <- Try(portOpt.toInt).toOption
              } yield Moved(hash, host, port)
            case _ => None
          }

        case _ => None
      }
    }
  }

  final case class Node(host: Option[Host], port: Int, nodeId: String)
  final case class Slot(start: Int, end: Int, master: Node, replicas: List[Node])

  private[scadis] def parseNode(input: Value.RArray): Option[Node] = {
    for {
      hostEntry <-
        input.value.headOption.collect { case bs: Value.RBulkString => bs }
      host =
        Codec.utf8Codec.decode(hostEntry).toOption.flatMap { h => Host.fromString(h) }
      port <-
        input.value.drop(1).headOption.collect { case l: Value.RLong => l.value.toInt }
      nodeIdEntry <-
        input.value.drop(2).headOption.collect { case bs: Value.RBulkString => bs }
      nodeId <-
        Codec.utf8Codec.decode(nodeIdEntry).toOption
    } yield Node(host, port.toInt, nodeId)
  }

  private[scadis] def parseSlot(input: Value.RArray): Option[Slot] = {
    for {
      start       <- input.value.headOption.collect { case l: Value.RLong => l.value.toInt }
      end         <- input.value.drop(1).headOption.collect { case l: Value.RLong => l.value.toInt }
      masterEntry <- input.value.drop(2).headOption.collect { case arr: Value.RArray => arr }
      master      <- parseNode(masterEntry)
    } yield Slot(start.toInt, end.toInt, master, List.empty)
  }

  def make[F[_]: Async: Parallel: LoggerFactory, I, O](
      address: SocketAddress[Host],
      config: Config,
      encoder: Encoder[I],
      decoder: Decoder[O]
  ): Resource[F, ClusterClient[F, I, O]] = {
    for {
      cluster <- Cluster.make[F](address)
      clusterClient =
        new ClusterClientImpl[F, I, O](address, encoder, decoder, config, cluster)
    } yield clusterClient
  }
}
