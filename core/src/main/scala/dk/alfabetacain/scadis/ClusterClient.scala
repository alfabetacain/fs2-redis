package dk.alfabetacain.scadis

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.effect.kernel.Deferred
import cats.effect.kernel.Ref
import cats.effect.kernel.Resource
import cats.effect.std.Supervisor
import cats.syntax.all._
import com.comcast.ip4s.Host
import com.comcast.ip4s.SocketAddress
import dk.alfabetacain.scadis.Client.KillClientFilter
import dk.alfabetacain.scadis.Util.expect
import dk.alfabetacain.scadis.codec.Codec
import dk.alfabetacain.scadis.codec.Codec.BulkStringCodec
import dk.alfabetacain.scadis.parser.Value
import fs2.io.net.Network
import org.typelevel.log4cats.LoggerFactory

import java.nio.charset.StandardCharsets
import scala.util.Try

trait ClusterClient[F[_], I, O] {
  def get(key: I): F[Option[O]]
  def set(key: I, value: I): F[Boolean]
  def ping(): F[String]
  def clientId(): F[Long]
  def killClient(filters: NonEmptyList[KillClientFilter], killMe: Boolean): F[Long]
  def raw(arguments: NonEmptyList[Value.RESPBulkString]): F[Value]
}

private[scadis] class ClusterClientImpl[F[_]: Async: LoggerFactory, I, O](
    mainAddress: SocketAddress[Host],
    mainConnection: Client[F, String, String],
    supervisor: Supervisor[F],
    clients: Ref[F, Map[SocketAddress[Host], F[Client[F, I, O]]]],
    inputCodec: BulkStringCodec[I],
    outputCodec: BulkStringCodec[O],
    config: ClusterClient.Config
) extends ClusterClient[F, I, O] {

  private val log = LoggerFactory.getLogger[F]

  override def raw(
      arguments: NonEmptyList[Value.RESPBulkString],
  ): F[Value] = privateRaw(arguments)

  private def privateRaw(
      arguments: NonEmptyList[Value.RESPBulkString],
      address: Option[SocketAddress[Host]] = None
  ): F[Value] = {
    val usedAddress = address.getOrElse(mainAddress)
    for {
      _          <- log.info(s"Searching using address: $usedAddress")
      someClient <- getClient(usedAddress)
      sendResult <- someClient.raw(arguments)
      actualResult <- sendResult match {
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
            .flatMap(newAddress => privateRaw(arguments, Some(newAddress)))

        case _ =>
          Async[F].pure(sendResult)
      }
    } yield actualResult
  }

  private def toBS(input: String): Value.RESPBulkString =
    Value.RESPBulkString(input.getBytes(StandardCharsets.US_ASCII))

  private def getClient(address: SocketAddress[Host]): F[Client[F, I, O]] = {
    for {
      deferred <- Deferred[F, Client[F, I, O]]
      result <- clients.modify { map =>
        map.get(address) match {
          case None => (map, Left(deferred))
          case Some(value) =>
            (map, Right(value))
        }
      }
      client <- result match {
        case Left(value) =>
          for {
            _ <- supervisor.supervise(Client.make(
              Network[F].client(address),
              Client.Config(autoReconnect = config.autoReconnect),
              inputCodec,
              outputCodec
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
    } yield client
  }

  private def encode(input: I): Value.RESPBulkString = inputCodec.encode(input)

  override def get(key: I): F[Option[O]] = {
    expect(
      raw(NonEmptyList.of(toBS("GET"), encode(key))),
      {
        case Value.RESPNull               => Option.empty
        case result: Value.RESPBulkString => outputCodec.decode(result).toOption
      }
    )
  }

  override def set(key: I, value: I): F[Boolean] = {
    expect(
      raw(NonEmptyList.of(toBS("SET"), encode(key), encode(value))),
      {
        case simple: Value.SimpleString => simple.value == "OK"
      }
    )
  }

  override def ping(): F[String] = {
    expect(
      raw(NonEmptyList.of(toBS("PING"))),
      {
        case simple: Value.SimpleString => simple.value
      }
    )
  }

  override def clientId(): F[Long] = {
    expect(
      raw(NonEmptyList.of(toBS("CLIENT"), toBS("ID"))),
      {
        case number: Value.RESPInteger => number.value
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
        case simple: Value.SimpleString => if (simple.value == "OK") 1 else 0
        case number: Value.RESPInteger  => number.value
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
        case err: Value.RESPError =>
          err.value match {
            case movedRegex(hashOpt, hostOpt, portOpt) =>
              println(s"got hits: $hashOpt, $hostOpt, $portOpt")
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

  def make[F[_]: Async: LoggerFactory, I, O](
      address: SocketAddress[Host],
      config: Config,
      inputCodec: BulkStringCodec[I],
      outputCodec: BulkStringCodec[O]
  ): Resource[F, ClusterClient[F, I, O]] = {
    val connect = Network[F].client(address)
    for {
      mainClient <- Client.make(connect, Client.Config(config.autoReconnect), Codec.utf8Codec, Codec.utf8Codec)
      supervisor <- Supervisor[F]
      ref <-
        Resource.eval(Ref.of[F, Map[SocketAddress[Host], F[Client[F, I, O]]]](Map.empty))
    } yield new ClusterClientImpl[F, I, O](address, mainClient, supervisor, ref, inputCodec, outputCodec, config)
  }
}
