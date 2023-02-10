package dk.alfabetacain.scadis

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.effect.kernel.Resource
import cats.effect.kernel.Sync
import cats.syntax.all._
import dk.alfabetacain.scadis.Client.KillClientFilter
import dk.alfabetacain.scadis.Util.expect
import dk.alfabetacain.scadis.Util.toBS
import dk.alfabetacain.scadis.codec.Decoder
import dk.alfabetacain.scadis.codec.Encoder
import dk.alfabetacain.scadis.parser.Value
import fs2.io.net.Socket
import org.typelevel.log4cats.LoggerFactory

trait Client[F[_], I, O] {
  def get(key: I): F[Option[O]]
  def set(key: I, value: I): F[Boolean]
  def ping(): F[String]
  def clientId(): F[Long]
  def killClient(filters: NonEmptyList[KillClientFilter], killMe: Boolean): F[Long]
  def raw(arguments: NonEmptyList[Value.RBulkString]): F[Value]
}

private[scadis] class ClientImpl[F[_]: Sync, I, O](
    conn: Connection[F],
    encoder: Encoder[I],
    decoder: Decoder[O]
) extends Client[F, I, O] {

  override def raw(arguments: NonEmptyList[Value.RBulkString]): F[Value] = {
    for {
      sendResult <- conn.send(arguments)
    } yield sendResult
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

object Client {

  final case class Config(autoReconnect: Boolean)

  sealed trait KillClientFilter

  object KillClientFilter {
    final case class Id(value: Long) extends KillClientFilter
  }

  def make[F[_]: Async: LoggerFactory, I, O](
      connect: Resource[F, Socket[F]],
      config: Config,
      encoder: Encoder[I],
      decoder: Decoder[O]
  ): Resource[F, Client[F, I, O]] = {
    Connection.make[F](connect, config.autoReconnect).map { conn =>
      new ClientImpl[F, I, O](conn, encoder, decoder)
    }
  }
}
