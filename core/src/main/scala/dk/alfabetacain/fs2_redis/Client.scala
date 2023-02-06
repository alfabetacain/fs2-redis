package dk.alfabetacain.fs2_redis

import cats.effect.kernel.Async
import cats.effect.kernel.Resource
import cats.syntax.all._
import dk.alfabetacain.fs2_redis.parser.Value
import fs2.io.net.Socket

import java.nio.charset.StandardCharsets
import cats.MonadThrow
import org.typelevel.log4cats.LoggerFactory

trait Client[F[_]] {
  def raw(command: String*): F[Value]
  def get(key: String): F[Option[String]]
  def set(key: String, value: String): F[Boolean]
}

object Client {

  final case class Config(autoReconnect: Boolean)

  private def expect[F[_]: MonadThrow, A](action: F[Value], mapper: PartialFunction[Value, A]): F[A] = {
    action.flatMap { result =>
      if (mapper.isDefinedAt(result)) {
        mapper(result).pure[F]
      } else {
        MonadThrow[F].raiseError(new IllegalArgumentException(s"Unexpected unparsable response: $result"))
      }
    }
  }

  def make[F[_]: Async: LoggerFactory](connect: Resource[F, Socket[F]], config: Config): Resource[F, Client[F]] = {
    Connection.make[F](connect, config.autoReconnect).map { conn =>
      new Client[F] {
        override def raw(command: String*): F[Value] = {
          for {
            encoded <-
              Async[F].delay {
                Value.RESPArray(
                  command.toList.filter(_.nonEmpty).map(v =>
                    Value.RESPBulkString(v.getBytes(StandardCharsets.UTF_8))
                  )
                )
              }
            sendResult <- conn.send(encoded)
          } yield sendResult
        }

        override def get(key: String): F[Option[String]] = {
          expect(
            raw("GET", key),
            {
              case simple: Value.SimpleString => Option(simple.value)
              case bulk: Value.RESPBulkString => Option(new String(bulk.data, StandardCharsets.UTF_8))
              case Value.RESPNull             => Option.empty
            }
          )
        }

        override def set(key: String, value: String): F[Boolean] = {
          expect(
            raw("SET", key, value),
            {
              case simple: Value.SimpleString => simple.value == "OK"
            }
          )
        }
      }
    }
  }
}
