package dk.alfabetacain.fs2_redis

import cats.effect.kernel.Async
import cats.effect.kernel.Resource
import cats.syntax.all._
import dk.alfabetacain.fs2_redis.parser.Value
import fs2.io.net.Socket

import java.nio.charset.StandardCharsets

trait Client[F[_]] {
  def raw(command: String*): F[Value]
}

object Client {

  final case class Config(autoReconnect: Boolean)

  def make[F[_]: Async](connect: Resource[F, Socket[F]], config: Config): Resource[F, Client[F]] = {
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
      }
    }
  }
}
