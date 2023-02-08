package dk.alfabetacain.fs2_redis

import cats.effect.kernel.Deferred
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.io.net.Network
import fs2.text

import java.nio.charset.StandardCharsets
import cats.effect.kernel.Async
import cats.effect.IOApp
import cats.effect.{ ExitCode, IO }
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.typelevel.log4cats.LoggerFactory
import dk.alfabetacain.fs2_redis.codec.Codec
import cats.data.NonEmptyList
import dk.alfabetacain.fs2_redis.parser.Value
import scala.util.Try

object Repl extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    implicit val loggerFactory = Slf4jFactory[IO]
    for {
      host <- IO.fromOption(Host.fromString(args.head))(new RuntimeException(s"Could not parse host: ${args.head}"))
      port <-
        IO.fromOption(Port.fromString(args.tail.head))(new RuntimeException(s"Could not parse port: ${args.tail.head}"))
      _ <- make[IO](host, port, true)
    } yield ExitCode.Success
  }

  private def asString(input: Value): Either[String, String] = {
    input match {
      case Value.RESPInteger(value) => Right(value.toString)
      case Value.RESPError(value) =>
        Left(value)
      case Value.RESPBulkString(data) => Try(new String(data, StandardCharsets.UTF_8)).toEither.left.map(_.toString())
      case Value.SimpleString(value)  => Right(value)
      case Value.RESPArray(elements) =>
        elements.map(asString).sequence[Either[String, *], String].map(_.mkString(","))
      case Value.RESPNull => Left("null")
    }
  }

  def make[F[_]: Async: LoggerFactory](host: Host, port: Port, autoReconnect: Boolean): F[Unit] = {

    val conn = for {
      client <- Client.make(
        Network[F].client(SocketAddress(host, port)),
        Client.Config(autoReconnect = autoReconnect),
        Codec.utf8Codec,
        Codec.utf8Codec,
      )
      isDone <- Resource.eval(Deferred[F, Unit])
    } yield (client, isDone)
    conn.use { case (conn, isDone) =>
      fs2.io.stdinUtf8[F](2048)
        .through(text.lines)
        .evalMap {
          case ":quit" =>
            isDone.complete(()).as(Option.empty[String])
          case cmd =>
            Option(cmd).pure[F]
        }
        .collect { case Some(v) => v }
        .map(_.split(" ").toList.map(_.trim()).filter(_.nonEmpty))
        .map(_.map(Codec.utf8Codec.encode))
        .mapFilter(NonEmptyList.fromList)
        .evalMap(conn.raw)
        .map(asString)
        .map(_.toString + "\n")
        .through(fs2.io.stdoutLines(StandardCharsets.UTF_8))
        .interruptWhen(isDone.get.attempt)
        .compile
        .drain
        .void
    }
  }
}
