package dk.alfabetacain.fs2_redis

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.kernel.Deferred
import cats.effect.kernel.Resource
import cats.effect.std.Console
import cats.effect.std.Queue
import cats.syntax.all._
import com.comcast.ip4s._
import dk.alfabetacain.fs2_redis.parser.Value
import fs2.Chunk
import fs2.Pipe
import fs2.Stream
import fs2.interop.scodec.StreamDecoder
import fs2.io.net.Network
import fs2.io.net.Socket
import fs2.text

import java.nio.charset.StandardCharsets
import scala.concurrent.duration._

object Main extends IOApp {

  def makeConnection2(socket: Socket[IO]): Resource[IO, Pipe[IO, String, Value]] = {
    for {
      queue <- Resource.eval(Queue.bounded[IO, Deferred[IO, Value]](10))
      consumer = socket.reads.through(StreamDecoder.many(parser.Value.combined).toPipeByte).evalMap { response =>
        for {
          nextWaiting <- queue.take
          _           <- nextWaiting.complete(response)
        } yield ()
      }
    } yield _.evalMap { command =>
      for {
        responseDeferred <- Deferred[IO, Value]
        value =
          Value.RESPArray(
            command.split(" ").toList.filter(_.nonEmpty).map(v =>
              Value.RESPBulkString(v.getBytes(StandardCharsets.UTF_8))
            )
          )

      } yield (value, responseDeferred)
    }
      .groupWithin(10, 0.seconds).evalMapChunk { chunk =>
        chunk.map { case (cmd, deferred) => queue.offer(deferred).as(cmd) }.toList.sequence
      }.evalMap { cmds =>
        IO {
          cmds.toArray.map(parser.Value.combined.encode).map(_.toOption.get.toByteArray)
            .flatten
        }
      }
      .flatMap(cmdBytes => Stream.chunk(Chunk.array(cmdBytes)))
      .through(socket.writes)
      .concurrently(consumer)
  }

  private def repl(connection: Client[IO]): IO[ExitCode] = {
    for {
      input <- Console[IO].readLine
      isDone <- input match {
        case ":quit" => IO.pure(true)
        case cmd =>
          for {
            response <- connection.raw(
              cmd
            )
            _ <- Console[IO].println(response)
          } yield false
      }
      _ <-
        if (isDone) {
          IO.unit
        } else {
          repl(connection)
        }
    } yield ExitCode.Success
  }

  override def run(args: List[String]): IO[ExitCode] = {

    val conn = for {
      socketConn <- Network[IO].client(SocketAddress(host"localhost", port"6379"))
      conn       <- makeConnection2(socketConn)
      isDone     <- Resource.eval(Deferred[IO, Unit])
    } yield (conn, isDone)
    conn.use { case (conn, isDone) =>
      fs2.io.stdinUtf8[IO](1024)
        .through(text.lines)
        .evalMap {
          case ":quit" =>
            isDone.complete(()).as(Option.empty[String])
          case cmd =>
            IO.pure(Option(cmd))
        }
        .collect { case Some(v) => v }
        .through(conn)
        .map(_.toString + "\n")
        .through(fs2.io.stdoutLines(StandardCharsets.UTF_8))
        .interruptWhen(isDone.get.attempt)
        .compile
        .drain
        .as(ExitCode.Success)
    }
    // conn.use(repl)
    /*
    Network[IO].client(SocketAddress(host"localhost", port"6379")).use { socket =>
      for {
        _ <- socket.write(Chunk.array(parser.Value.combined.encode(
          Value.RESPArray(List(Value.RESPBulkString("PING".getBytes(StandardCharsets.US_ASCII))))
        ).toOption.get.toByteArray))
        _ <- socket.reads.through(StreamDecoder.once(parser.Value.combined).toPipeByte).foreach(res =>
          IO.println(s"Got response: '$res'")
        ).compile.drain
      } yield ExitCode.Success
    }
     */
  }
}
