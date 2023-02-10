package dk.alfabetacain.scadis

import cats.MonadThrow
import cats.effect.kernel.Async
import cats.effect.kernel.Deferred
import cats.effect.kernel.Resource
import cats.effect.std.Queue
import cats.effect.syntax.all._
import cats.syntax.all._
import dk.alfabetacain.scadis.parser.Value
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.Channel
import fs2.interop.scodec.StreamDecoder
import fs2.io.net.Socket
import org.typelevel.log4cats.LoggerFactory

trait Connection[F[_]] {
  def send(input: Value.RESPArray): F[Value]
}

object Connection {

  class ConnectionClosedException() extends RuntimeException

  final case class QueueItem[F[_]](cmd: Value.RESPArray, onComplete: Deferred[F, Either[Throwable, Value]])

  def make[F[_]: Async: LoggerFactory](
      connect: Resource[F, Socket[F]],
      autoReconnect: Boolean = false
  ): Resource[F, Connection[F]] = {
    for {
      channel <- Resource.eval(Channel.bounded[F, QueueItem[F]](10))
      queue   <- Resource.eval(Queue.bounded[F, QueueItem[F]](10))
      connectionFiber <- runStreams(Stream.empty, channel, queue, connect, autoReconnect).guarantee(shutdown[F](
        channel,
        queue
      )).background
      conn = new Connection[F] {
        override def send(input: Value.RESPArray): F[Value] = {
          for {
            onComplete <- Deferred[F, Either[Throwable, Value]]
            sendResult <- channel.send(QueueItem(input, onComplete))
            _ <- MonadThrow[F].fromEither(sendResult.leftMap(_ => new RuntimeException("channel already closed!")))
            maybeResult <- onComplete.get
            result      <- MonadThrow[F].fromEither(maybeResult)
          } yield result
        }
      }
      _ <- Resource.onFinalize(shutdown(channel, queue))
    } yield conn
  }

  private def shutdown[F[_]: Async](
      channel: Channel[F, QueueItem[F]],
      queue: Queue[F, QueueItem[F]]
  ): F[Unit] = {
    for {
      _ <- channel.close
      _ <- channel.closed
      // drain the channel in case some items were in flight but not queued
      channelItems <- channel.stream.compile.toList
      queueItems   <- drainQueue(queue)
      _ <-
        (channelItems ++ queueItems).map { item =>
          val errorResult = Left(new ConnectionClosedException)
          for {
            _ <- item.onComplete.complete(errorResult)
          } yield ()
        }.sequence
    } yield ()
  }

  private def drainQueue[F[_]: Async, A](queue: Queue[F, A]): F[List[A]] = {
    Stream.unfoldEval(queue)(_.tryTake.map(_.map((_, queue))))
      .compile.toList
  }

  private def reader[F[_]: Async](
      socket: Socket[F],
      queue: Queue[F, QueueItem[F]],
  ): F[Unit] = {
    socket.reads.through(StreamDecoder.many(parser.Value.combined).toPipeByte).evalMap { response =>
      for {
        nextWaiting <- queue.take
        _           <- nextWaiting.onComplete.complete(Right(response))
      } yield ()
    }.compile.drain
  }

  private def writer[F[_]: Async](
      outstanding: Stream[F, QueueItem[F]],
      socket: Socket[F],
      channel: Channel[F, QueueItem[F]],
      queue: Queue[F, QueueItem[F]],
  ): F[Unit] = {
    (outstanding ++ channel.stream)
      .evalMap { case QueueItem(cmd, deferred) =>
        for {
          _ <- queue.offer(QueueItem(cmd, deferred))
        } yield Value.combined.encode(cmd).toOption.get.toByteArray
      }
      .flatMap(bytes => Stream.chunk(Chunk.array(bytes)))
      .through(socket.writes)
      .compile
      .drain
  }

  private def runStreams[F[_]: Async: LoggerFactory](
      outstanding: Stream[F, QueueItem[F]],
      channel: Channel[F, QueueItem[F]],
      queue: Queue[F, QueueItem[F]],
      connect: Resource[F, Socket[F]],
      autoReconnect: Boolean,
  ): F[Unit] = {
    val log = LoggerFactory[F].getLogger
    for {
      _ <- connect.use { socket =>
        val fibers = for {
          writerFiber <-
            writer(
              outstanding,
              socket,
              channel,
              queue,
            ).background
          readerFiber <-
            reader(socket, queue).background
        } yield (readerFiber, writerFiber)
        fibers.use { case (writerFiber, readerFiber) =>
          Async[F].race(writerFiber, readerFiber)
        }
      }
      _ <-
        if (autoReconnect) {
          for {
            _           <- log.info(s"Reconnecting...")
            outstanding <- drainQueue(queue)
            _           <- runStreams(Stream(outstanding: _*), channel, queue, connect, autoReconnect)
          } yield ()
        } else {
          Async[F].unit
        }
    } yield ()
  }
}
