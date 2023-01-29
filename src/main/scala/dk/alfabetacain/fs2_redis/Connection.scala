package dk.alfabetacain.fs2_redis

import cats.effect.kernel.Async
import cats.effect.kernel.Deferred
import cats.effect.kernel.Resource
import cats.effect.std.Queue
import cats.effect.std.Supervisor
import cats.effect.syntax.all._
import cats.syntax.all._
import dk.alfabetacain.fs2_redis.parser.Value
import fs2.Chunk
import fs2.Stream
import fs2.concurrent.Channel
import fs2.interop.scodec.StreamDecoder
import fs2.io.net.Socket
import cats.MonadThrow

trait Connection[F[_]] {
  def send(input: Value.RESPArray): F[Value]
}

object Connection {

  class ConnectionClosedException() extends RuntimeException

  final case class QueueItem[F[_]](cmd: Value.RESPArray, onComplete: Deferred[F, Either[Throwable, Value]])

  def make[F[_]: Async](
      connect: Resource[F, Socket[F]],
      autoReconnect: Boolean = false
  ): Resource[F, Connection[F]] = {
    Supervisor[F].flatMap { supervisor =>
      for {
        channel <- Resource.eval(Channel.bounded[F, QueueItem[F]](10))
        queue   <- Resource.eval(Queue.bounded[F, QueueItem[F]](10))
        _       <- Resource.eval(setup(Stream.empty, supervisor, channel, queue, connect, autoReconnect))
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
        _ <- Resource.onFinalize(channel.closed)
        _ <- Resource.onFinalize(channel.close.void)
      } yield conn
    }
  }

  private def queueToList[F[_]: Async, A](queue: Queue[F, A], acc: List[A]): F[List[A]] = {
    queue.tryTake.flatMap {
      case None => acc.pure[F]
      case Some(item) =>
        queueToList(queue, item :: acc)
    }
  }

  private def reader[F[_]: Async](
      socket: Socket[F],
      supervisor: Supervisor[F],
      queue: Queue[F, QueueItem[F]],
      interruptOn: F[Either[Throwable, Unit]],
      onComplete: F[Unit],
  ): F[Unit] = {
    val log = scribe.cats[F]
    socket.reads.through(StreamDecoder.many(parser.Value.combined).toPipeByte).evalMap { response =>
      for {
        nextWaiting <- queue.take
        _           <- nextWaiting.onComplete.complete(Right(response))
      } yield ()
    }.interruptWhen(interruptOn)
      .compile.drain.guaranteeCase {
        case err =>
          for {
            _ <- log.error(s"Writer failed. Shutting down... ($err)")
            _ <- onComplete
          } yield ()
      }
  }

  private def writer[F[_]: Async](
      outstanding: Stream[F, QueueItem[F]],
      socket: Socket[F],
      channel: Channel[F, QueueItem[F]],
      queue: Queue[F, QueueItem[F]],
      interruptOn: F[Either[Throwable, Unit]],
      onComplete: F[Unit],
  ): F[Unit] = {
    val log = scribe.cats[F]
    (outstanding ++ channel.stream)
      .evalMap { case QueueItem(cmd, deferred) =>
        for {
          _ <- queue.offer(QueueItem(cmd, deferred))
        } yield Value.combined.encode(cmd).toOption.get.toByteArray
      }
      .flatMap(bytes => Stream.chunk(Chunk.array(bytes)))
      .through(socket.writes)
      .interruptWhen(interruptOn)
      .compile.drain.guaranteeCase {
        case err =>
          for {
            _ <- log.error(s"Writer failed. Shutting down... ($err)")
            _ <- onComplete
          } yield ()
      }
  }

  private def onStreamTermination[F[_]: Async](
      supervisor: Supervisor[F],
      channel: Channel[F, QueueItem[F]],
      queue: Queue[F, QueueItem[F]],
      connect: Resource[F, Socket[F]],
      autoReconnect: Boolean,
      signalShutdown: F[Unit],
      writerCompleted: F[Unit],
      readerCompleted: F[Unit],
  ): F[Unit] = {
    val log = scribe.cats[F]
    for {
      _ <- log.info("Signaling shutdown")
      _ <- signalShutdown
      _ <- writerCompleted
      _ <- log.info("Writer completed")
      _ <- readerCompleted
      _ <- log.info("Reader completed")
      _ <-
        if (!autoReconnect) {
          for {
            _ <- channel.close
            _ <- channel.closed
          } yield ()
        } else {
          Async[F].unit
        }
      outstanding <- queueToList(queue, List.empty)
      _ <-
        if (autoReconnect) {
          for {
            _ <- log.info("Reconnecting...")
            _ <- outstanding.map(queue.offer).sequence
            _ <- setup(Stream(outstanding: _*), supervisor, channel, queue, connect, autoReconnect)
          } yield ()
        } else {
          outstanding.map { item =>
            for {
              _ <- log.info("Completing queued task")
              errorResult = Left(new ConnectionClosedException)
              _ <- item.onComplete.complete(errorResult)
            } yield ()
          }.sequence
        }
      _ <- log.info("Done with error handling")
    } yield ()
  }

  private def setup[F[_]: Async](
      outstanding: Stream[F, QueueItem[F]],
      supervisor: Supervisor[F],
      channel: Channel[F, QueueItem[F]],
      queue: Queue[F, QueueItem[F]],
      connect: Resource[F, Socket[F]],
      autoReconnect: Boolean
  ): F[Unit] = {
    val log = scribe.cats[F]
    Deferred[F, Unit].flatMap { whenDone =>
      val action: F[Unit] = connect.use { socket =>
        for {
          blocker        <- Deferred[F, Unit]
          shutdownSignal <- Deferred[F, Unit]
          writerDone <- supervisor.supervise(
            writer(
              outstanding,
              socket,
              channel,
              queue,
              shutdownSignal.get.attempt,
              blocker.complete(()).void
            )
          )

          readerDone <- supervisor.supervise(reader(
            socket,
            supervisor,
            queue,
            shutdownSignal.get.attempt,
            blocker.complete(()).void
          ))
          _ <- supervisor.supervise {
            val action = for {
              _ <- blocker.get
              _ <- onStreamTermination(
                supervisor,
                channel,
                queue,
                connect,
                autoReconnect,
                shutdownSignal.complete(()).void,
                writerDone.join.void,
                readerDone.join.void
              )
            } yield ()
            action.guarantee(whenDone.complete(()).void)
          }
          _ <- log.info("Awaiting termination...")
          _ <- whenDone.get
          _ <- log.info("Terminated")
        } yield ()
      }
      supervisor.supervise(action).void
    }
  }
}
