package dk.alfabetacain.fs2_redis

import fs2.Stream
import cats.effect.syntax.all._
import cats.syntax.all._
import dk.alfabetacain.fs2_redis.parser.Value
import cats.effect.kernel.Async
import fs2.io.net.Socket
import cats.effect.kernel.Resource
import cats.effect.std.Queue
import cats.effect.kernel.Deferred
import fs2.interop.scodec.StreamDecoder
import java.nio.charset.StandardCharsets
import fs2.Chunk
import fs2.concurrent.Channel

trait Client[F[_]] {
  def raw(command: String*): F[Value]
}

object Client {

  final case class Config(autoReconnect: Boolean)

  private def emptyQueue[F[_]: Async](
      queue: Queue[F, Deferred[F, Value]],
  ): F[Unit] = {
    val log = scribe.cats[F]
    queue.tryTake.flatMap {
      case None => Async[F].unit
      case Some(deferred) =>
        for {
          _ <- log.info("Completing queued task")
          _ <- deferred.complete(Value.RESPNull)
          _ <- emptyQueue[F](queue)
        } yield ()
    }
  }

  private def shutdown[F[_]: Async](
      signalShutdown: F[Unit],
      writerDone: F[Unit],
      readerDone: F[Unit],
      queue: Queue[F, Deferred[F, Value]],
      channel: Channel[F, (Value.RESPArray, Deferred[F, Value])]
  ): F[Unit] = {
    val log = scribe.cats[F]
    for {
      _ <- signalShutdown
      // _ <- channel.close
      _ <- log.info("channel closed...")
      _ <- writerDone
      _ <- log.info("writer done...")
      _ <- readerDone
      _ <- log.info("reader done...")
      _ <- emptyQueue(queue)
      _ <- log.info("queue empty...")
    } yield ()
  }

  def make2[F[_]: Async](connect: Resource[F, Socket[F]], config: Config): Resource[F, Client[F]] = {
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

  def make[F[_]: Async](socket: Socket[F], config: Config): Resource[F, Client[F]] = {
    val log = scribe.cats[F]
    for {
      queue          <- Resource.eval(Queue.bounded[F, Deferred[F, Value]](10))
      shutdownSignal <- Resource.eval(Deferred[F, Unit])
      writerDone     <- Resource.eval(Deferred[F, Unit])
      consumerDone   <- Resource.eval(Deferred[F, Unit])
      inputChannel   <- Resource.eval(Channel.bounded[F, (Value.RESPArray, Deferred[F, Value])](10))
      consumer <- socket.reads.through(StreamDecoder.many(parser.Value.combined).toPipeByte).evalMap { response =>
        for {
          nextWaiting <- queue.take
          _           <- nextWaiting.complete(response)
        } yield ()
      }.interruptWhen(shutdownSignal.get.attempt)
        .compile.drain.guaranteeCase {
          case err =>
            for {
              _ <- log.error(s"Writer failed. Shutting down... ($err)")
              _ <- consumerDone.complete(())
              _ <- shutdown[F](shutdownSignal.complete(()).void, writerDone.get, consumerDone.get, queue, inputChannel)
            } yield ()
        }.guarantee(consumerDone.complete(()).void).background

      writer <- inputChannel.stream.evalMap { case (cmd, deferred) =>
        for {
          _ <- queue.offer(deferred)
        } yield Value.combined.encode(cmd).toOption.get.toByteArray
      }.flatMap(bytes => Stream.chunk(Chunk.array(bytes))).through(socket.writes)
        .interruptWhen(shutdownSignal.get.attempt)
        .compile.drain.guaranteeCase {
          case err =>
            for {
              _ <- log.error(s"Writer failed. Shutting down... ($err)")
              _ <- writerDone.complete(())
              _ <- shutdown[F](shutdownSignal.complete(()).void, writerDone.get, consumerDone.get, queue, inputChannel)
            } yield ()
        }.guarantee(writerDone.complete(()).void).background

    } yield new Client[F] {
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
          deferred   <- Deferred[F, Value]
          sendResult <- inputChannel.send((encoded, deferred))
          output <-
            sendResult.fold[F[Value]](
              _ => Async[F].raiseError(new RuntimeException("connection closed!")),
              _ => deferred.get
            )
        } yield output
      }
    }
  }
}
