package dk.alfabetacain.fs2_redis.codec

import dk.alfabetacain.fs2_redis.parser.Value

import java.nio.charset.StandardCharsets
import scala.util.Try

trait Codec[A, B] {
  def encode(input: A): B
  def decode(input: B): Either[Throwable, A]
}

object Codec {
  type BulkStringCodec[A] = Codec[A, Value.RESPBulkString]

  def utf8Codec: Codec[String, Value.RESPBulkString] = {
    new Codec[String, Value.RESPBulkString] {
      override def encode(input: String): Value.RESPBulkString =
        Value.RESPBulkString(input.getBytes(StandardCharsets.UTF_8))

      override def decode(input: Value.RESPBulkString): Either[Throwable, String] = {
        Try(new String(input.data, StandardCharsets.UTF_8)).toEither
      }
    }
  }
}
