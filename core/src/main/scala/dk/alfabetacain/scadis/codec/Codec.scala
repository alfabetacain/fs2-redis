package dk.alfabetacain.scadis.codec

import dk.alfabetacain.scadis.parser.Value

import java.nio.charset.StandardCharsets
import scala.util.Try

trait Codec[A] extends Encoder[A] with Decoder[A] {
  def encode(input: A): Value.RBulkString
  def decode(input: Value.RBulkString): Either[Throwable, A]
}

trait Encoder[A] {
  def encode(input: A): Value.RBulkString
}

trait Decoder[A] {
  def decode(input: Value.RBulkString): Either[Throwable, A]
}

object Codec {

  def utf8Codec: Codec[String] = {
    new Codec[String] {
      override def encode(input: String): Value.RBulkString =
        Value.RBulkString(input.getBytes(StandardCharsets.UTF_8))

      override def decode(input: Value.RBulkString): Either[Throwable, String] = {
        Try(new String(input.value, StandardCharsets.UTF_8)).toEither
      }
    }
  }
}
