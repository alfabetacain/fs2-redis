package dk.alfabetacain.fs2_redis.parser

import scodec.codecs
import scodec.Codec
import scodec.bits.ByteVector
import java.nio.charset.StandardCharsets
import scodec.bits.BitVector
import scodec.SizeBound
import scodec.Attempt
import scodec.DecodeResult
import scodec.Err
import cats.Show

sealed trait Value

object Value {
  final case class SimpleString(value: String)       extends Value
  final case class RESPArray(elements: List[Value])  extends Value
  final case class RESPInteger(value: Long)          extends Value
  final case class RESPBulkString(data: Array[Byte]) extends Value
  final case class RESPError(value: String)          extends Value
  final case object RESPNull                         extends Value

  object SimpleString {
    implicit val show = Show.fromToString[SimpleString]
  }

  private val simpleStringMarker = BitVector("+".getBytes(StandardCharsets.US_ASCII))
  private val integerMarker      = BitVector(":".getBytes(StandardCharsets.US_ASCII))
  private val bulkStringMarker   = BitVector("$".getBytes(StandardCharsets.US_ASCII))
  private val arrayMarker        = BitVector("*".getBytes(StandardCharsets.US_ASCII))
  private val errorMarker        = BitVector("-".getBytes(StandardCharsets.US_ASCII))
  private val commandTerminator  = ByteVector("\r\n".getBytes(StandardCharsets.US_ASCII))

  private val crlfTerminated: Codec[String] = {
    codecs.filtered(
      codecs.utf8,
      new Codec[BitVector] {
        override def sizeBound: SizeBound = SizeBound.unknown
        override def encode(value: BitVector): Attempt[BitVector] =
          Attempt.successful(value ++ commandTerminator.bits)
        override def decode(bits: BitVector): Attempt[DecodeResult[BitVector]] = {
          bits.bytes.indexOfSlice(commandTerminator) match {
            case -1 => Attempt.failure(Err("Does not contain \\r\\n."))
            case idx => Attempt.successful(DecodeResult(
                bits.take(idx * 8L),
                bits.drop(idx * 8L + commandTerminator.bits.size)
              ))
          }
        }
      }
    )
  }

  val simpleStringCodec: Codec[SimpleString] = {
    codecs.constant(simpleStringMarker).consume(_ => crlfTerminated)(_ => ()).xmap(SimpleString.apply, _.value)
  }

  val errorCodec: Codec[RESPError] = {
    codecs.constant(errorMarker).consume(_ => crlfTerminated)(_ => ()).xmap(RESPError.apply, _.value)
  }

  private val crlfTerminatedLong: Codec[Long] = crlfTerminated.xmap(_.toLong, _.toString)

  val integerCodec: Codec[RESPInteger] = {
    codecs.constant(integerMarker).consume(_ => crlfTerminated)(_ => ())
      .xmap(s => RESPInteger(s.toLong), _.value.toString)
  }

  val nullArrayCodec: Codec[Value] = {
    codecs.constant(ByteVector("*-1\r\n".getBytes(StandardCharsets.US_ASCII)))
      .xmap(_ => RESPNull, _ => ())
  }

  val nullStringCodec: Codec[Value] = {
    codecs.constant(ByteVector("$-1\r\n".getBytes(StandardCharsets.US_ASCII)))
      .xmap(_ => RESPNull, _ => ())
  }

  val bulkStringCodec: Codec[RESPBulkString] = {
    codecs.constant(bulkStringMarker).consume[RESPBulkString](_ =>
      codecs.variableSizeBytesLong(crlfTerminatedLong, codecs.bytes).flatZip(_ =>
        codecs.constant(commandTerminator)
      ).xmap[ByteVector](
        _._1,
        (_, ())
      ).xmap(
        bv => RESPBulkString(bv.toArray),
        bulk => ByteVector(bulk.data)
      )
    )(_.data.size)
  }

  lazy val arrayCodec: Codec[RESPArray] = {
    codecs.constant(arrayMarker).consume(_ => codecs.listOfN(crlfTerminatedLong.xmap(_.toInt, _.toLong), combined))(_ =>
      ()
    ).flatZip(_ => codecs.constant(commandTerminator)).xmap[List[Value]](_._1, (_, ())).xmap(
      data => RESPArray(data),
      _.elements
    )
  }

  lazy val combined: Codec[Value] = {
    codecs.choice(
      simpleStringCodec.widen[Value](
        x => x,
        {
          case s: SimpleString => Attempt.successful(s)
          case other           => Attempt.failure(Err(s"Expected SimpleString, but got $other"))
        }
      ),
      integerCodec.widen[Value](
        x => x,
        {
          case s: RESPInteger => Attempt.successful(s)
          case other          => Attempt.failure(Err(s"Expected RESPInteger, but got $other"))
        }
      ),
      bulkStringCodec.widen[Value](
        x => x,
        {
          case s: RESPBulkString => Attempt.successful(s)
          case other             => Attempt.failure(Err(s"Expected RESPBulkString, but got $other"))

        }
      ),
      arrayCodec.widen[Value](
        x => x,
        {
          case s: RESPArray => Attempt.successful(s)
          case other        => Attempt.failure(Err(s"Expected RESPArray, but got $other"))
        }
      ),
      errorCodec.widen[Value](
        x => x,
        {
          case s: RESPError => Attempt.successful(s)
          case other        => Attempt.failure(Err(s"Expected RESPError, but got $other"))
        }
      ),
      nullStringCodec,
      nullArrayCodec,
    )
  }
}
