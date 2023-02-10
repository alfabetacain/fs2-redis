package dk.alfabetacain.scadis.parser

import cats.Show
import cats.kernel.Eq
import scodec.Attempt
import scodec.Codec
import scodec.DecodeResult
import scodec.Err
import scodec.SizeBound
import scodec.bits.BitVector
import scodec.bits.ByteVector
import scodec.codecs

import java.nio.charset.StandardCharsets

sealed trait Value

object Value {
  final case class RString(value: String)          extends Value
  final case class RArray(value: List[Value])      extends Value
  final case class RLong(value: Long)              extends Value
  final case class RBulkString(value: Array[Byte]) extends Value
  final case class RError(value: String)           extends Value
  final case object RNull                          extends Value

  implicit val eqValue: Eq[Value] = Eq.instance {
    case (Value.RNull, Value.RNull)                              => true
    case (Value.RString(leftString), Value.RString(rightString)) => Eq[String].eqv(leftString, rightString)
    case (Value.RLong(leftNumber), Value.RLong(rightNumber))     => Eq[Long].eqv(leftNumber, rightNumber)
    case (leftErr: Value.RError, rightErr: Value.RError)         => Eq[String].eqv(leftErr.value, rightErr.value)
    case (leftBS: Value.RBulkString, rightBS: Value.RBulkString) =>
      Eq[List[Byte]].eqv(leftBS.value.toList, rightBS.value.toList)
    case (leftArr: Value.RArray, rightArr: Value.RArray) =>
      leftArr.value.zip(rightArr.value).map(tuple => eqValue.eqv(tuple._1, tuple._2)).fold(true)(_ && _)
    case _ => false
  }

  implicit val showRString: Show[RString]         = Show.fromToString
  implicit val showRLong: Show[RLong]             = Show.fromToString
  implicit val showRError: Show[RError]           = Show.fromToString
  implicit val showRBulkString: Show[RBulkString] = Show.show(bs => "RBulkString(" + ByteVector(bs.value).toHex + ")")

  implicit lazy val showArray: Show[RArray] = Show.show(_.value.map(Show[Value].show).mkString("\n"))

  implicit lazy val showValue: Show[Value] = Show.show {
    case s: RString      => showRString.show(s)
    case bs: RBulkString => showRBulkString.show(bs)
    case number: RLong   => showRLong.show(number)
    case RNull           => "RNull"
    case error: RError   => showRError.show(error)
    case array: RArray   => showArray.show(array)
  }

  object RString {
    implicit val show = Show.fromToString[RString]
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

  val simpleStringCodec: Codec[RString] = {
    codecs.constant(simpleStringMarker).withContext("simple string marker").consume(_ => crlfTerminated)(_ => ()).xmap(
      RString.apply,
      _.value
    )
  }

  val errorCodec: Codec[RError] = {
    codecs.constant(errorMarker).withContext("error marker").consume(_ => crlfTerminated)(_ => ()).xmap(
      RError.apply,
      _.value
    )
  }

  private val crlfTerminatedLong: Codec[Long] = crlfTerminated.xmap(_.toLong, _.toString)

  val integerCodec: Codec[RLong] = {
    codecs.constant(integerMarker).withContext("number marker").consume(_ => crlfTerminated)(_ => ())
      .xmap(s => RLong(s.toLong), _.value.toString)
  }

  val nullArrayCodec: Codec[Value] = {
    codecs.constant(ByteVector("*-1\r\n".getBytes(StandardCharsets.US_ASCII))).withContext("null array")
      .xmap(_ => RNull, _ => ())
  }

  val nullStringCodec: Codec[Value] = {
    codecs.constant(ByteVector("$-1\r\n".getBytes(StandardCharsets.US_ASCII))).withContext("null bulk string")
      .xmap(_ => RNull, _ => ())
  }

  val bulkStringCodec: Codec[RBulkString] = {
    codecs.constant(bulkStringMarker).withContext("bulk string marker").consume[RBulkString](_ =>
      codecs.variableSizeBytesLong(crlfTerminatedLong, codecs.bytes).flatZip(_ =>
        codecs.constant(commandTerminator)
      ).xmap[ByteVector](
        _._1,
        (_, ())
      ).xmap(
        bv => RBulkString(bv.toArray),
        bulk => ByteVector(bulk.value)
      )
    )(_.value.size)
  }

  lazy val arrayCodec: Codec[RArray] = {
    codecs.constant(arrayMarker).withContext("array marker").consume(_ =>
      codecs.listOfN(crlfTerminatedLong.xmap(_.toInt, _.toLong), combined)
    )(_ =>
      ()
    ).xmap(
      data => RArray(data),
      _.value
    )
  }

  lazy val combined: Codec[Value] = {
    codecs.choice(
      simpleStringCodec.widen[Value](
        x => x,
        {
          case s: RString => Attempt.successful(s)
          case other      => Attempt.failure(Err(s"Expected SimpleString, but got $other"))
        }
      ),
      integerCodec.widen[Value](
        x => x,
        {
          case s: RLong => Attempt.successful(s)
          case other    => Attempt.failure(Err(s"Expected RESPInteger, but got $other"))
        }
      ),
      bulkStringCodec.widen[Value](
        x => x,
        {
          case s: RBulkString => Attempt.successful(s)
          case other          => Attempt.failure(Err(s"Expected RESPBulkString, but got $other"))

        }
      ),
      arrayCodec.widen[Value](
        x => x,
        {
          case s: RArray => Attempt.successful(s)
          case other     => Attempt.failure(Err(s"Expected RESPArray, but got $other"))
        }
      ),
      errorCodec.widen[Value](
        x => x,
        {
          case s: RError => Attempt.successful(s)
          case other     => Attempt.failure(Err(s"Expected RESPError, but got $other"))
        }
      ),
      nullStringCodec,
      nullArrayCodec,
    )
  }
}
