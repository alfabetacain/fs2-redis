package dk.alfabetacain.scadis.parser

import cats.effect.IO
import org.scalacheck.Gen
import scodec.bits.BitVector
import weaver._
import weaver.scalacheck._

import java.nio.charset.StandardCharsets

object ParserSuite extends SimpleIOSuite with Checkers {

  private lazy val rStringGen     = Gen.alphaNumStr.map(Value.RString.apply)
  private lazy val rBulkStringGen = Gen.alphaNumStr.map(s => Value.RBulkString(s.getBytes(StandardCharsets.US_ASCII)))
  private lazy val rLongGen       = Gen.choose(-1000, 1000).map(int => Value.RLong(int))
  private lazy val rNullGen       = Gen.const(Value.RNull)
  private lazy val rErrorGen      = Gen.alphaNumStr.map(Value.RError.apply)

  private def rArrayGen(depth: Int): Gen[Value.RArray] =
    Gen.lzy(Gen.listOf(rValueGen(depth)).map(values => Value.RArray(values)))

  // using depth to prevent stackoverflows
  private def rValueGen(depth: Int): Gen[Value] =
    if (depth > 0) {
      Gen.lzy(Gen.oneOf(
        rStringGen,
        rBulkStringGen,
        rLongGen,
        rNullGen,
        rErrorGen,
        rArrayGen(depth - 1)
      ))
    } else {
      Gen.lzy(Gen.oneOf(
        rStringGen,
        rBulkStringGen,
        rLongGen,
        rNullGen,
        rErrorGen,
      ))

    }

  private def testEncodeDecode(input: Value): F[Expectations] = {
    for {
      encoded <- IO.fromTry(Value.combined.encode(input).toTry)
      decoded <- IO.fromTry(Value.combined.decode(encoded).toTry)
    } yield expect.eql(input, decoded.value)
  }

  test("can encode/decode bulk string null") {
    testEncodeDecode(Value.RNull)
  }

  test("can decode array null") {
    for {
      decoded <- IO.fromTry(Value.combined.decode(BitVector("*-1\r\n".getBytes(StandardCharsets.US_ASCII))).toTry)
    } yield expect(decoded.value == Value.RNull)
  }

  test("can encode/decode value") {
    forall(rValueGen(2))(testEncodeDecode)
  }
}
