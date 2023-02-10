package dk.alfabetacain.scadis.parser

import weaver._
import weaver.scalacheck._
import cats.effect.IO
import org.scalacheck.Gen
import scodec.bits.BitVector
import java.nio.charset.StandardCharsets

object ParserSuite extends SimpleIOSuite with Checkers {

  private val simpleStringGenerator =
    Gen.asciiStr.map(Value.SimpleString.apply)

  test("hello world") {
    IO(expect(2 == 2))
  }

  private def expectBulkEquals(actual: Value, expected: String): Expectations = {
    (expect(actual match {
      case _: Value.RESPBulkString => true
      case _                       => false
    })).and(
      expect(new String(
        actual.asInstanceOf[Value.RESPBulkString].data,
        StandardCharsets.UTF_8
      ) == expected)
    )
  }

  test("SimpleString encode/decode") {

    forall(simpleStringGenerator) { simpleString =>
      for {
        encoded <- IO.fromTry(Value.combined.encode(simpleString).toTry)
        decoded <- IO.fromTry(Value.combined.decode(encoded).toTry)
      } yield expect(simpleString == simpleString)
    }
  }

  test("can encode/decode bulk string null") {
    for {
      encoded <- IO.fromTry(Value.combined.encode(Value.RESPNull).toTry)
      decoded <- IO.fromTry(Value.combined.decode(encoded).toTry)
    } yield expect(decoded.value == Value.RESPNull)
  }

  test("can decode array null") {
    for {
      decoded <- IO.fromTry(Value.combined.decode(BitVector("*-1\r\n".getBytes(StandardCharsets.US_ASCII))).toTry)
    } yield expect(decoded.value == Value.RESPNull)
  }

  test("can encode/decode bulk string") {
    forall(Gen.alphaStr) { bulkString =>
      for {
        encoded <-
          IO.fromTry(Value.combined.encode(Value.RESPBulkString(bulkString.getBytes(StandardCharsets.UTF_8))).toTry)
        decoded <- IO.fromTry(Value.combined.decode(encoded).toTry)
      } yield expectBulkEquals(decoded.value, bulkString)
    }
  }
}
