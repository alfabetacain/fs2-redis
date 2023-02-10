package dk.alfabetacain.scadis

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.comcast.ip4s.SocketAddress
import com.comcast.ip4s._
import dk.alfabetacain.scadis.codec.Codec
import dk.alfabetacain.scadis.parser.Value
import fs2.io.net.Network
import org.typelevel.log4cats.slf4j.Slf4jFactory
import weaver._

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.annotation.nowarn
import scala.util.Try

object ClientSuite extends IOSuite {

  override type Res = RedisContainer

  override def sharedResource: Resource[IO, Res] = {
    RedisContainer.make()
  }

  @nowarn("msg=private default argument")
  private def randomStr(prefix: String = ""): String = prefix + "-" + UUID.randomUUID().toString()

  private implicit val loggerFactory = Slf4jFactory[IO]

  @nowarn("msg=private default argument")
  private def connection(
      redis: RedisContainer,
      autoReconnect: Boolean = true,
  ): Resource[IO, Client[IO, String, String]] = {
    for {
      connection <- Client.make[IO, String, String](
        Network[IO].client(
          SocketAddress(host"localhost", Port.fromString(redis.getRedisPort().toString).get)
        ),
        Client.Config(autoReconnect),
        Codec.utf8Codec,
        Codec.utf8Codec,
      )
    } yield connection
  }

  private def asString(input: Value): Either[String, String] = {
    input match {
      case Value.RLong(value) => Right(value.toString)
      case Value.RError(value) =>
        Left(value)
      case Value.RBulkString(data) => Try(new String(data, StandardCharsets.UTF_8)).toEither.left.map(_.toString())
      case Value.RString(value)    => Right(value)
      case Value.RArray(elements) =>
        elements.map(asString).sequence[Either[String, *], String].map(_.mkString(","))
      case Value.RNull => Left("null")
    }
  }

  test("Automatic reconnects") { redis =>
    connection(redis, true).use { conn =>
      val key   = randomStr("key")
      val value = randomStr("value")
      for {
        _          <- conn.set(key, value)
        clientId   <- conn.clientId()
        killResult <- conn.killClient(NonEmptyList.of(Client.KillClientFilter.Id(clientId)), false)
        _          <- expect(killResult == 1).failFast
        foundValue <- conn.get(key)
      } yield expect(foundValue == Some(value))
    }
  }

  test("fail when not reconnecting") { redis =>
    connection(redis, false).use { client =>
      val key = randomStr("key")
      for {
        clientId   <- client.clientId()
        killResult <- client.killClient(NonEmptyList.of(Client.KillClientFilter.Id(clientId)), false)
        _          <- expect(killResult == 1).failFast
        foundValue <- client.get(key).attempt
      } yield expect(foundValue.isLeft)
    }
  }

  test("get/set") { redis =>
    connection(redis, false).use { client =>
      val key   = randomStr("key")
      val value = randomStr("value")
      for {
        initial <- client.get(key)
        _       <- client.set(key, value)
        found   <- client.get(key)
      } yield expect(initial == None).and(expect(found == Some(value)))
    }
  }

  test("a thousand pings") { redis =>
    List.fill(1000)(connection(redis, false).use { client =>
      client.ping()
    }).parSequence.map(ls => forEach(ls)(item => expect(item == "PONG")))
  }

  test("a thousand pings on one connection") { redis =>
    connection(redis, false).use { client =>
      List.fill(1000)(
        client.ping()
      ).parSequence.map(ls => forEach(ls)(item => expect(item == "PONG")))
    }
  }

  test("Sending unknown command does not break the connection") { redis =>
    connection(redis, false).use { client =>
      val key   = randomStr("key")
      val value = randomStr("value")
      for {
        _     <- client.raw(NonEmptyList.of(Util.toBS("UNKNOWNCOMMAND")))
        _     <- client.set(key, value)
        found <- client.get(key)
      } yield expect(found == Some(value))

    }
  }
}
