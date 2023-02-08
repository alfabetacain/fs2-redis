package dk.alfabetacain.fs2_redis

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.comcast.ip4s.SocketAddress
import com.comcast.ip4s._
import dk.alfabetacain.fs2_redis.parser.Value
import dk.alfabetacain.fs2_redis.parser.Value.RESPArray
import dk.alfabetacain.fs2_redis.parser.Value.RESPBulkString
import dk.alfabetacain.fs2_redis.parser.Value.RESPError
import dk.alfabetacain.fs2_redis.parser.Value.RESPInteger
import dk.alfabetacain.fs2_redis.parser.Value.RESPNull
import dk.alfabetacain.fs2_redis.parser.Value.SimpleString
import fs2.io.net.Network
import weaver._

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.util.Try
import scala.annotation.nowarn
import org.typelevel.log4cats.slf4j.Slf4jFactory
import dk.alfabetacain.fs2_redis.codec.Codec
import cats.data.NonEmptyList

object IntegrationSuite extends IOSuite {

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
      case RESPInteger(value) => Right(value.toString)
      case RESPError(value) =>
        Left(value)
      case RESPBulkString(data) => Try(new String(data, StandardCharsets.UTF_8)).toEither.left.map(_.toString())
      case SimpleString(value)  => Right(value)
      case RESPArray(elements) =>
        elements.map(asString).sequence[Either[String, *], String].map(_.mkString(","))
      case RESPNull => Left("null")
    }
  }

  test("Automatic reconnects") { redis =>
    connection(redis, true).use { conn =>
      val key   = randomStr("key")
      val value = randomStr("value")
      for {
        _          <- conn.set(key, value)
        clientId   <- conn.clientId()
        _          <- IO.println(s"id = $clientId")
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
      List.fill(2000)(
        client.ping()
      ).parSequence.map(ls => forEach(ls)(item => expect(item == "PONG")))
    }
  }
}
