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

object IntegrationSuite extends IOSuite {

  override type Res = RedisContainer

  override def sharedResource: Resource[IO, Res] = {
    RedisContainer.make()
  }

  @nowarn("msg=private default argument")
  private def randomStr(prefix: String = ""): String = prefix + "-" + UUID.randomUUID().toString()

  private implicit val loggerFactory = Slf4jFactory[IO]

  private def connection(redis: RedisContainer, autoReconnect: Boolean = false): Resource[IO, Client[IO]] = {
    for {
      connection <- Client.make[IO](
        Network[IO].client(
          SocketAddress(host"localhost", Port.fromString(redis.getRedisPort().toString).get)
        ),
        Client.Config(autoReconnect)
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

  test("raw integration test") { redis =>
    connection(redis).use { conn =>
      val key   = "key"
      val value = "value"
      for {
        _       <- IO.println("Getting key 1")
        current <- conn.raw("GET", key)
        _       <- IO.println("Got key 1")
        _       <- expect(current == Value.RESPNull).failFast
        _       <- conn.raw("SET", key, value)
        now     <- conn.raw("GET", key)
        _       <- IO.println("Got key 2")
      } yield expect(asString(now) == Right(value))
    }
  }

  test("Automatic reconnects") { redis =>
    connection(redis, true).use { conn =>
      val key   = randomStr("key")
      val value = randomStr("value")
      for {
        _          <- conn.raw("SET", key, value)
        clientId   <- conn.raw("CLIENT", "ID").map(asString)
        _          <- expect(clientId.isRight).failFast
        _          <- IO.println(s"id = $clientId")
        killResult <- conn.raw("CLIENT", "KILL", "ID", clientId.toOption.get, "SKIPME", "no")
        _          <- expect(asString(killResult) == Right("1")).failFast
        foundValue <- conn.raw("GET", key)
      } yield expect(asString(foundValue) == Right(value))
    }
  }

  test("fail when not reconnecting") { redis =>
    connection(redis, false).use { client =>
      val key = randomStr("key")
      for {
        clientId   <- client.raw("CLIENT", "ID").map(asString)
        _          <- expect(clientId.isRight).failFast
        killResult <- client.raw("CLIENT", "KILL", "ID", clientId.toOption.get, "SKIPME", "no")
        _          <- expect(asString(killResult) == Right("1")).failFast
        foundValue <- client.raw("GET", key).attempt
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
}
