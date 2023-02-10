package dk.alfabetacain.scadis

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.comcast.ip4s._
import dk.alfabetacain.scadis.codec.Codec
import org.typelevel.log4cats.slf4j.Slf4jFactory
import weaver._

import java.util.UUID
import scala.annotation.nowarn

object ClusterSuite extends IOSuite {

  override type Res = RedisClusterContainer

  override def sharedResource: Resource[IO, Res] = {
    RedisClusterContainer.make()
  }

  @nowarn("msg=private default argument")
  private def randomStr(prefix: String = ""): String = prefix + "-" + UUID.randomUUID().toString()

  private implicit val loggerFactory = Slf4jFactory[IO]

  @nowarn("msg=private default argument")
  private def connection(
      redis: RedisClusterContainer,
      autoReconnect: Boolean = true,
  ): Resource[IO, ClusterClient[IO, String, String]] = {
    for {
      connection <- ClusterClient.make[IO, String, String](
        SocketAddress(host"localhost", Port.fromString(redis.getRedisPort().toString).get),
        ClusterClient.Config(autoReconnect),
        Codec.utf8Codec,
        Codec.utf8Codec,
      )
    } yield connection
  }

  test("get/set many times") { redis =>
    connection(redis, false).use { client =>
      List.fill(100) {
        val key   = randomStr("key")
        val value = randomStr("value")
        for {
          existing <- client.get(key)
          _        <- client.set(key, value)
          updated  <- client.get(key)
        } yield expect(existing == None).and(expect(updated == Some(value)))
      }.sequence.map(res => forEach(res)(identity))
    }
  }
}
