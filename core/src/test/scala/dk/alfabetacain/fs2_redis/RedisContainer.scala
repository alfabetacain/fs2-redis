package dk.alfabetacain.fs2_redis

import cats.effect.kernel.Resource
import cats.effect.IO
import org.testcontainers.utility.DockerImageName
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

class RedisContainer(dockerImageName: DockerImageName)
    extends GenericContainer[RedisContainer](dockerImageName) {
  def getRedisPort(): Int = this.getMappedPort(RedisContainer.redisPort)
}

object RedisContainer {

  private val redisPort = 6379

  def make(): Resource[IO, RedisContainer] = {
    for {
      container <- Resource.eval(IO(
        new RedisContainer(DockerImageName.parse("redis:6.2.0"))
          .withExposedPorts(redisPort)
          .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))
      ))
      _ <- Resource.make(IO(container.start()))(_ => IO(container.stop()))
    } yield container
  }
}
