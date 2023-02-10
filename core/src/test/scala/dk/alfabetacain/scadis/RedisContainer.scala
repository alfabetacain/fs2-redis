package dk.alfabetacain.scadis

import cats.effect.IO
import cats.effect.kernel.Resource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

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
