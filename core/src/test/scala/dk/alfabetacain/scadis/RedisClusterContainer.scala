package dk.alfabetacain.scadis

import cats.effect.kernel.Resource
import cats.effect.IO
import org.testcontainers.utility.DockerImageName
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

class RedisClusterContainer(dockerImageName: DockerImageName)
    extends GenericContainer[RedisClusterContainer](dockerImageName) {
  def getRedisPort(): Int = this.getMappedPort(RedisClusterContainer.redisPort)
}

object RedisClusterContainer {

  private val redisPort = 7001

  def make(): Resource[IO, RedisClusterContainer] = {
    for {
      container <- Resource.eval(IO(
        new RedisClusterContainer(DockerImageName.parse("grokzen/redis-cluster:6.2.0"))
          .withEnv("MASTERS", "3")
          .withEnv("INITIAL_PORT", "7000")
          .withExposedPorts(7000, 7001, 7002, 7003, 7004)
          .waitingFor(Wait.forLogMessage(".*Full resync from master.*", 3))
      ))
      _ <- Resource.make(IO(container.start()))(_ => IO(container.stop()))
    } yield container
  }
}
