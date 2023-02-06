package dk.alfabetacain.fs2_redis

import dk.alfabetacain.fs2_redis.parser.Value

trait RedisCodec[A] {
  def encode(input: A): Value
  def decode(input: Value): Either[Throwable, A]
}
