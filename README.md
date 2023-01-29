# fs2-redis

Simple cats-effect based redis client

## Example usage

### obtaining a connection

```scala
val socketResource = Network[IO].client(SocketAddress(host"localhost", port"6379"))

Client.make[IO](socketResource, Client.Config(autoReconnect = true)).use { client =>
	for {
		clientId <- client.raw("CLIENT", "ID")
		_ <- IO.println(clientId)
	} yield ExitCode.Success
}

```

### repl

```scala
// will run until you run :quit
Repl.make[IO](host"localhost", port"6379", autoReconnect = true).as(ExitCode.Success)

```
