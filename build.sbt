import Dependencies._

ThisBuild / scalaVersion     := "2.13.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

val fs2Version    = "3.4.0"
val weaverVersion = "0.8.1"

lazy val root = (project in file("."))
  .settings(
    name                            := "fs2-redis",
    libraryDependencies += scalaTest % Test,
    libraryDependencies ++= Seq(
      "org.scodec"          %% "scodec-core"       % (if (scalaVersion.value.startsWith("2.")) "1.11.10" else "2.1.0"),
      "co.fs2"              %% "fs2-core"          % fs2Version,
      "co.fs2"              %% "fs2-scodec"        % fs2Version,
      "co.fs2"              %% "fs2-io"            % fs2Version,
      "org.typelevel"       %% "cats-effect"       % "3.4.4",
      "com.outr"            %% "scribe"            % "3.10.7",
      "com.outr"            %% "scribe-slf4j2"     % "3.10.7",
      "com.outr"            %% "scribe-cats"       % "3.10.7",
      "com.disneystreaming" %% "weaver-cats"       % weaverVersion % Test,
      "com.disneystreaming" %% "weaver-scalacheck" % weaverVersion % Test,
      "org.testcontainers"   % "testcontainers"    % "1.17.6"      % Test,
    ),
    testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
    Compile / run / fork := true,
    Test / fork          := true,
    scalacOptions ++= Seq(
      "-Xlint"
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
