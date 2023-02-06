import Dependencies._

ThisBuild / scalaVersion     := "2.13.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "dk.alfabetacain"
ThisBuild / organizationName := "alfabetacain"

val fs2Version    = "3.4.0"
val weaverVersion = "0.8.1"

lazy val scala2_12  = "2.12.17"
lazy val scala2_13  = "2.13.8"
lazy val catsEffect = "3.4.5"
lazy val logback    = "1.4.5"

lazy val core = (project in file("core"))
  .settings(
    name               := "fs2-redis-core",
    crossScalaVersions := Seq(scala2_12, scala2_13),
    libraryDependencies ++= Seq(
      "org.scodec"          %% "scodec-core"        % (if (scalaVersion.value.startsWith("2.")) "1.11.10" else "2.1.0"),
      "co.fs2"              %% "fs2-core"           % fs2Version,
      "co.fs2"              %% "fs2-scodec"         % fs2Version,
      "co.fs2"              %% "fs2-io"             % fs2Version,
      "org.typelevel"       %% "cats-effect-kernel" % catsEffect,
      "org.typelevel"       %% "log4cats-core"      % "2.5.0",
      "org.typelevel"       %% "log4cats-slf4j"     % "2.5.0"       % Test,
      "ch.qos.logback"       % "logback-classic"    % logback       % Test,
      "org.typelevel"       %% "cats-effect"        % catsEffect    % Test,
      "com.disneystreaming" %% "weaver-cats"        % weaverVersion % Test,
      "com.disneystreaming" %% "weaver-scalacheck"  % weaverVersion % Test,
      "org.testcontainers"   % "testcontainers"     % "1.17.6"      % Test,
    ),
    testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
    Compile / run / fork := true,
    Test / fork          := true,
    scalacOptions ++= Seq(
      "-Xlint"
    ),
    addCompilerPlugin("com.olegpy"   %% "better-monadic-for" % "0.3.1"),
    addCompilerPlugin("org.typelevel" % "kind-projector"     % "0.13.2" cross CrossVersion.full),
  )

lazy val repl = (project in file("repl"))
  .settings(
    name               := "fs2-redis-repl",
    crossScalaVersions := Seq(scala2_13),
    testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect"     % catsEffect,
      "org.typelevel" %% "log4cats-slf4j"  % "2.5.0",
      "ch.qos.logback" % "logback-classic" % logback,
    ),
  ).dependsOn(core)

lazy val root = (project in file("."))
  .aggregate(core, repl)
  .settings(
    name               := "fs2-redis",
    crossScalaVersions := Nil,
    publish / skip     := true,
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
