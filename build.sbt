
name := "curiodb"

version := "0.0.1"

val akkaV = "2.4-SNAPSHOT"

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-persistence-experimental" % akkaV,
  "com.typesafe.akka" %% "akka-cluster" % akkaV
)

val shellScript = Option(Seq("#!/usr/bin/env sh", """exec java -jar "$0" "$@""""))

assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = shellScript)

assemblyJarName in assembly := s"${name.value}-${version.value}"

fork := true
