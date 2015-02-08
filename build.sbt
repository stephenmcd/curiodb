
name := "curiodb"

version := "0.0.1"

val akkaV = "2.3.9"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-persistence-experimental" % akkaV
)

val shellScript = Option(Seq("#!/usr/bin/env sh", """exec java -jar "$0" "$@""""))

assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = shellScript)

assemblyJarName in assembly := s"${name.value}-${version.value}"
