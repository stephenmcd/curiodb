
name := "curiodb"

version := "0.0.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.1"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Option(Seq("#!/usr/bin/env sh", """exec java -jar "$0" "$@"""")))

assemblyJarName in assembly := s"${name.value}-${version.value}"
