import sbt._
import sbt.Keys._
import sbtassembly._
import sbtassembly.AssemblyKeys._

object CurioDB extends Build {

  val scalaV = "2.11.7"
  val akkaV  = "2.4-SNAPSHOT"
  val sprayV = "1.3.3"

  val reverseConcat: MergeStrategy = new MergeStrategy {
    val name = "reverseConcat"
    def apply(tempDir: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] =
      MergeStrategy.concat(tempDir, path, files.reverse)
  }

  lazy val indexedTreeMap = RootProject(uri("git://github.com/stephenmcd/indexed-tree-map.git"))
  lazy val hyperLogLog = RootProject(uri("git://github.com/stephenmcd/java-hll.git#with-fastutils"))

  lazy val root = Project("root", file("."))
    .dependsOn(indexedTreeMap, hyperLogLog)
    .settings(
      scalaVersion := scalaV,
      scalaVersion in indexedTreeMap := scalaV,
      scalaVersion in hyperLogLog := scalaV,
      name := "curiodb",
      version := "0.0.1",
      fork := true,
      mainClass in (Compile, run) := Some("curiodb.CurioDB"),
      //resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.ivy2/local",
      resolvers += "Akka Snapshots" at "http://repo.akka.io/snapshots/",
      resolvers += "spray repo" at "http://repo.spray.io",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaV,
        "com.typesafe.akka" %% "akka-persistence-experimental" % akkaV,
        "com.typesafe.akka" %% "akka-cluster" % akkaV,
        "io.spray" %% "spray-can" % sprayV,
        "io.spray" %% "spray-json" % "1.3.2",
        "com.wandoulabs.akka" %% "spray-websocket" % "0.1.4",
        "org.luaj" % "luaj-jse" % "3.0.1"
      ),
      assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript =
        Option(Seq("#!/usr/bin/env sh", """ exec java -jar "$0" "$@" """))),
      assemblyJarName in assembly := s"${name.value}-${version.value}",
      assemblyMergeStrategy in assembly := {
        case PathList(ps @ _*) if ps.takeRight(2).mkString("/") == "leveldb/DB.class" => MergeStrategy.first
        case x if Assembly.isConfigFile(x) => reverseConcat
        case x => (assemblyMergeStrategy in assembly).value(x)
      }

    )

}
