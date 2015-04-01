import sbt._
import sbt.Keys._
import sbtassembly._
import sbtassembly.AssemblyKeys._

object CurioDB extends Build {

  val akkaV = "2.4-SNAPSHOT"

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
      name := "curiodb",
      version := "0.0.1",
      fork := true,
      mainClass in (Compile, run) := Some("curiodb.CurioDB"),
      resolvers += "Akka Snapshots" at "http://repo.akka.io/snapshots/",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaV,
        "com.typesafe.akka" %% "akka-persistence-experimental" % akkaV,
        "com.typesafe.akka" %% "akka-cluster" % akkaV
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
