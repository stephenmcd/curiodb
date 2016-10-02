import sbt._
import sbt.Keys._
import sbtassembly._
import sbtassembly.AssemblyKeys._
import spray.revolver.RevolverPlugin._

object CurioDB extends Build {

  val scalaV   = "2.11.7"
  val akkaV    = "2.4.11"
  val sprayV   = "1.3.4"
  val curiodbV = "0.0.1"

  val dependencies = Seq(
    "com.typesafe.akka"   %% "akka-actor" % akkaV,
    "com.typesafe.akka"   %% "akka-persistence" % akkaV,
    "com.typesafe.akka"   %% "akka-cluster" % akkaV,
    "io.spray"            %% "spray-can" % sprayV,
    "io.spray"            %% "spray-json" % "1.3.2",
    "com.wandoulabs.akka" %% "spray-websocket" % "0.1.4",
    "org.luaj"             % "luaj-jse" % "3.0.1"
  )

  val reverseConcat: MergeStrategy = new MergeStrategy {
    val name = "reverseConcat"
    def apply(tempDir: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] =
      MergeStrategy.concat(tempDir, path, files.reverse)
  }

  lazy val indexedTreeMap = RootProject(uri("git://github.com/stephenmcd/indexed-tree-map.git"))
  lazy val hyperLogLog    = RootProject(uri("git://github.com/stephenmcd/java-hll.git#with-fastutils"))

  lazy val root = Project("root", file("."), settings = Seq(
    name := "curiodb",
    version := curiodbV,
    scalaVersion in Global := scalaV,
    //resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.ivy2/local",
    resolvers += "Akka Snapshots" at "http://repo.akka.io/snapshots/",
    resolvers += "spray repo" at "http://repo.spray.io",
    libraryDependencies ++= dependencies,
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript =
      Option(Seq("#!/usr/bin/env sh", s"""exec -a ${name.value} java -jar "$$0" "$$@" """))),
    assemblyJarName in assembly := s"${name.value}-${version.value}",
    assemblyMergeStrategy in assembly := {
      case PathList(ps @ _*) if ps.takeRight(2).mkString("/") == "leveldb/DB.class" => MergeStrategy.first
      case x if Assembly.isConfigFile(x) => reverseConcat
      case x => (assemblyMergeStrategy in assembly).value(x)
    }
  ) ++ Revolver.settings).dependsOn(indexedTreeMap, hyperLogLog)

}
