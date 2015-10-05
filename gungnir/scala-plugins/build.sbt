organization := "org.gennai"
name := "gungnir-scala-plugins"
version := "0.0.1"
scalaVersion := "2.11.5"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
jarName in assembly := "scala-plugin.jar"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.gennai" % "gungnir-core" % "0.0.1" % "provided",
  "org.gennai" % "gungnir-core" % "0.0.1" % "test" classifier "tests",
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
)
