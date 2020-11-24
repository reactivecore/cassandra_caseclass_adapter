import sbt.Keys.scalacOptions

lazy val commonSettings = Seq(
  version := "0.0.2-SNAPSHOT",

  organization := "net.reactivecore",

  scalaVersion := "2.11.12",

  crossScalaVersions := Seq("2.11.12", "2.12.10"),

  scalacOptions += "-target:jvm-1.8"
)

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.3",
  "com.datastax.oss"   % "java-driver-core"          % "4.7.2",
  "com.datastax.oss"   % "java-driver-query-builder" % "4.7.2",
  "org.scalatest" %% "scalatest" % "3.0.9" % "test"
)

libraryDependencies ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) if n <= 11 =>
      List(compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full))
    case _                       => Nil
  }

}

pomExtra := {
  <url>https://github.com/reactivecore/cassandra_caseclass_adapter</url>
    <licenses>
      <license>
        <name>Apache License, Version 2.0</name>
        <url>https://www.apache.org/licenses/LICENSE-2.0.html</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:@github.com:reactivecore/cassandra_caseclass_adapter.git</connection>
      <url>git@github.com:reactivecore/cassandra_caseclass_adapter.git</url>
    </scm>
    <developers>
      <developer>
        <id>nob13</id>
        <name>Norbert Schultz</name>
        <url>https://www.reactivecore.de</url>
      </developer>
    </developers>
}



import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(DanglingCloseParenthesis, scalariform.formatter.preferences.Preserve)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(name := "cassandra-caseclass-adapter")

parallelExecution in Test := false