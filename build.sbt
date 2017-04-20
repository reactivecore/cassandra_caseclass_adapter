lazy val commonSettings = Seq(
  version := "0.0.1-SNAPSHOT",

  organization := "net.reactivecore",

  scalaVersion := "2.11.8",

  crossScalaVersions := Seq("2.10.6", "2.11.8", "2.12.1")
)

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.2",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.7",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

libraryDependencies <++= scalaVersion { sv =>
  if (sv.startsWith("2.10")){
    Seq(compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full))
  } else Nil
}

import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(PreserveDanglingCloseParenthesis, true)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(name := "cassandra-caseclass-adapter")

parallelExecution in Test := false