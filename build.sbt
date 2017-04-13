lazy val commonSettings = Seq(
  version := "0.0.1-SNAPSHOT",

  organization := "net.reactivecore",

  scalaVersion := "2.11.8"
)

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.2",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.7",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

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