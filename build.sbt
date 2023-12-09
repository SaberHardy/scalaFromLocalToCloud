ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.3"


lazy val root = (project in file("."))
  .settings(
    name := "ScalaExos"
  )
libraryDependencies += "org.scalafx" %% "scalafx" % "16.0.0-R25"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"
libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "3.3.2")
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.5.0"
