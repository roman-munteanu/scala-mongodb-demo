name := "scala-mongodb-demo"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.mongodb" %% "casbah"   % "3.1.0",
  "joda-time"   % "joda-time" % "2.9.2",
  "org.joda"    % "joda-convert" % "1.8.1",
  "org.reactivemongo" %% "reactivemongo" % "0.11.9"
)

resolvers += "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/"