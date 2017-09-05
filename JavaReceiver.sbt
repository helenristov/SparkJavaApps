name := "Java Receiver Spark Application"
version := "1.0"
scalaVersion := "2.11.7"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
                            "org.slf4j" % "slf4j-simple" % "1.7.5")


libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0"

libraryDependencies ++= Seq(
"org.apache.spark" % "spark-streaming_2.11" % "2.1.0",
"com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0" )



