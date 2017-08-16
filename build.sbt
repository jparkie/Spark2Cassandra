/**
  * Organization:
  */
organization     := "com.github.jparkie"
organizationName := "jparkie"

/**
  * Library Meta:
  */
name     := "Spark2Cassandra"
licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))

/**
  * Scala:
  */
scalaVersion       := "2.11.8"

/**
  * Library Dependencies:
  */

// Versions:
val SparkVersion                   = "2.0.2"
val SparkTestVersion               = "2.0.2_0.7.1"
val ScalaTestVersion               = "3.0.3"
val SparkCassandraConnectorVersion = "2.0.1"
val CassandraAllVersion            = "3.11.0"
val CassandraClientutilVersion     = "3.0.14"
val CassandraUnitVersion           = "3.1.3.2"
val GrizzledSl4jVersion            = "1.3.1"
val GuavaVersion                   = "22.0"
val Slf4jVersion                   = "1.7.7"

// Dependencies:
val sparkCore       = "org.apache.spark"     %% "spark-core"                % SparkVersion                   % "provided"
val sparkSql        = "org.apache.spark"     %% "spark-sql"                 % SparkVersion                   % "provided"
val sparkTest       = "com.holdenkarau"      %% "spark-testing-base"        % SparkTestVersion               % "test"
val scalaTest       = "org.scalatest"        %% "scalatest"                 % ScalaTestVersion               % "test"
val ssc             = "com.datastax.spark"   %% "spark-cassandra-connector" % SparkCassandraConnectorVersion
val cassandraAll    = "org.apache.cassandra" %  "cassandra-all"             % CassandraAllVersion
val cassandraClient = "org.apache.cassandra" %  "cassandra-clientutil"      % CassandraClientutilVersion
val cassandraUnit   = "org.cassandraunit"    %  "cassandra-unit"            % CassandraUnitVersion           % "test"
val grizzledSl4j    = "org.clapper"          %% "grizzled-slf4j"            % GrizzledSl4jVersion
val guava           = "com.google.guava"     %  "guava"                     % GuavaVersion
val jclOverSlf4j    = "org.slf4j"            % "jcl-over-slf4j"             % Slf4jVersion


libraryDependencies ++= Seq(sparkCore, sparkSql, sparkTest, scalaTest, ssc, cassandraAll, cassandraUnit, grizzledSl4j, guava, jclOverSlf4j)

// Force cassandraUnit and ssc to utilize cassandraAll, cassandraClient.
dependencyOverrides ++= Set(cassandraAll, cassandraClient)

/**
  * Tests:
  */
parallelExecution in Test := false

/**
  * Scoverage:
  */
coverageEnabled in Test := true

/**
  * Publishing to Sonatype:
  */
publishMavenStyle := true

publishArtifact in Test := false

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := {
  <url>https://github.com/jparkie/Spark2Cassandra</url>
    <scm>
      <url>git@github.com:jparkie/Spark2Cassandra.git</url>
      <connection>scm:git:git@github.com:jparkie/Spark2Cassandra.git</connection>
    </scm>
    <developers>
      <developer>
        <id>jparkie</id>
        <name>Jacob Park</name>
        <url>https://github.com/jparkie</url>
      </developer>
    </developers>
}

/**
  * Release:
  */
import ReleaseTransformations._

releasePublishArtifactsAction := PgpKeys.publishSigned.value

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shade.com.google.@1").inAll
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}