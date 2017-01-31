import sbt.cross.CrossVersionUtil

configs(Benchmark, Regression)

inConfig(Benchmark)(Defaults.testSettings)

inConfig(Regression)(Defaults.testSettings)

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-core" % "3.8.7" % Test,
  "org.specs2" %% "specs2-mock" % "3.8.7" % Test,
  "com.h2database" % "h2" % "1.4.191" % "test",
  "com.storm-enroute" %% "scalameter" % "0.8.2" % Benchmark,
  "com.storm-enroute" %% "scalameter" % "0.8.2" % Regression
)

libraryDependencies ++= (CrossVersionUtil.binaryScalaVersion(scalaVersion.value) match {
  case "2.10" => Seq("com.typesafe.play" %% "anorm" % "2.4.0" % Benchmark)
  case "2.11" => Seq("com.typesafe.play" %% "anorm" % "2.5.2" % Benchmark)
  case "2.12" => Nil // note: can't run benchmarks for 2.12 until a suitable anorm artifact is avaiable
})

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
