organization := "foo"

name := "fpp"

version := "2.2"

scalaVersion := "2.10.3"

libraryDependencies += "org.sellmerfud" % "optparse_2.10" % "1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.3" % "test"

scalacOptions += "-deprecation"

scalacOptions += "-feature"


// For sbt-pack

packSettings

packMain := Map(
   "fpp-intervalmon" -> "pws.netops.fpp.intervalmon.Main",
   "fpp-gribconv"    -> "pws.netops.fpp.grib.Main",
   "fpp-gradscondor" -> "pws.netops.fpp.grads.MainCondor",
   "fpp-gradsexec"   -> "pws.netops.fpp.grads.MainExec",
   "fpp-filepost"    -> "pws.netops.fpp.filepost.Main"
)

packGenerateWindowsBatFile := false
