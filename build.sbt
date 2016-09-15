name := "zhihu_parser"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.4.0"

libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1209"

libraryDependencies ++= Seq("org.scalikejdbc" %% "scalikejdbc" % "2.4.+",
                            "com.h2database" % "h2" % "1.4.+",
                            "ch.qos.logback" % "logback-classic" % "1.1.+")

import com.trueaccord.scalapb.{ScalaPbPlugin => PB}
PB.protobufSettings
