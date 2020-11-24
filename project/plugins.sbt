logLevel := Level.Warn

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")
//
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1") // fot sbt-0.13.5 or higher
//
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.3")
