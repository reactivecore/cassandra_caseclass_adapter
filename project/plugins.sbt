logLevel := Level.Warn

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.5")
//
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")
//
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.3")
