resolvers += Classpaths.typesafeReleases

resolvers += Classpaths.sbtPluginReleases

resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.4")

//addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.5.0")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "1.0.1")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0.BETA1")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")
