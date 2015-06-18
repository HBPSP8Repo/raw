package raw.publications

case class Publication(title: String, authors: Seq[String], affiliations: Seq[String], controlledterms: Seq[String])

case class Author(name: String, title: String, year: Int)
