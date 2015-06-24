count(authors)

val authorsCount = result.asInstanceOf[Int]
assert(authorsCount === 50, "Wrong number of authors")

--

count(publications)

val pubsCount = result.asInstanceOf[Int]
assert(pubsCount === 1000, "Wrong number of publications")
