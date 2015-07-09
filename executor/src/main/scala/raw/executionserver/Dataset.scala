package raw.executionserver

abstract class Dataset {
  val accessPaths: List[AccessPath[_]]
}

