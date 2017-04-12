package net.reactivecore.cca

class SetTest extends TestBaseWithCassandra {

  case class Schoolclass(
    id: Int,
    children: Set[String]
  )

  val ddl =
    """
      |CREATE TABLE school_class (
      |  id INT,
      |  children Set<TEXT>,
      |  PRIMARY KEY(id)
      |);
    """.stripMargin

  trait Env {
    executeCql(ddl)
    val adapter = CassandraCaseClassAdapter.make[Schoolclass]("school_class")
  }

  it should "read and write" in new Env {
    adapter.loadAllFromCassandra(session) shouldBe empty
    val clazz = Schoolclass(12, Set("Alice", "Bob", "Charly"))

    adapter.insert(clazz, session)

    adapter.loadAllFromCassandra(session) shouldBe Seq(clazz)
  }
}
