package net.reactivecore.cca

class ListTest extends TestBaseWithCassandra {

  case class CourseRecord(
      user: String,
      grades: Seq[Float]
  )

  val ddl =
    """
      |CREATE TABLE course_records (
      |  user TEXT,
      |  grades LIST<FLOAT>,
      |  PRIMARY KEY(user)
      |)
    """.stripMargin

  trait Env {
    executeCql(ddl)

    val adapter = CassandraCaseClassAdapter.make[CourseRecord]("course_records")
  }

  it should "read and write" in new Env {
    adapter.loadAllFromCassandra(session) shouldBe empty
    val entryWithEmpty = CourseRecord("Alice", Seq())
    val entryWithGrades = CourseRecord("Bob", Seq(1.5f, 2.5f))
    adapter.insert(entryWithEmpty, session)
    adapter.insert(entryWithGrades, session)
    adapter.loadAllFromCassandra(session).sortBy(_.user) shouldBe Seq(entryWithEmpty, entryWithGrades)
  }
}
