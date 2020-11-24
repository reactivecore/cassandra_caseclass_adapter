package net.reactivecore.cca

class ComplexTypesTest extends TestBaseWithCassandra {

  case class UdtValue(
      name: String,
      age: Option[Int]
  )

  case class ListOfUdt(
      id: Int,
      values: Seq[UdtValue]
  )

  case class SetOfUdt(
      id: Int,
      values: Set[UdtValue]
  )

  case class OptionalUdt(
      id: Int,
      value: Option[UdtValue]
  )

  val ddl =
    """
      |
      |CREATE TYPE udt_value (
      |  name TEXT,
      |  age INT
      |);
      |
      |CREATE TABLE list_of_udt (
      |  id INT,
      |  values LIST<FROZEN<udt_value>>,
      |  PRIMARY KEY(id)
      |);
      |
      |CREATE TABLE set_of_udt (
      |  id INT,
      |  values SET<FROZEN<udt_value>>,
      |  PRIMARY KEY(id)
      |);
      |
      |CREATE TABLE optional_udt (
      |  id INT,
      |  value FROZEN<udt_value>,
      |  PRIMARY KEY(id)
      |);
    """.stripMargin

  trait Env {
    executeCql(ddl)
    val listAdapter = CassandraCaseClassAdapter.make[ListOfUdt]("list_of_udt")
    val setAdapter = CassandraCaseClassAdapter.make[SetOfUdt]("set_of_udt")
    val optionalAdapter = CassandraCaseClassAdapter.make[OptionalUdt]("optional_udt")

    val listValues = Seq(
      ListOfUdt(1, Seq.empty),
      ListOfUdt(2, Seq(UdtValue("Alice", None))),
      ListOfUdt(3, Seq(UdtValue("Bob", Some(42)))),
      ListOfUdt(4, Seq(UdtValue("Charly", Some(4)), UdtValue("Dorothee", Some(100)))))

    val setValues = listValues.map { x => SetOfUdt(x.id, x.values.toSet) }
    val optionalValues = Seq(
      OptionalUdt(1, None),
      OptionalUdt(2, Some(UdtValue("Alice", Some(42)))),
      OptionalUdt(3, Some(UdtValue("Bob", None))))
  }

  it should "read and write lists of UDTs" in new Env {
    listAdapter.loadAllFromCassandra(session) shouldBe empty
    listValues.foreach(listAdapter.insert(_, session))
    listAdapter.loadAllFromCassandra(session).sortBy(_.id) shouldBe listValues
  }

  it should "read and write sets of UDTs" in new Env {
    setAdapter.loadAllFromCassandra(session) shouldBe empty
    setValues.foreach(setAdapter.insert(_, session))
    setAdapter.loadAllFromCassandra(session).sortBy(_.id) shouldBe setValues
  }

  it should "read and write optional UDT values" in new Env {
    optionalAdapter.loadAllFromCassandra(session) shouldBe empty
    optionalValues.foreach(optionalAdapter.insert(_, session))
    optionalAdapter.loadAllFromCassandra(session).sortBy(_.id) shouldBe optionalValues
  }

}
