package net.reactivecore.cca

import java.util.UUID

import com.datastax.oss.driver.api.core.uuid.Uuids

class SimpleDecodeAndEncodeTest extends TestBaseWithCassandra {

  override protected val autoCleanDatabase = false

  case class Person(
      id: UUID,
      name: String,
      age: Int)

  val ddl = s"""
     |CREATE TABLE persons (
     |  id UUID,
     |  name TEXT,
     |  age INT,
     |  PRIMARY KEY (id)
     |);
   """.stripMargin

  trait Env {
    implicit val adapter = CassandraCaseClassAdapter.make[Person]("persons")

    def insertManual(p: Person): Unit = {
      val query = session.prepare("INSERT INTO persons (id, name, age) VALUES (?, ?, ?)")
      session.execute(query.bind(p.id, p.name, p.age.asInstanceOf[AnyRef]))
    }

    val testPerson = Person(Uuids.random(), "John Doe", 42)
  }

  trait EnvWithNewKeyspace extends Env {
    clearKeyspace()
    executeCql(ddl)
  }

  it should "name the items correctly" in new Env {
    adapter.columnNames shouldBe Seq("id", "name", "age")
  }

  it should "deserialize one item properly" in new EnvWithNewKeyspace {
    insertManual(testPerson)

    val backRow = session.execute("SELECT * FROM persons").one()
    val backPerson = adapter.fromRow(backRow)
    backPerson shouldBe testPerson
  }

  it should "insert properly" in new EnvWithNewKeyspace {
    adapter.insert(testPerson, session)
    val backRow = session.execute("SELECT * FROM persons").one()
    val backPerson = adapter.fromRow(backRow)
    backPerson shouldBe testPerson
  }

  it should "load all from cassandra" in new EnvWithNewKeyspace {
    adapter.loadAllFromCassandra(session) shouldBe empty

    adapter.insert(testPerson, session)
    val otherPerson = Person(
      id = UUID.randomUUID(),
      age = 21,
      name = "Alice")
    adapter.insert(otherPerson, session)
    adapter.loadAllFromCassandra(session).sortBy(_.age) shouldBe Seq(otherPerson, testPerson)
  }

}
