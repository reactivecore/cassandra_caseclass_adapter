package net.reactivecore.cca

import java.util.UUID

class OptionalTest extends TestBaseWithCassandra {

  case class User(
      id: UUID,
      name: String,
      oemail: Option[String]
  )

  val ddl =
    """
      |CREATE TABLE users (
      |  id UUID,
      |  name TEXT,
      |  oemail TEXT, -- optional
      |  PRIMARY KEY(id)
      |)
    """.stripMargin

  trait Env {
    val adapter = CassandraCaseClassAdapter.make[User]("users")

    executeCql(ddl)

    def pureInsert(user: User): Unit = {

      val preparedQuery = session.prepare("INSERT INTO users (id, name, oemail) VALUES (?, ?, ?)")
      session.execute(preparedQuery.bind(user.id, user.name, user.oemail.orNull))
    }

    val withEmail = User(id = UUID.randomUUID(), name = "John Doe", oemail = Some("user@example.com"))
    val withoutEmail = User(id = UUID.randomUUID(), name = "Alice Doe", oemail = None)
  }

  it should "be readable" in new Env {
    adapter.loadAllFromCassandra(session) shouldBe empty
    pureInsert(withEmail)
    pureInsert(withoutEmail)
    adapter.loadAllFromCassandra(session).sortBy(_.name) shouldBe Seq(withoutEmail, withEmail)
  }

  it should "be insertable" in new Env {
    adapter.insert(withEmail, session)
    adapter.insert(withoutEmail, session)
    adapter.loadAllFromCassandra(session).sortBy(_.name) shouldBe Seq(withoutEmail, withEmail)
  }

}
