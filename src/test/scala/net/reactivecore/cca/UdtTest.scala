package net.reactivecore.cca

class UdtTest extends TestBaseWithCassandra {

  case class Address(
      street: String,
      num: Int,
      city: String)

  case class UserWithAdress(
      user: String,
      address: Address)

  val ddl =
    """
      |CREATE TYPE address_type (
      | street TEXT,
      | num INT,
      | city TEXT
      |);
      |
      |CREATE TABLE user_with_addresses (
      |  user TEXT,
      |  address FROZEN<address_type>,
      |  PRIMARY KEY(user)
      |);
    """.stripMargin

  trait Env {
    executeCql(ddl)
    val adapter = CassandraCaseClassAdapter.make[UserWithAdress]("user_with_addresses")

    val testValue = UserWithAdress(
      user = "John Doe",
      address = Address(
        street = "Alexanderplatz",
        num = 1,
        city = "Berlin"))
  }

  it should "load and save" in new Env {
    adapter.loadAllFromCassandra(session) shouldBe empty
    adapter.insert(testValue, session)
    adapter.loadAllFromCassandra(session) shouldBe Seq(testValue)
  }

}
