package net.reactivecore.cca

import java.net.{ Inet4Address, InetAddress }
import java.sql.Timestamp
import java.util.{ Date, UUID }

import com.datastax.driver.core.utils.UUIDs

class TypeTests extends TestBaseWithCassandra {

  case class Foo(
    id: UUID,
    ivalue: Int,
    lvalue: Long,
    fvalue: Float,
    dvalue: Double,
    timestamp: Timestamp,
    timestamp2: Date,
    bvalue: Boolean,
    bigintvalue: BigInt,
    bigdecimal: BigDecimal,
    inet: InetAddress
  )

  val ddl =
    """
      |CREATE TABLE foo (
      |  id TIMEUUID,
      |  ivalue INT,
      |  lvalue BIGINT,
      |  fvalue FLOAT,
      |  dvalue DOUBLE,
      |  timestamp TIMESTAMP,
      |  timestamp2 TIMESTAMP,
      |  bvalue BOOLEAN,
      |  bigintvalue VARINT,
      |  bigdecimal DECIMAL,
      |  inet INET,
      |  PRIMARY KEY(id)
      |)
    """.stripMargin

  trait Env {
    executeCql(ddl)

    val adapter = CassandraCaseClassAdapter.make[Foo]("foo")

    val currentTimeMs = System.currentTimeMillis()

    val instance = Foo(
      id = UUIDs.timeBased(),
      ivalue = 32,
      lvalue = 453496346883423L,
      dvalue = 3.14159242345,
      fvalue = 6.12f,
      timestamp = new Timestamp(currentTimeMs),
      timestamp2 = new Date(currentTimeMs + 1000),
      bvalue = true,
      bigintvalue = BigInt("35358348563496734975643793476987356735967"),
      bigdecimal = BigDecimal("325482395843957349753745.25425745743986734976493"),
      inet = InetAddress.getByName("1.2.3.4")
    )
  }

  it should "read and write" in new Env {
    adapter.loadAllFromCassandra(session) shouldBe empty
    adapter.insert(instance, session)
    adapter.loadAllFromCassandra(session) shouldBe Seq(instance)
  }
}
