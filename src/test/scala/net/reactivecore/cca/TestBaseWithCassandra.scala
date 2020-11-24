package net.reactivecore.cca

import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.CqlSession

/**
 * Recreates an empty unittest keyspace upon each single test.
 */
abstract class TestBaseWithCassandra extends TestBase {

  protected val autoCleanDatabase = true

  private val unittestKeyspace = "unittest"

  private val dropUnittestKeyspaceIfExists = s"DROP KEYSPACE IF EXISTS $unittestKeyspace;"

  private val createUnittestKeyspace =
    s"CREATE KEYSPACE $unittestKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

  private var _sessionWithOutKeyspace: Option[CqlSession] = None
  private var _session: Option[CqlSession] = None

  protected def sessionWithoutKeyspace: CqlSession = _sessionWithOutKeyspace.getOrElse {
    throw new IllegalStateException(s"Cluster not initialized")
  }

  protected def session: CqlSession = _session.getOrElse {
    throw new IllegalStateException(s"Sessio not initialized")
  }

  def executeCql(cql: String): Unit = {
    val parts = cql.split(";") // not very accurate, as a ";" could happen in a string / comment, but ok for our test
    parts.foreach { part =>
      val trimmed = part.trim
      if (trimmed.nonEmpty) {
        session.execute(part)
      }
    }
  }

  override protected def beforeAll(): Unit = {
    _sessionWithOutKeyspace = Some(
      CqlSession.builder()
        .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
        .withLocalDatacenter("datacenter1")
        .build())

    if (!autoCleanDatabase) {
      clearKeyspace()
    }
  }

  override protected def afterAll(): Unit = {
    sessionWithoutKeyspace.close()
    _sessionWithOutKeyspace = None
  }

  override protected def beforeEach(): Unit = {
    if (autoCleanDatabase) {
      clearKeyspace()
    }

    _session = Some(
      CqlSession.builder()
        .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
        .withLocalDatacenter("datacenter1")
        .withKeyspace(unittestKeyspace)
        .build())
  }

  override protected def afterEach(): Unit = {
    session.close()
    _session = None
  }

  protected def clearKeyspace(): Unit = {
    sessionWithoutKeyspace.setSchemaMetadataEnabled(false)
    sessionWithoutKeyspace.execute(dropUnittestKeyspaceIfExists)
    sessionWithoutKeyspace.execute(createUnittestKeyspace)
    sessionWithoutKeyspace.setSchemaMetadataEnabled(true)
  }
}
