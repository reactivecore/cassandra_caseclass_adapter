package net.reactivecore.cca

import com.datastax.driver.core.{ Cluster, QueryOptions, Session }

/**
 * Recreates an empty unittest keyspace upon each single test.
 */
abstract class TestBaseWithCassandra extends TestBase {

  protected val autoCleanDatabase = true

  private val unittestKeyspace = "unittest"

  private val dropUnittestKeyspaceIfExists = s"DROP KEYSPACE IF EXISTS $unittestKeyspace;"

  private val createUnittestKeyspace =
    s"CREATE KEYSPACE $unittestKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

  private var _session: Option[Session] = None
  private var _cluster: Option[Cluster] = None

  protected def cluster: Cluster = _cluster.getOrElse {
    throw new IllegalStateException(s"Cluster not initialized")
  }

  protected def session: Session = _session.getOrElse {
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
    val queryOptions = new QueryOptions()
      .setRefreshNodeIntervalMillis(0)
      .setRefreshNodeListIntervalMillis(0)
      .setRefreshSchemaIntervalMillis(0)

    _cluster = Some(Cluster.builder()
      .withQueryOptions(queryOptions)
      .addContactPoint("127.0.0.1").build())

    if (!autoCleanDatabase){
      clearKeyspace()
    }
  }

  override protected def afterAll(): Unit = {
    _cluster.get.close()
    _cluster = None
  }

  override protected def beforeEach(): Unit = {
    if (autoCleanDatabase) {
      clearKeyspace()
    }

    _session = Some(cluster.connect(unittestKeyspace))
  }

  override protected def afterEach(): Unit = {
    session.close()
    _session = None
  }

  protected def clearKeyspace(): Unit = {
    val session = cluster.connect()
    session.execute(dropUnittestKeyspaceIfExists)
    session.execute(createUnittestKeyspace)
    session.close()
  }
}
