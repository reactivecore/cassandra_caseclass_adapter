package net.reactivecore.cca

import com.datastax.driver.core.{ Row, Session, UserType }
import net.reactivecore.cca.utils.{ CassandraReader, CompiledGroup, OrderedWriter }
import shapeless._

import scala.collection.mutable
import scala.collection.JavaConverters._

trait CassandraCaseClassAdapter[T] {

  /**
   * Deserializes an instance from an Cassandra Row.
   * Access is done by Name.
   */
  def fromRow(row: Row): T

  /**
   * Insert an instance into cassandra.
   */
  def insert(instance: T, session: Session): Unit

  /**
   * Returns the associated table name.
   * @return
   */
  def tableName: String

  /**
   * Returns all column names
   */
  def columnNames: Seq[String]

  /**
   * Load all of from cassandra. (Danger, could be many elements).
   */
  def loadAllFromCassandra(session: Session): Seq[T]
}

private class AutoCassandraCaseClassAdapter[T: CompoundCassandraConversionCodec](val tableName: String) extends CassandraCaseClassAdapter[T] {
  private val codec = the[CompoundCassandraConversionCodec[T]]

  /**
   * Deserializes an instance from an Cassandra Row.
   * Access is done by Name.
   */
  override def fromRow(row: Row): T = {
    val accessor = CassandraReader.make(row)
    codec.decodeFrom(accessor)
  }

  override def columnNames: Seq[String] = {
    codec.fields.map(_._1)
  }

  override def insert(instance: T, session: Session): Unit = {
    val query = s"INSERT INTO ${tableName} (${columnNames.mkString(",")}) VALUES (${columnNames.map(_ => "?").mkString(",")})"
    val writer = OrderedWriter.makeCollector()
    codec.orderedWrite(instance, writer)

    val values = treatUdtValues(writer.result().values, session)

    session.execute(query, values: _*)
  }

  private def treatUdtValues(values: IndexedSeq[AnyRef], session: Session): IndexedSeq[AnyRef] = {
    values.map {
      case CompiledGroup(columnName, values) =>
        val subValuesTreated = treatUdtValues(values, session)
        val userType = userTypeCache.getUserType(columnName, session)
        val value = userType.newValue()

        val fieldNames = userType.getFieldNames.asScala.toIndexedSeq
        if (fieldNames.size != values.size) {
          throw new EncodingException(s"Writing UDT in ${columnName} failed for ${tableName}, expected ${fieldNames.size}, found ${values.size}")
        }
        // TODO: Converter operation could be cached
        fieldNames.zip(subValuesTreated).foreach {
          case (fieldName, singleFieldValue) =>
            value.set(fieldName, singleFieldValue, singleFieldValue.getClass.asInstanceOf[Class[AnyRef]])
        }
        value
      case somethingElse => somethingElse
    }
  }

  override def loadAllFromCassandra(session: Session): Seq[T] = {
    val query = s"SELECT * FROM ${tableName};"
    import scala.collection.JavaConverters._
    session.execute(query).all().asScala.map(fromRow)
  }

  private object userTypeCache {

    object lock
    var cache: mutable.Map[String, UserType] = mutable.Map.empty

    def getUserType(columnName: String, session: Session): UserType = {
      val candidate = lock.synchronized(cache.get(columnName))
      candidate match {
        case Some(v) => v
        case None =>
          val userType = fetchUserType(columnName, session)
          lock.synchronized {
            cache.put(columnName, userType)
          }
          userType
      }
    }

    private def fetchUserType(columnName: String, session: Session): UserType = {
      val meta = session.getCluster.getMetadata
      val column = (for {
        keyspace <- Option(meta.getKeyspace(session.getLoggedKeyspace))
        table <- Option(keyspace.getTable(tableName))
        column <- Option(table.getColumn(columnName))
      } yield column).getOrElse {
        throw new EncodingException(s"Could not find type for column ${columnName} in ${tableName}")
      }
      column.getType match {
        case user: UserType => user
        case somethingElse  => throw new EncodingException(s"Expected UserTYpe for ${columnName} in ${tableName}, found ${somethingElse}")
      }
    }
  }
}

object CassandraCaseClassAdapter {

  /**
   * Generates a new [[CassandraCaseClassAdapter]] with the help of an implicit [[CassandraConversionCodec]].
   *
   * @param tableName cassandra table name this adapter is bound too.
   */
  def make[T <: Product: CassandraConversionCodec](tableName: String): CassandraCaseClassAdapter[T] = new AutoCassandraCaseClassAdapter[T](tableName)(
    implicitly[CassandraConversionCodec[T]].asInstanceOf[CompoundCassandraConversionCodec[T]]
  )

}