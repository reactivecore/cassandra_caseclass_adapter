package net.reactivecore.cca

import com.datastax.driver.core.{ Row, Session, UserType }
import net.reactivecore.cca.utils._
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

    val written = writer.result()

    val firstGroup = written.values match {
      case IndexedSeq(CompiledGroup("", values, GroupType.Compound)) => values
      case _ =>
        // should not happen
        throw new EncodingException(s"Expected case class to convert exactly into one group")
    }

    val treated = treatUdtValues(firstGroup, session)
    session.execute(query, treated: _*)
  }

  /**
   * Treats the converion of compound elements into UDTValues.
   * This is dependent to a running session connection (ast there is no way in creating UDTValues without
   * UserType, which is also not possible to create without DB Connection).
   */
  private def treatUdtValues(values: IndexedSeq[AnyRef], session: Session, typeHint: Option[UserType] = None): IndexedSeq[AnyRef] = {
    // TODO: This is messy and only supports depth of one...
    values.map {
      case CompiledGroup(columnName, subValues, GroupType.ListGroup) =>
        val userType = userTypeCache.getUserType(columnName, session)
        val subValuesTreated = treatUdtValues(subValues, session, Some(userType))

        val converted = subValuesTreated.asJava
        converted
      case CompiledGroup(columnName, subValues, GroupType.SetGroup) =>
        val userType = userTypeCache.getUserType(columnName, session)
        val subValuesTreated = treatUdtValues(subValues, session, Some(userType))

        val converted = subValuesTreated.toSet.asJava
        converted
      case CompiledGroup(columnName, subValues, GroupType.Compound) =>
        val subValuesTreated = treatUdtValues(subValues, session)
        val userType = typeHint.getOrElse(userTypeCache.getUserType(columnName, session))
        val value = userType.newValue()

        val fieldNames = userType.getFieldNames.asScala.toIndexedSeq
        if (fieldNames.size != subValues.size) {
          throw new EncodingException(s"Writing UDT in ${columnName} failed for ${tableName}, expected ${fieldNames.size}, found ${subValues.size}")
        }
        // TODO: Converter operation could be cached
        fieldNames.zip(subValuesTreated).foreach {
          case (fieldName, singleFieldValue) if singleFieldValue == null =>
            value.setToNull(fieldName)
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

  val userTypeCache = new UserTypeCache(tableName)
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