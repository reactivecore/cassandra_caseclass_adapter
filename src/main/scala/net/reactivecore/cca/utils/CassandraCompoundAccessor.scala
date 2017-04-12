package net.reactivecore.cca.utils

import com.datastax.driver.core.GettableData

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

/**
 * Wraps read access to cassandra compound types (e.g. Rows).
 * Note: methods are allowed to return null as this is the natural cassandra representation.
 */
trait CassandraCompoundAccessor {
  def getByName[CassandraType: ClassTag](name: String): CassandraType

  def getSetByName[CassandraType: ClassTag](name: String): Iterable[CassandraType]

  def getCompoundByName(name: String): Option[CassandraCompoundAccessor]

  def getByNum[CassandraType: ClassTag](num: Int): CassandraType
}

object CassandraCompoundAccessor {

  private case class DefaultCassandraAccessor(row: GettableData) extends CassandraCompoundAccessor {

    override def getByName[CassandraType: ClassTag](name: String): CassandraType = {
      val clazz = implicitly[ClassTag[CassandraType]].runtimeClass.asInstanceOf[Class[CassandraType]]
      row.get(name, clazz)
    }

    override def getSetByName[CassandraType: ClassTag](name: String): Set[CassandraType] = {
      val clazz = implicitly[ClassTag[CassandraType]].runtimeClass.asInstanceOf[Class[CassandraType]]
      val candidate = row.getSet(name, clazz)
      if (candidate == null) {
        null
      } else {
        candidate.asScala.toSet
      }
    }

    override def getByNum[CassandraType: ClassTag](num: Int): CassandraType = {
      val clazz = implicitly[ClassTag[CassandraType]].runtimeClass.asInstanceOf[Class[CassandraType]]
      row.get(num, clazz)
    }

    override def getCompoundByName(name: String): Option[CassandraCompoundAccessor] = {
      Option(row.getUDTValue(name)).map(DefaultCassandraAccessor)
    }
  }

  def make(row: GettableData): CassandraCompoundAccessor = {
    DefaultCassandraAccessor(row)
  }
}

