package net.reactivecore.cca.utils

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.{ ListType, MapType, SetType, UserDefinedType }
import net.reactivecore.cca.EncodingException

import scala.collection.mutable

private[cca] class UserTypeCache(tableName: String) {

  object lock
  var cache: mutable.Map[String, UserDefinedType] = mutable.Map.empty

  def getUserType(columnName: String, session: CqlSession): UserDefinedType = {
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

  private def fetchUserType(columnName: String, session: CqlSession): UserDefinedType = {
    val meta = session.getMetadata
    val column = (for {
      keyspace <- Option(meta.getKeyspace(session.getKeyspace.get().asInternal()).get())
      table <- Option(keyspace.getTable(tableName).get())
      column <- Option(table.getColumn(columnName).get())
    } yield column).getOrElse {
      throw new EncodingException(s"Could not find type for column ${columnName} in ${tableName}")
    }
    column.getType match {
      case user: UserDefinedType => user
      case c: ListType if c.getElementType.isInstanceOf[UserDefinedType] => c.getElementType.asInstanceOf[UserDefinedType]
      case c: SetType if c.getElementType.isInstanceOf[UserDefinedType] => c.getElementType.asInstanceOf[UserDefinedType]
      case c: MapType if c.getKeyType.isInstanceOf[UserDefinedType] => c.getKeyType.asInstanceOf[UserDefinedType]
      case c: MapType if c.getValueType.isInstanceOf[UserDefinedType] => c.getKeyType.asInstanceOf[UserDefinedType]
      case c: MapType => throw new EncodingException(s"Not supported complex list types of more than one value")
      case somethingElse => throw new EncodingException(s"Expected UserType for ${columnName} in ${tableName}, found ${somethingElse}")
    }
  }
}