package net.reactivecore.cca.utils

import com.datastax.driver.core.DataType.CollectionType
import com.datastax.driver.core.{ Session, UserType }
import net.reactivecore.cca.EncodingException

import scala.collection.mutable
import scala.collection.JavaConverters._

private[cca] class UserTypeCache(tableName: String) {

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
      case collectionType: CollectionType =>
        val subType = collectionType.getTypeArguments.asScala
        subType match {
          case Seq(one: UserType) =>
            one
          case _ => throw new EncodingException(s"Not supported complex list types of more than one value")
        }
      case somethingElse => throw new EncodingException(s"Expected UserType for ${columnName} in ${tableName}, found ${somethingElse}")
    }
  }
}