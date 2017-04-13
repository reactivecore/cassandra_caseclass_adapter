package net.reactivecore.cca.utils

import com.datastax.driver.core.{ GettableData, Row, UDTValue }
import net.reactivecore.cca.DecodingException

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

/** Generalized access to a cassandra row or single field. */
trait CassandraReader {
  /** Reads a single type, returns None if the type doesn't exist. Throw if it exists, but is from a wrong type. */
  def get[CassandraType: ClassTag]: CassandraType

  /** Reads a single set. Note: cassandra treats null and empty set the same, returns None if the set itself was empty. */
  def getSet[CassandraType: ClassTag]: Iterable[CassandraType]

  /** Reads a set of readers (e.g. UDT in Set). */
  def getUdtSet: Iterable[CassandraReader]

  /** Reads a single seq (List). Note cassandra treats null and empty as the same, returns None if the set itself was empty. */
  def getList[CassandraType: ClassTag]: Iterable[CassandraType]

  /** Reads a list of readers (e.g. UDT in List). */
  def getUdtList: Iterable[CassandraReader]

  def getNamed(name: String): CassandraReader

  def getByNum(num: Int): CassandraReader

  /** If the cell itself is null (e.g. result of getNamed on a non existing value). */
  def isNull: Boolean = false

  /** The reverse path to this value */
  def path: List[Any]

  /** Describes the position of the current reader. */
  def position = path.reverse.mkString("/")
}

object CassandraReader {

  private def clazzOf[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
  private def className[T: ClassTag] = implicitly[ClassTag[T]].toString()

  private case class NullReader(path: List[Any]) extends CassandraReader {
    override def get[CassandraType: ClassTag]: CassandraType = throw new DecodingException(s"Expected non-nullable value at ${position}, got null")

    override def getSet[CassandraType: ClassTag]: Iterable[CassandraType] = Set.empty

    override def getUdtSet: Iterable[CassandraReader] = Set.empty

    override def getList[CassandraType: ClassTag]: Iterable[CassandraType] = Seq.empty

    override def getUdtList: Iterable[CassandraReader] = Seq.empty

    override def getNamed(name: String): CassandraReader = NullReader(name :: path)

    override def getByNum(num: Int): CassandraReader = NullReader(num :: path)

    override def isNull: Boolean = true
  }

  private case class RowCassandraReader(path: List[Any], row: GettableData) extends CassandraReader {
    override def get[CassandraType: ClassTag]: CassandraType = {
      throw new DecodingException(s"Expected ${className[CassandraType]} at ${position}, got pure row")
    }

    override def getSet[CassandraType: ClassTag]: Iterable[CassandraType] = {
      throw new DecodingException(s"Expected Set of ${className[CassandraType]} at ${position}, got pure row")
    }

    override def getList[CassandraType: ClassTag]: Iterable[CassandraType] = {
      throw new DecodingException(s"Expected Seq of ${className[CassandraType]} at ${position}, got pure row")
    }

    override def getUdtList: Iterable[CassandraReader] = {
      throw new DecodingException(s"Expected UDT Seq at ${position}, got pure row")
    }

    override def getUdtSet: Iterable[CassandraReader] = {
      throw new DecodingException(s"Expected UDT Set at ${position}, got pure row")
    }

    override def getNamed(name: String): CassandraReader = {
      if (row.isNull(name)) NullReader(name :: path) else {
        RowCellReaderByCellName(name :: path, row, name)
      }
    }

    override def getByNum(num: Int): CassandraReader = {
      if (row.isNull(num)) {
        NullReader(num :: path)
      } else {
        RowCellReaderByCellId(num :: path, row, num)
      }
    }
  }

  private case class RowCellReaderByCellId(path: List[Any], row: GettableData, cellId: Int) extends CassandraReader {
    override def get[CassandraType: ClassTag]: CassandraType = {
      row.get(cellId, clazzOf[CassandraType])
    }

    override def getSet[CassandraType: ClassTag]: Iterable[CassandraType] = {
      row.getSet(cellId, clazzOf[CassandraType]).asScala
    }

    override def getUdtSet: Iterable[CassandraReader] = {
      row.getSet(cellId, clazzOf[UDTValue]).asScala.map(v => RowCassandraReader("set" :: path, v))
    }

    override def getList[CassandraType: ClassTag]: Iterable[CassandraType] = {
      row.getList(cellId, clazzOf[CassandraType]).asScala
    }

    override def getUdtList: Iterable[CassandraReader] = {
      row.getList(cellId, classOf[UDTValue]).asScala.map(v => RowCassandraReader("list" :: path, v))
    }

    override def getNamed(name: String): CassandraReader = {
      val udtValue = row.getUDTValue(cellId)
      if (udtValue == null || udtValue.isNull(name)) {
        NullReader(name :: path)
      } else {
        RowCellReaderByCellName(name :: path, udtValue, name)
      }
    }

    override def getByNum(num: Int): CassandraReader = {
      val udtValue = row.getUDTValue(cellId)
      if (udtValue == null || udtValue.isNull(num)) {
        NullReader(num :: path)
      } else {
        RowCellReaderByCellId(num :: path, udtValue, num)
      }
    }
  }

  private case class RowCellReaderByCellName(path: List[Any], row: GettableData, columnName: String) extends CassandraReader {
    override def get[CassandraType: ClassTag]: CassandraType = {
      row.get(columnName, clazzOf[CassandraType])
    }

    override def getSet[CassandraType: ClassTag]: Iterable[CassandraType] = {
      row.getSet(columnName, clazzOf[CassandraType]).asScala
    }

    override def getUdtSet: Iterable[CassandraReader] = {
      row.getSet(columnName, clazzOf[UDTValue]).asScala.map(v => RowCassandraReader("set" :: path, v))
    }

    override def getList[CassandraType: ClassTag]: Iterable[CassandraType] = {
      row.getList(columnName, clazzOf[CassandraType]).asScala
    }

    override def getUdtList: Iterable[CassandraReader] = {
      row.getList(columnName, classOf[UDTValue]).asScala.map(v => RowCassandraReader("list" :: path, v))
    }

    override def getNamed(name: String): CassandraReader = {
      val udtValue = row.getUDTValue(columnName)
      if (udtValue == null || udtValue.isNull(name)) {
        NullReader(name :: path)
      } else {
        RowCellReaderByCellName(name :: path, udtValue, name)
      }
    }

    override def getByNum(num: Int): CassandraReader = {
      val udtValue = row.getUDTValue(columnName)
      if (udtValue == null || udtValue.isNull(num)) {
        NullReader(num :: path)
      } else {
        RowCellReaderByCellId(num :: path, udtValue, num)
      }
    }
  }

  def make(row: Row): CassandraReader = RowCassandraReader(List.empty, row)
}
