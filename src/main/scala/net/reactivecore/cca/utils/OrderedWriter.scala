package net.reactivecore.cca.utils

sealed trait GroupType
object GroupType {

  /** Named objects (full rows or UDT Values). */
  case object Compound extends GroupType

  /** List objects. */
  case object ListGroup extends GroupType

  /** Set of objects. */
  case object SetGroup extends GroupType
}

/**
 * Acceprts writing of any type.
 * The ordering is important.
 */
trait OrderedWriter {
  type Group <: OrderedWriter

  def write[T](value: T)

  /** Set the column name of the following type. */
  def setColumnName(columnName: String): Unit

  /** Start an anonymous group (e.g. Lists). */
  def startGroup(groupType: GroupType): Group

  def endGroup(group: Group)
}

case class CompiledGroup(
    name: String,
    values: IndexedSeq[AnyRef],
    groupType: GroupType
) {
  def isAnonymous: Boolean = name.isEmpty
}

class OrderedWriterCollector(name: String = "", groupType: GroupType) extends OrderedWriter {
  val collector = IndexedSeq.newBuilder[AnyRef]
  var nextName: String = ""

  def result(): CompiledGroup = CompiledGroup(name, collector.result(), groupType)

  type Group = OrderedWriterCollector

  override def write[T](value: T): Unit = collector += value.asInstanceOf[AnyRef]

  override def setColumnName(columnName: String): Unit = nextName = columnName

  override def startGroup(groupType: GroupType): Group = new OrderedWriterCollector(nextName.ensuring(_ != null), groupType)

  override def endGroup(group: Group): Unit = {
    write(group.result())
  }
}

object OrderedWriter {

  def makeCollector(): OrderedWriterCollector = new OrderedWriterCollector("", GroupType.Compound)

}
