package net.reactivecore.cca.utils

/**
 * Acceprts writing of any type.
 * The ordering is important.
 */
trait OrderedWriter {
  type Group <: OrderedWriter

  def write[T](value: T)

  def startGroup(columnName: String): Group

  def endGroup(group: Group)
}

case class CompiledGroup(
  name: String,
  values: IndexedSeq[AnyRef]
)

class OrderedWriterCollector(name: String = "") extends OrderedWriter {
  val collector = IndexedSeq.newBuilder[AnyRef]
  def result(): CompiledGroup = CompiledGroup(name, collector.result())

  type Group = OrderedWriterCollector

  override def write[T](value: T): Unit = collector += value.asInstanceOf[AnyRef]

  override def startGroup(columnName: String): Group = new OrderedWriterCollector(columnName)

  override def endGroup(group: Group): Unit = {
    write(group.result())
  }
}

object OrderedWriter {

  def makeCollector(): OrderedWriterCollector = new OrderedWriterCollector()

}
