package net.reactivecore.cca

import net.reactivecore.cca.utils.{ CassandraReader, GroupType, OrderedWriter }
import shapeless._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

/**
 * Heart of the Cassandra Case Class Adapter. Describes types in a way in a way
 * that they can be constructed from Rows and Serialized in a useable way.
 */
@implicitNotFound("No implicit converter found, this often means that a sub type has no implicit converter")
sealed trait CassandraConversionCodec[T] {
  def isPrimitive: Boolean

  /**
   * Decodes from Cassandra Reader
   */
  def decodeFrom(reader: CassandraReader): T

  /**
   * Writes into ordered Writer.
   */
  def orderedWrite(instance: T, writer: OrderedWriter): Unit

  private[cca] def forceOrderedWrite(instance: Any, writer: OrderedWriter): Unit = {
    orderedWrite(instance.asInstanceOf[T], writer)
  }
}

/**
 * A Codec for named compound types (e.g. case classes).
 */
case class CompoundCassandraConversionCodec[T](
    fields: List[(String, CassandraConversionCodec[_])],
    // Constructor from pure Scala Types
    constructor: Seq[Any] => T,
    // Deconstructor into pure scala types.
    deconstructor: T => List[Any]) extends CassandraConversionCodec[T] {

  def size: Int = fields.size

  def isPrimitive: Boolean = false

  def decodeFrom(reader: CassandraReader): T = {
    val values = fields.map {
      case (name, subCodec) =>
        subCodec.decodeFrom(reader.getNamed(name))
    }
    constructor(values)
  }

  def orderedWrite(instance: T, writer: OrderedWriter): Unit = {
    val group = writer.startGroup(GroupType.Compound)
    val deconstructed = deconstructor(instance)
    deconstructed.zip(fields).foreach {
      case (subFieldValue, (columnName, subCodec)) =>
        group.setColumnName(columnName)
        subCodec.forceOrderedWrite(subFieldValue, group)
    }
    writer.endGroup(group)
  }
}

abstract class IterableCodec[SubType, IterableType <: Iterable[SubType]] extends CassandraConversionCodec[IterableType] {
  val subCodec: CassandraConversionCodec[SubType]
  override def isPrimitive: Boolean = false
}

case class SetCodec[SubType, CassandraType](subCodec: PrimitiveCassandraConversionCodec[SubType, CassandraType]) extends IterableCodec[SubType, Set[SubType]] {

  override def decodeFrom(reader: CassandraReader): Set[SubType] = {
    reader.getSet(subCodec.classTag).map(subCodec.cassandraToScala).toSet
  }

  override def orderedWrite(instance: Set[SubType], writer: OrderedWriter): Unit = {
    val converted = instance.map(subCodec.scalaToCassandra).asJava
    writer.write(converted)
  }
}

case class SetUdtCodec[SubType, CassandraType](subCodec: CompoundCassandraConversionCodec[SubType]) extends IterableCodec[SubType, Set[SubType]] {

  override def decodeFrom(reader: CassandraReader): Set[SubType] = {
    reader.getUdtSet.map(subCodec.decodeFrom).toSet
  }

  override def orderedWrite(instance: Set[SubType], writer: OrderedWriter): Unit = {
    val group = writer.startGroup(GroupType.SetGroup)
    instance.foreach { v =>
      subCodec.orderedWrite(v, group)
    }
    writer.endGroup(group)
  }
}

case class SeqCodec[SubType, CassandraType](subCodec: PrimitiveCassandraConversionCodec[SubType, CassandraType]) extends IterableCodec[SubType, Seq[SubType]] {

  override def decodeFrom(reader: CassandraReader): Seq[SubType] = {
    reader.getList(subCodec.classTag).map(subCodec.cassandraToScala).toSeq
  }

  override def orderedWrite(instance: Seq[SubType], writer: OrderedWriter): Unit = {
    val converted = instance.map(subCodec.scalaToCassandra).asJava
    writer.write(converted)
  }
}

case class SeqUdtCodec[SubType, CassandraType](subCodec: CompoundCassandraConversionCodec[SubType]) extends IterableCodec[SubType, Seq[SubType]] {

  override def decodeFrom(reader: CassandraReader): Seq[SubType] = {
    reader.getUdtList.map(subCodec.decodeFrom).toSeq
  }

  override def orderedWrite(instance: Seq[SubType], writer: OrderedWriter): Unit = {
    val group = writer.startGroup(GroupType.ListGroup)
    instance.foreach { v =>
      subCodec.orderedWrite(v, group)
    }
    writer.endGroup(group)
  }
}

case class OptionalCodec[SubType](subCodec: CassandraConversionCodec[SubType]) extends CassandraConversionCodec[Option[SubType]] {
  override def decodeFrom(reader: CassandraReader): Option[SubType] = {
    if (reader.isNull) {
      None
    } else {
      Some(subCodec.decodeFrom(reader))
    }
  }

  override def orderedWrite(instance: Option[SubType], writer: OrderedWriter): Unit = {
    instance match {
      case None        => writer.write(null)
      case Some(value) => subCodec.orderedWrite(value, writer)
    }
  }

  override def isPrimitive: Boolean = false
}

object CompoundCassandraConversionCodec {
  def makeEmpty[T](instance: T): CompoundCassandraConversionCodec[T] = CompoundCassandraConversionCodec(
    Nil,
    _ => instance,
    _ => Nil)
}

case class PrimitiveCassandraConversionCodec[T, CassandraType: ClassTag](
    cassandraToScala: CassandraType => T,
    scalaToCassandra: T => CassandraType) extends CassandraConversionCodec[T] {

  val classTag: ClassTag[CassandraType] = the[ClassTag[CassandraType]]

  val cassandraClass: Class[CassandraType] = the[ClassTag[CassandraType]].runtimeClass.asInstanceOf[Class[CassandraType]]

  override def isPrimitive: Boolean = true

  private[cca] def forceScalaToCassandra(o: Any): CassandraType = scalaToCassandra(o.asInstanceOf[T])

  override def decodeFrom(reader: CassandraReader): T = cassandraToScala(reader.get[CassandraType])

  override def orderedWrite(instance: T, writer: OrderedWriter): Unit = {
    writer.write(scalaToCassandra(instance))
  }
}

object PrimitiveCassandraConversionCodec {
  def makeTrivial[T: ClassTag] = PrimitiveCassandraConversionCodec[T, T](
    cassandraToScala = c => if (c == null) {
      throw new DecodingException(s"Got null when value was expected")
    } else c,
    scalaToCassandra = s => s)

  def makeTrivialConverted[T, CassandraType: ClassTag](implicit encoding: T => CassandraType, back: CassandraType => T) = PrimitiveCassandraConversionCodec[T, CassandraType](
    cassandraToScala = back,
    scalaToCassandra = encoding)
}

object CassandraConversionCodec extends LabelledProductTypeClassCompanion[CassandraConversionCodec] with DefaultCodecs {

  object typeClass extends LabelledProductTypeClass[CassandraConversionCodec] {

    override def product[H, T <: HList](name: String, headCodec: CassandraConversionCodec[H], tailCodec: CassandraConversionCodec[T]): CassandraConversionCodec[::[H, T]] = {
      tailCodec match {
        case c: CompoundCassandraConversionCodec[T] =>
          CompoundCassandraConversionCodec[::[H, T]](
            fields = name -> headCodec :: c.fields,
            constructor = values => values.head.asInstanceOf[H] :: c.constructor(values.tail),
            deconstructor = value => value.head :: c.deconstructor(value.tail))
        case somethingElse => throw new IllegalStateException(s"Tails cant be from type ${somethingElse.getClass.getSimpleName}")
      }
    }

    override def emptyProduct: CassandraConversionCodec[HNil] = CompoundCassandraConversionCodec.makeEmpty(HNil)

    override def project[F, G](codec: => CassandraConversionCodec[G], to: (F) => G, from: (G) => F): CassandraConversionCodec[F] = {
      val baseCodec = codec
      baseCodec match {
        case c: CompoundCassandraConversionCodec[G] =>
          CompoundCassandraConversionCodec[F](
            fields = c.fields,
            constructor = values => from(c.constructor(values)),
            deconstructor = value => c.deconstructor(to(value)))
        case somethingElse => throw new IllegalStateException(s"Can't project ${somethingElse.getClass.getSimpleName}")
      }
    }
  }
}
