package net.reactivecore.cca

import java.util.UUID

import com.datastax.driver.core.Session
import net.reactivecore.cca.utils.{ CassandraCompoundAccessor, OrderedWriter }
import shapeless._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

/**
 * Heart of the Cassandra Case Class Adapter. Describes types in a way in a way
 * that they can be constructed from Rows and Serialized in a useable way.
 */
@implicitNotFound("No implicit converter found, this often means that a sub type has no implicit converter")
sealed trait CassandraConversionCodec[T] {
  def isPrimitive: Boolean
}

/**
 * A Codec for named compound types (e.g. case classes).
 */
case class CompoundCassandraConversionCodec[T](
    fields: List[(String, CassandraConversionCodec[_])],
    // Constructor from pure Scala Types
    constructor: Seq[Any] => T,
    // Deconstructor into pure scala types.
    deconstructor: T => List[Any]
) extends CassandraConversionCodec[T] {

  def size: Int = fields.size

  def isPrimitive: Boolean = false

  /**
   * Decode this compound object from a cassandra accessor.
   */
  def decodeFrom(accessor: CassandraCompoundAccessor): T = {
    val values = fields.map {
      case (name, subCodec) =>
        subCodec match {
          case p: PrimitiveCassandraConversionCodec[_, _] =>
            p.cassandraToScala(accessor.getByName(name)(p.classTag))
          case c: CompoundCassandraConversionCodec[_] =>
            val subAccessor = accessor.getCompoundByName(name).getOrElse {
              throw new DecodingException(s"Could not decode ${name}, got empty value. Trying to dereference <null> into a non-nullable sub object?")
            }
            c.decodeFrom(subAccessor)
          case s: SetCodec[_] =>
            s.subCodec match {
              case p: PrimitiveCassandraConversionCodec[_, _] =>
                val set = accessor.getSetByName(name)(p.classTag)
                if (set == null) {
                  null
                } else {
                  set.map(p.cassandraToScala)
                }
              case c: CompoundCassandraConversionCodec[_] =>
                ???
              case _ =>
                throw new DecodingException(s"Strange set in set")
            }
        }
    }
    constructor(values)
  }

  def orderedWrite(instance: T, writer: OrderedWriter): Unit = {
    val deconstructed = deconstructor(instance)
    deconstructed.zip(fields).foreach {
      case (subFieldValue, (columnName, subCodec)) =>
        subCodec match {
          case p: PrimitiveCassandraConversionCodec[_, _] =>
            writer.write(p.forceScalaToCassandra(subFieldValue))
          case c: CompoundCassandraConversionCodec[_] =>
            val subWriter = writer.startGroup(columnName)
            c.forceOrderedWrite(subFieldValue, subWriter)
            writer.endGroup(subWriter)
          case s: SetCodec[_] =>
            // TODO: Move to SetCodec
            s.subCodec match {
              case sp: PrimitiveCassandraConversionCodec[_, _] =>
                import scala.collection.JavaConverters._
                val subFieldValues = subFieldValue.asInstanceOf[Set[_]].map(sp.forceScalaToCassandra).asJava
                writer.write(subFieldValues)
              case _: CompoundCassandraConversionCodec[_] =>
                ???
              case _: SetCodec[_] =>
                ???
            }

        }
    }
  }

  private[cca] def forceOrderedWrite(instance: Any, writer: OrderedWriter): Unit = {
    orderedWrite(instance.asInstanceOf[T], writer)
  }
}

case class SetCodec[T](subCodec: CassandraConversionCodec[T]) extends CassandraConversionCodec[Set[T]]() {
  override def isPrimitive: Boolean = false
}

object CompoundCassandraConversionCodec {
  def makeEmpty[T](instance: T): CompoundCassandraConversionCodec[T] = CompoundCassandraConversionCodec(
    Nil,
    _ => instance,
    _ => Nil
  )
}

case class PrimitiveCassandraConversionCodec[T, CassandraType: ClassTag](
    cassandraToScala: CassandraType => T,
    scalaToCassandra: T => CassandraType
) extends CassandraConversionCodec[T] {

  val classTag: ClassTag[CassandraType] = the[ClassTag[CassandraType]]

  val cassandraClass: Class[CassandraType] = the[ClassTag[CassandraType]].runtimeClass.asInstanceOf[Class[CassandraType]]

  override def isPrimitive: Boolean = true

  private[cca] def forceScalaToCassandra(o: Any): CassandraType = scalaToCassandra(o.asInstanceOf[T])
}

object PrimitiveCassandraConversionCodec {
  def makeTrivial[T: ClassTag] = PrimitiveCassandraConversionCodec[T, T](
    cassandraToScala = c => if (c == null) {
    throw new DecodingException(s"Got null when value was expected")
  } else c,
    scalaToCassandra = s => s
  )
}

object CassandraConversionCodec extends LabelledProductTypeClassCompanion[CassandraConversionCodec] with DefaultCodecs {

  def generate[T <: Product](implicit ct: Lazy[CassandraConversionCodec[T]]): CompoundCassandraConversionCodec[T] = apply[T].asInstanceOf[CompoundCassandraConversionCodec[T]]

  object typeClass extends LabelledProductTypeClass[CassandraConversionCodec] {

    override def product[H, T <: HList](name: String, headCodec: CassandraConversionCodec[H], tailCodec: CassandraConversionCodec[T]): CassandraConversionCodec[::[H, T]] = {
      tailCodec match {
        case c: CompoundCassandraConversionCodec[T] =>
          CompoundCassandraConversionCodec[::[H, T]](
            fields = name -> headCodec :: c.fields,
            constructor = values => values.head.asInstanceOf[H] :: c.constructor(values.tail),
            deconstructor = value => value.head :: c.deconstructor(value.tail)
          )
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
            deconstructor = value => c.deconstructor(to(value))
          )
        case somethingElse => throw new IllegalStateException(s"Can't project ${somethingElse.getClass.getSimpleName}")
      }
    }
  }
}
