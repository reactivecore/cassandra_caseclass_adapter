package net.reactivecore.cca

import java.math.BigInteger
import java.net.InetAddress
import java.sql.Timestamp
import java.util.{ Date, UUID }

import shapeless.{ LabelledGeneric, LabelledProductTypeClass, the }

trait DefaultCodecs {

  private def nullableWrap[A <: AnyRef, B <: AnyRef](f: A => B): A => B = { a =>
    if (a == null) {
      null.asInstanceOf[B]
    } else (f(a))
  }

  implicit val stringCodec = PrimitiveCassandraConversionCodec.makeTrivial[String]

  implicit val intCodec = PrimitiveCassandraConversionCodec.makeTrivialConverted[Int, java.lang.Integer]
  implicit val longCodec = PrimitiveCassandraConversionCodec.makeTrivialConverted[Long, java.lang.Long]
  implicit val boolCodec = PrimitiveCassandraConversionCodec.makeTrivialConverted[Boolean, java.lang.Boolean]

  implicit val uuidCodec = PrimitiveCassandraConversionCodec.makeTrivial[UUID]

  implicit val floatCodec = PrimitiveCassandraConversionCodec.makeTrivialConverted[Float, java.lang.Float]
  implicit val doubleCodec = PrimitiveCassandraConversionCodec.makeTrivialConverted[Double, java.lang.Double]

  implicit val dateCodec = PrimitiveCassandraConversionCodec.makeTrivial[Date]

  implicit val timestampCodec = PrimitiveCassandraConversionCodec[Timestamp, Date](
    cassandraToScala = nullableWrap { d: Date => new Timestamp(d.getTime) },
    scalaToCassandra = s => s
  )

  implicit val ipAdressCodec = PrimitiveCassandraConversionCodec.makeTrivial[InetAddress]

  implicit val bigIntCodec = PrimitiveCassandraConversionCodec[BigInt, BigInteger](
    cassandraToScala = nullableWrap { b: BigInteger => BigInt(b) },
    scalaToCassandra = (b: BigInt) => b.bigInteger
  )

  implicit val bidDecimalCodec = PrimitiveCassandraConversionCodec[BigDecimal, java.math.BigDecimal](
    cassandraToScala = nullableWrap { b: java.math.BigDecimal => BigDecimal(b) },
    scalaToCassandra = s => s.underlying()
  )

  implicit def primitiveSetCodec[T, CassandraType](implicit primitiveCodec: PrimitiveCassandraConversionCodec[T, CassandraType]): CassandraConversionCodec[Set[T]] =
    SetCodec(primitiveCodec)

  implicit def primitiveSeqCodec[T, CassandraType](implicit primitiveCodec: PrimitiveCassandraConversionCodec[T, CassandraType]): CassandraConversionCodec[Seq[T]] =
    SeqCodec(primitiveCodec)

  implicit def udtSeqCodec[T](implicit compoundCassandraConversionCodec: CompoundCassandraConversionCodec[T]): CassandraConversionCodec[Seq[T]] =
    SeqUdtCodec(compoundCassandraConversionCodec)

  implicit def udtSeqCodec[T <: Product](implicit codec: CassandraConversionCodec[T]): CassandraConversionCodec[Seq[T]] = {
    val upcasted = codec.asInstanceOf[CompoundCassandraConversionCodec[T]]
    SeqUdtCodec(upcasted)
  }

  implicit def udtSetCodec[T <: Product](implicit codec: CassandraConversionCodec[T]): CassandraConversionCodec[Set[T]] = {
    val upcasted = codec.asInstanceOf[CompoundCassandraConversionCodec[T]]
    SetUdtCodec(upcasted)
  }

  implicit def optionalCodec[T, CassandraType](implicit subCodec: CassandraConversionCodec[T]): CassandraConversionCodec[Option[T]] =
    OptionalCodec(subCodec)
}
