package net.reactivecore.cca

import java.math.BigInteger
import java.net.InetAddress
import java.sql.Timestamp
import java.util.{ Date, UUID }

import shapeless.the

import scala.reflect.ClassTag

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

  implicit def primitiveOptionalCodec[T, CassandraType](implicit primitiveCodec: PrimitiveCassandraConversionCodec[T, CassandraType]): CassandraConversionCodec[Option[T]] =
    OptionalCodec(primitiveCodec)
}
