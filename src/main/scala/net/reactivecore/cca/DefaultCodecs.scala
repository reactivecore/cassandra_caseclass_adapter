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

  implicit val intCodec = PrimitiveCassandraConversionCodec.makeTrivial[Int]
  implicit val longCodec = PrimitiveCassandraConversionCodec.makeTrivial[Long]
  implicit val boolCodec = PrimitiveCassandraConversionCodec.makeTrivial[Boolean]

  implicit val uuidCodec = PrimitiveCassandraConversionCodec.makeTrivial[UUID]

  implicit val floatCodec = PrimitiveCassandraConversionCodec.makeTrivial[Float]
  implicit val doubleCodec = PrimitiveCassandraConversionCodec.makeTrivial[Double]

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

  implicit def setCodec[T: CassandraConversionCodec]: CassandraConversionCodec[Set[T]] = SetCodec(the[CassandraConversionCodec[T]])

  implicit def optionalCodec[T: ClassTag](implicit ev: Null <:< T): CassandraConversionCodec[Option[T]] = PrimitiveCassandraConversionCodec[Option[T], T](
    cassandraToScala = c => Option(c),
    scalaToCassandra = s => s.orNull
  )
}
