package net.reactivecore.cca

import java.lang
import java.math.BigInteger
import java.net.InetAddress
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

trait DefaultCodecs {

  implicit val stringCodec: PrimitiveCassandraConversionCodec[String, String] =
    PrimitiveCassandraConversionCodec.makeTrivial[String]
  implicit val intCodec: PrimitiveCassandraConversionCodec[Int, Integer] =
    PrimitiveCassandraConversionCodec.makeTrivialConverted[Int, java.lang.Integer]
  implicit val longCodec: PrimitiveCassandraConversionCodec[Long, lang.Long] =
    PrimitiveCassandraConversionCodec.makeTrivialConverted[Long, java.lang.Long]
  implicit val boolCodec: PrimitiveCassandraConversionCodec[Boolean, lang.Boolean] =
    PrimitiveCassandraConversionCodec.makeTrivialConverted[Boolean, java.lang.Boolean]
  implicit val uuidCodec: PrimitiveCassandraConversionCodec[UUID, UUID] =
    PrimitiveCassandraConversionCodec.makeTrivial[UUID]
  implicit val floatCodec: PrimitiveCassandraConversionCodec[Float, lang.Float] =
    PrimitiveCassandraConversionCodec.makeTrivialConverted[Float, java.lang.Float]
  implicit val doubleCodec: PrimitiveCassandraConversionCodec[Double, lang.Double] =
    PrimitiveCassandraConversionCodec.makeTrivialConverted[Double, java.lang.Double]
  implicit val dateCodec: PrimitiveCassandraConversionCodec[Instant, Instant] =
    PrimitiveCassandraConversionCodec.makeTrivial[Instant]

  implicit val timestampCodec: PrimitiveCassandraConversionCodec[Timestamp, Instant] = PrimitiveCassandraConversionCodec[Timestamp, Instant](
    cassandraToScala = d => new Timestamp(d.toEpochMilli),
    scalaToCassandra = s => s.toInstant)

  implicit val ipAdressCodec: PrimitiveCassandraConversionCodec[InetAddress, InetAddress] =
    PrimitiveCassandraConversionCodec.makeTrivial[InetAddress]

  implicit val bigIntCodec: PrimitiveCassandraConversionCodec[BigInt, BigInteger] =
    PrimitiveCassandraConversionCodec[BigInt, BigInteger](
      cassandraToScala = b => BigInt(b),
      scalaToCassandra = (b: BigInt) => b.bigInteger)

  implicit val bidDecimalCodec: PrimitiveCassandraConversionCodec[BigDecimal, java.math.BigDecimal] =
    PrimitiveCassandraConversionCodec[BigDecimal, java.math.BigDecimal](
      cassandraToScala = b => BigDecimal(b),
      scalaToCassandra = s => s.underlying())

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
