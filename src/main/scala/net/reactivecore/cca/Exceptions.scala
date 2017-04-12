package net.reactivecore.cca

abstract class CassandraCaseClassAdapterException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause)

class DecodingException(msg: String) extends CassandraCaseClassAdapterException(msg)

class EncodingException(msg: String) extends CassandraCaseClassAdapterException(msg)