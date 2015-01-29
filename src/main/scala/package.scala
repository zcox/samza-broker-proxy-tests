package com.banno

import org.apache.samza.serializers._

package object samza {
  implicit object ByteArrayWrapperSerde extends Serde[ByteArrayWrapper] {
    override def fromBytes(bytes: Array[Byte]): ByteArrayWrapper = new ByteArrayWrapper(bytes)
    override def toBytes(wrapper: ByteArrayWrapper): Array[Byte] = wrapper.bytes
  }

  implicit object ByteArraySerde extends Serde[Array[Byte]] {
    override def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
    override def toBytes(bytes: Array[Byte]): Array[Byte] = bytes
  }

  implicit val stringSerde: Serde[String] = new StringSerde("UTF-8")
  implicit val integerSerde: Serde[Integer] = new IntegerSerde

  implicit object IntSerde extends Serde[Int] {
    override def fromBytes(bytes: Array[Byte]): Int = integerSerde.fromBytes(bytes)
    override def toBytes(int: Int): Array[Byte] = integerSerde.toBytes(int)
  }
}
