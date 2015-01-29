package com.banno.samza

import java.util.Arrays

/** Just wraps a byte array and uses Arrays.equals and Arrays.hashCode so that byte arrays can be used as keys in hash maps.
  * This is necessary to work with Samza's CachedStore, see the following for details:
  * http://www.mail-archive.com/dev%40samza.incubator.apache.org/msg02225.html
  * https://issues.apache.org/jira/browse/SAMZA-505
  */
final class ByteArrayWrapper(val bytes: Array[Byte]) {
  override def equals(other: Any): Boolean = 
    other.isInstanceOf[ByteArrayWrapper] && Arrays.equals(bytes, other.asInstanceOf[ByteArrayWrapper].bytes)

  override def hashCode(): Int = Arrays.hashCode(bytes)
}
