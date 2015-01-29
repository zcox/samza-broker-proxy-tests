package com.banno.samza

import org.apache.samza.config.Config
import org.apache.samza.serializers.{Serde, SerdeFactory}

class ByteArrayWrapperSerdeFactory extends SerdeFactory[ByteArrayWrapper] {
  override def getSerde(name: String, config: Config): Serde[ByteArrayWrapper] = ByteArrayWrapperSerde
}
