package com.sonar.expedition.common.serialization

import com.dyuproject.protostuff.{Message, JsonIOUtil}

object Serialization {


    def toByteArray[T <: Message[T]](any: T) = JsonIOUtil.toByteArray(any, any.cachedSchema(), false)

    def fromByteArray[T <: Message[T]](byteArray: Array[Byte], message: T) = {
        JsonIOUtil.mergeFrom(byteArray, message, message.cachedSchema(), false)
        message
    }
}
