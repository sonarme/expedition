package com.sonar.expedition.scrawler.serializer

import com.esotericsoftware.kryo.serializers.CollectionSerializer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import java.util

class ArrayListSerializer extends CollectionSerializer {
    override def create(kryo: Kryo, input: Input, `type`: Class[util.Collection[_]]) =
        new util.ArrayList[Any]()

}


