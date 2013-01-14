package com.sonar.expedition.scrawler.serializer

import com.esotericsoftware.kryo.serializers.{MapSerializer, CollectionSerializer}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import java.util

class ArrayListSerializer extends CollectionSerializer {
    override def create(kryo: Kryo, input: Input, `type`: Class[util.Collection[_]]) =
        new util.ArrayList[Any]()

}

class HashSetSerializer extends CollectionSerializer {
    override def create(kryo: Kryo, input: Input, `type`: Class[util.Collection[_]]) =
        new util.HashSet[Any]()

}

class HashMapSerializer extends MapSerializer {

    override def create(kryo: Kryo, input: Input, `type`: Class[util.Map[_, _]]) =
        new util.HashMap[Any, Any]()

}


