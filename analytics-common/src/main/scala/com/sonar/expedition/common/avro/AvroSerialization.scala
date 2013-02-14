package com.sonar.expedition.common.avro

import com.sonar.expedition.common.adx.search.model.BidRequestHolder
import org.openrtb.mobile.BidRequest
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import java.io.ByteArrayOutputStream
import org.apache.avro.io.{EncoderFactory, DecoderFactory, DirectBinaryEncoder}

object AvroSerialization {
    val registeredClasses = Seq(classOf[BidRequest], classOf[BidRequestHolder])
    val readers =
        registeredClasses.map(
            clazz => clazz -> new SpecificDatumReader(clazz)
        ).toMap[Class[_], SpecificDatumReader[_]]
    val writers =
        registeredClasses.map(
            clazz => clazz -> new SpecificDatumWriter(clazz)
        ).toMap[Class[_], SpecificDatumWriter[_]]

    def toByteArray(any: Any) = {
        val baos = new ByteArrayOutputStream()
        try {
            val writer = writers(any.getClass).asInstanceOf[SpecificDatumWriter[Any]]
            val encoder = EncoderFactory.get().binaryEncoder(baos, null)
            writer.write(any, encoder)
            baos.toByteArray
        } finally {
            baos.close()
        }
    }

    def fromByteArray[T >: Null : Manifest](byteArray: Array[Byte]) = {
        val reader = readers(manifest[T].erasure).asInstanceOf[SpecificDatumReader[T]]
        val decoder = DecoderFactory.get().binaryDecoder(byteArray, null)
        reader.read(null, decoder)
    }
}
