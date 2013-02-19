package com.sonar.expedition.common.avro

import org.apache.avro.specific.{SpecificData, SpecificDatumReader, SpecificDatumWriter}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.avro.io.{EncoderFactory, DecoderFactory}

object AvroSerialization {

    import resource._

    def toByteArray(any: Any) =
        (for (baos <- managed(new ByteArrayOutputStream)) yield {
            // caches schema internally
            val schema = SpecificData.get.getSchema(any.getClass)
            val writer = new SpecificDatumWriter[Any](schema)
            val encoder = EncoderFactory.get().jsonEncoder(schema, baos)
            writer.write(any, encoder)
            encoder.flush()
            baos.toByteArray
        }).opt.get

    def fromByteArray[T >: Null : Manifest](byteArray: Array[Byte]) =
        (for (bais <- managed(new ByteArrayInputStream(byteArray))) yield {
            // caches schema internally
            val schema = SpecificData.get.getSchema(manifest[T].erasure)
            val reader = new SpecificDatumReader[T](schema)
            val decoder = DecoderFactory.get().jsonDecoder(schema, bais)
            reader.read(null, decoder)
        }).opt.get
}
