package com.sonar.expedition.scrawler.pipes

import com.sonar.scalding.cassandra.{WideRowScheme, CassandraSource}
import com.sun.tools.javac.util.ByteBuffer
import org.codehaus.jackson.map.ser.std.StringSerializer
import com.twitter.scalding.Tsv

class TapServiceProfile {

    CassandraSource(
        rpcHost = "yourCassandraIp",
        keyspaceName = "yourKeyspace",
        columnFamilyName = "ProfileView",
        scheme = WideRowScheme(keyField = 'rowKeyBuffer,
            nameField = 'dataListBuffer)
    ).flatMap(('rowKeyBuffer, 'dataListBuffer) ->('rowKey, 'columnName, 'columnValue)) {
        in: (ByteBuffer, List[(ByteBuffer, ByteBuffer)]) => {
            val rowKey = StringSerializer.get().fromByteBuffer(in._1)
            if (in._2 == null) List.empty
            else in._2 map {
                case (columnName, columnValue) => (rowKey, StringSerializer.get().fromByteBuffer(columnName), StringSerializer.get().fromByteBuffer(columnValue))
            }
        }
    }.write(Tsv("/tmp/casstapout.txt"))

}
