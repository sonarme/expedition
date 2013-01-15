package com.sonar.expedition.scrawler.jobs

import com.twitter.scalding.{Args, Job}
import com.sonar.scalding.cassandra._
import com.twitter.scalding.SequenceFile
import com.sonar.expedition.scrawler.pipes.DTOProfileInfoPipe
import com.sonar.scalding.cassandra.CassandraSource
import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import me.prettyprint.cassandra.serializers.StringSerializer
import cascading.tuple.Fields

class NotificationFixJob(args: Args) extends DefaultJob(args) with DTOProfileInfoPipe {
    val nameMap = Map(
        """Posizioni Attuali""" -> "Current Positions", """Posizioni Passate""" -> "Past Positions", """Istruzione""" -> "Education", """Connessioni in comune""" -> "Shared Connections", """Impieghi in comune""" -> "Shared Employments", """Istruzione in comune""" -> "Shared Educations", """Citt\u00e0 di residenza in comune""" -> "Shared Hometowns", """Luogo in comune""" -> "Shared Location", """Amici in comune""" -> "Shared Friends", """Gruppi in comune""" -> "Shared Groups", """"Mi piace" in comune""" -> "Shared Likes", """Altri "Mi piace"""" -> "Other Likes", """Info""" -> "Info",
        """Postes actuels""" -> "Current Positions", """Postes ant\u00e9rieurs""" -> "Past Positions", """Formation""" -> "Education", """Connexions partag\u00e9es""" -> "Shared Connections", """Emplois partag\u00e9s""" -> "Shared Employments", """Formations partag\u00e9es""" -> "Shared Educations", """Villes natales partag\u00e9es""" -> "Shared Hometowns", """Lieu partag\u00e9""" -> "Shared Location", """Amis en commun""" -> "Shared Friends", """Groupes partag\u00e9s""" -> "Shared Groups", """"J'aime" en commun""" -> "Shared Likes", """Autres "J'aime"""" -> "Other Likes", """Infos""" -> "Info",
        """Posiciones actuales""" -> "Current Positions", """Posiciones anteriores""" -> "Past Positions", """Educaci\u00f3n""" -> "Education", """Conexiones compartidas""" -> "Shared Connections", """Empleos compartidos""" -> "Shared Employments", """Educaci\u00f3n compartida""" -> "Shared Educations", """Ciudad natal compartida""" -> "Shared Hometowns", """Ubicaci\u00f3n compartida""" -> "Shared Location", """Amigos compartidos""" -> "Shared Friends", """Grupos compartidos""" -> "Shared Groups", """Intereses compartidos""" -> "Shared Likes", """Otros intereses""" -> "Other Likes", """Informaci\u00f3n""" -> "Info",
        """aktuelle Arbeitsstelle""" -> "Current Positions", """frühere Arbeitsstellen""" -> "Past Positions", """Ausbildung""" -> "Education", """gemeinsame Verbindungen""" -> "Shared Connections", """gemeinsame Arbeit""" -> "Shared Employments", """gemeinsame Ausbildung""" -> "Shared Educations", """gemeinsamer Wohnort""" -> "Shared Hometowns", """gemeinsamer Standort""" -> "Shared Location", """gemeinsame Freunde""" -> "Shared Friends", """gemeinsame Gruppen""" -> "Shared Groups", """gemeinsame Vorlieben""" -> "Shared Likes", """andere Vorlieben""" -> "Other Likes", """Information""" -> "Info",
        """\u0422\u0435\u043a\u0443\u0449\u0438\u0435 \u043f\u043e\u0437\u0438\u0446\u0438\u0438""" -> "Current Positions", """\u041f\u0440\u0435\u0434\u044b\u0434\u0443\u0449\u0438\u0435 \u043f\u043e\u0437\u0438\u0446\u0438\u0438""" -> "Past Positions", """\u041e\u0431\u0440\u0430\u0437\u043e\u0432\u0430\u043d\u0438\u0435""" -> "Education", """\u041e\u0431\u0449\u0438\u0435 \u0437\u043d\u0430\u043a\u043e\u043c\u044b\u0435""" -> "Shared Connections", """\u041e\u0431\u0449\u0438\u0435 \u0437\u0430\u043d\u044f\u0442\u0438\u044f""" -> "Shared Employments", """\u041e\u0431\u0449\u0438\u0435 \u0437\u0430\u043d\u044f\u0442\u0438\u044f""" -> "Shared Educations", """\u041e\u0431\u0449\u0438\u0435 \u0440\u043e\u0434\u043d\u044b\u0435 \u0433\u043e\u0440\u043e\u0434\u0430""" -> "Shared Hometowns", """\u041e\u0431\u0449\u0435\u0435 \u043c\u0435\u0441\u0442\u043e\u043f\u043e\u043b\u043e\u0436\u0435\u043d\u0438\u0435""" -> "Shared Location", """\u041e\u0431\u0449\u0438\u0435 \u0434\u0440\u0443\u0437\u044c\u044f""" -> "Shared Friends", """\u041e\u0431\u0449\u0438\u0435 \u0433\u0440\u0443\u043f\u043f\u044b""" -> "Shared Groups", """\u041e\u0431\u0449\u0438\u0435 \u0441\u0438\u043c\u043f\u0430\u0442\u0438\u0438""" -> "Shared Likes", """\u0414\u0440\u0443\u0433\u0438\u0435 \u0441\u0438\u043c\u043f\u0430\u0442\u0438\u0438""" -> "Other Likes", """\u0418\u043d\u0444\u043e""" -> "Info",
        """Posi\u00e7\u00f5es Atuais""" -> "Current Positions", """Posi\u00e7\u00f5es Passadas""" -> "Past Positions", """Educa\u00e7\u00e3o""" -> "Education", """Conex\u00f5es Compartilhadas""" -> "Shared Connections", """Empregos Compartilhados""" -> "Shared Employments", """Estudos Compartilhados""" -> "Shared Educations", """Cidades Compartilhadas""" -> "Shared Hometowns", """Local Compartilhado""" -> "Shared Location", """Amigos Compartilhados""" -> "Shared Friends", """Grupos Compartilhados""" -> "Shared Groups", """Curtir Compartilhado""" -> "Shared Likes", """Outros Curtir""" -> "Other Likes", """Informa\u00e7\u00f5es""" -> "Info"
    ).filterNot {
        case (key, value) => key == value
    }
    val headerMap = Map(
        """Datori di lavoro""" -> "Employers", """Istruzione""" -> "Education", """Citt\u00e0 di residenza""" -> "Hometown", """Luogo""" -> "Location",
        """Employeurs""" -> "Employers", """Formation""" -> "Education", """Ville natale""" -> "Hometown", """Lieu""" -> "Location",
        """Empleadores""" -> "Employers", """Educaci\u00f3n""" -> "Education", """Ciudad de residencia""" -> "Hometown", """Ubicaci\u00f3n""" -> "Location",
        """Arbeitgeber""" -> "Employers", """Ausbildung""" -> "Education", """Wohnort""" -> "Hometown", """aktueller Ort""" -> "Location",
        """\u0420\u0430\u0431\u043e\u0442\u043e\u0434\u0430\u0442\u0435\u043b\u0438""" -> "Employers", """\u041e\u0431\u0440\u0430\u0437\u043e\u0432\u0430\u043d\u0438\u0435""" -> "Education", """\u0420\u043e\u0434\u043d\u043e\u0439 \u0433\u043e\u0440\u043e\u0434""" -> "Hometown", """\u041c\u0435\u0441\u0442\u043e\u043f\u043e\u043b\u043e\u0436\u0435\u043d\u0438\u0435""" -> "Location",
        """Empregadores""" -> "Employers", """Educa\u00e7\u00e3o""" -> "Education", """Cidade de Resid\u00eancia""" -> "Hometown", """Local""" -> "Location"
    ).filterNot {
        case (key, value) => key == value
    }
    val rpcHostArg = args("rpcHost")
    val profiles = CassandraSource(
        rpcHost = rpcHostArg,
        keyspaceName = "dossier",
        columnFamilyName = "Notification",
        scheme = WideRowScheme(keyField = 'rowKeyBuffer,
            nameValueFields = ('columnNameBuffer, 'jsondataBuffer))
    ).read
    profiles
            .flatMapTo(('rowKeyBuffer, 'columnNameBuffer, 'jsondataBuffer) ->('rowKey, 'columnName, 'json)) {
        in: (ByteBuffer, ByteBuffer, ByteBuffer) =>
            val (rowKeyBuffer, columnNameBuffer, jsondataBuffer) = in
            if (jsondataBuffer == null || !jsondataBuffer.hasRemaining) None
            else {
                var json = StringSerializer.get().fromByteBuffer(jsondataBuffer)
                if (json.startsWith( """{"type":"rec""")) {
                    var modified = false
                    for ((key, value) <- nameMap) {
                        val nameStr = """{"name":"""" + key + """","data""""
                        if (json.contains(nameStr)) {
                            val valueStr = """{"name":"""" + value + """","data""""
                            json = json.replaceAllLiterally(nameStr, valueStr)
                            modified = true
                        }
                    }
                    for ((key, value) <- headerMap) {
                        val nameStr = """{"header":"""" + key + """","body""""
                        if (json.contains(nameStr)) {
                            val valueStr = """{"header":"""" + value + """","body""""
                            json = json.replaceAllLiterally(nameStr, valueStr)
                            modified = true
                        }
                    }
                    if (modified) Some((rowKeyBuffer, columnNameBuffer, json))
                    else None
                } else None
            }
    }.write(CassandraSource(
        rpcHost = rpcHostArg,
        keyspaceName = "dossier",
        columnFamilyName = "Notification",
        scheme = WideRowScheme(keyField = 'rowKey))
    )
}
