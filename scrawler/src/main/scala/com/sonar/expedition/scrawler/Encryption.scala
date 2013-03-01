package com.sonar.expedition.scrawler

import javax.crypto.spec.SecretKeySpec
import javax.crypto.Cipher

import com.sonar.dossier.ScalaGoodies._

object Encryption extends App {
    val secret = "+zcC78yOYfELQ/kQ"
    val secretKey = new SecretKeySpec(secret.getBytes("UTF-8"), "AES")
    val encipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    encipher.init(Cipher.ENCRYPT_MODE, secretKey)

    def encrypt(s: String) = urlSafeBase64Encode(encipher.doFinal(s.getBytes("UTF-8")))
}
