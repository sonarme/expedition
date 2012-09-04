package com.sonar.expedition.scrawler.crawler

import org.scribe.oauth.OAuthService
import org.scribe.model.{Verb, OAuthRequest, Token}
import org.scribe.builder.ServiceBuilder
import org.scribe.builder.api.DefaultApi10a

class Yelp(consumerKey: String, consumerSecret: String, token: String, tokenSecret: String) {
    val service: OAuthService = new ServiceBuilder().provider(classOf[YelpApi2]).apiKey(consumerKey).apiSecret(consumerSecret).build()
    val accessToken: Token = new Token(token, tokenSecret)

    def search(term: String, latitude: Double, longitude: Double) = {
        val request = new OAuthRequest(Verb.GET, "http://api.yelp.com/v2/search")
        request.addQuerystringParameter("term", term)
        request.addQuerystringParameter("ll", latitude + "," + longitude)
        service.signRequest(accessToken, request)
        val response = request.send()
        response.getBody
    }
}

class YelpApi2 extends DefaultApi10a {
    def getRequestTokenEndpoint = null

    def getAccessTokenEndpoint = null

    def getAuthorizationUrl(requestToken: Token) = null
}
