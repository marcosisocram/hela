package com.marcosisocram

import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import java.util.regex.Pattern

class DefaultHandle(hikariDataSource: HikariDataSource) : HttpHandler {


    private val extratoHandle = ExtratoHandle(hikariDataSource)
    private val transacaoHandle = TransacaoHandle(hikariDataSource)
    private val logger = KotlinLogging.logger {}


    override fun handleRequest(exchange: HttpServerExchange) {

    }
}