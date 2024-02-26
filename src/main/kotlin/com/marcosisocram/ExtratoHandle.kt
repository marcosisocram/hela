package com.marcosisocram

import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.PathTemplateMatch
import kotlinx.coroutines.DelicateCoroutinesApi
import java.time.LocalDateTime

private val clientes = mapOf(Pair(1, 100000), Pair(2, 80000), Pair(3, 1000000), Pair(4, 10000000), Pair(5, 500000))

private val logger = KotlinLogging.logger {}

class ExtratoHandle(private val hikariDataSource: HikariDataSource) : HttpHandler {

    override fun handleRequest(exchange: HttpServerExchange) {

        val match = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY)

        val id: Int = (match.parameters["id"] ?: "0").toInt()
        if (id < 0 || id > 5) {
            exchange.statusCode = 404
            return
        }

        val connection = hikariDataSource.connection

        var response = """
                            {
                            """

        connection.use { itConnection ->

            var responseSaldo = ""

            //saldo
            itConnection.prepareStatement("select saldo from clientes where id = ?")
                .use { itPrep ->
                    itPrep.setInt(1, id)
                    itPrep.executeQuery().use { itRs ->

                        while (itRs.next()) {
                            val string = itRs.getString("saldo")
                            responseSaldo += """ "saldo": {
                                                "total": $string,
                                                "data_extrato": "${LocalDateTime.now()}",
                                                "limite": ${clientes[id]}
                                              },
                                        """.trimIndent()
                        }
                        if (responseSaldo.trimIndent().isEmpty()) {
                            responseSaldo = """
                                    "saldo": {
                                                "total": 0,
                                                "data_extrato": "${LocalDateTime.now()}",
                                                "limite": ${clientes[id]}
                                              },
                                """.trimIndent()
                        }
                    }
                }

            var responseUltimas = """
                                            "ultimas_transacoes": [
                                        """.trimIndent()

            itConnection.prepareStatement("select valor, tipo, descricao, realizada_em from transacoes where id_cliente = ? order by realizada_em desc limit 10")
                .use { itPrepared ->
                    itPrepared.setInt(1, id)
                    itPrepared.executeQuery()
                        .use { itRs ->


                            while (itRs.next()) {

                                responseUltimas += """
                                                    {
                                                        "valor": ${itRs.getInt("valor")},
                                                        "tipo": "${itRs.getString("tipo")}",
                                                        "descricao": "${itRs.getString("descricao")}",
                                                        "realizada_em": "${itRs.getTimestamp("realizada_em").toLocalDateTime()}"
                                                    },
                                                """.trimIndent()
                            }


                            if (responseUltimas.contains("},")) {
                                responseUltimas = responseUltimas.dropLast(1)
                            }

                            responseUltimas += """
                                                ]
                                            """.trimIndent()

                        }
                }

            responseSaldo += responseUltimas

            response += responseSaldo
        }

        response += """
                                        }
                                    """.trimIndent()

        val sender = exchange.responseSender

        sender.send(response)


    }
}