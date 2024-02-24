package com.marcosisocram

import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.PathTemplateMatch
import java.time.LocalDateTime

private val clientes = mapOf(Pair(1, 100000), Pair(2, 80000), Pair(3, 1000000), Pair(4, 10000000), Pair(5, 500000))

private val logger = KotlinLogging.logger {}

class ExtratoHandle(private val hikariDataSource: HikariDataSource) : HttpHandler {

    override fun handleRequest(exchange: HttpServerExchange) {

        val match = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY)

        val idStr = match.parameters["id"] ?: "0"
        if(idStr != "1" && idStr != "2" && idStr != "3" && idStr != "4" && idStr != "5") {
            exchange.statusCode = 404
            return
        }

        val id: Int = idStr.toInt()

        val connection1 = hikariDataSource.connection

        var response = """
                            {
                            """

        connection1.use { connection2 ->
            connection2.prepareStatement("select s from t where c = $id order by r desc fetch first 1 rows only")
                .use { st ->
                    st.executeQuery().use { rs ->
                        while (rs.next()) {
                            val string = rs.getString("s")
                            response += """"saldo": {
                                                "total": $string,
                                                "data_extrato": "${LocalDateTime.now()}",
                                                "limite": ${clientes[id]}
                                              },
                                        """.trimIndent()
                        }
                    }

                    if (!response.contains("saldo")) {
                        response += """"saldo": {
                                                "total": 0,
                                                "data_extrato": "${LocalDateTime.now()}",
                                                "limite": ${clientes[id]}
                                            },
                                        """.trimIndent()
                    }
                }
            connection2.prepareStatement("select  v, p, d, r from t where c = $id order by r desc fetch first 10 rows only")
                .use { st ->
                    st.executeQuery().use { rs ->
                        var responseUltimas = """
                                            "ultimas_transacoes": [
                                        """.trimIndent()
                        while (rs.next()) {

                            responseUltimas += """
                                                    {
                                                        "valor": ${rs.getInt("v")},
                                                        "tipo": "${rs.getString("p")}",
                                                        "descricao": "${rs.getString("d")}",
                                                        "realizada_em": "${rs.getTimestamp("r").toLocalDateTime()}"
                                                    },
                                                """.trimIndent()
                        }
                        if (responseUltimas.contains("},")) {
                            responseUltimas = responseUltimas.dropLast(1)
                        }

                        responseUltimas += """
                                                ]
                                            """.trimIndent()

                        response += responseUltimas
                    }
                }
        }
        response += """
                                        }
                                    """.trimIndent()

//        exchange.responseHeaders["Content-Type"] = "application/json"
        val sender = exchange.responseSender

        sender.send(response)

    }
}