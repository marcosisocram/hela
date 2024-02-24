package com.marcosisocram

import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.PathTemplateMatch
import java.util.regex.Pattern

private val logger = KotlinLogging.logger {}
private val clientes = mapOf(Pair(1, 100000), Pair(2, 80000), Pair(3, 1000000), Pair(4, 10000000), Pair(5, 500000))

class TransacaoHandle(private val hikariDataSource: HikariDataSource) : HttpHandler {
    override fun handleRequest(exchange: HttpServerExchange) {


        val match = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY)

        val idStr = match.parameters["id"] ?: "0"
        if (idStr != "1" && idStr != "2" && idStr != "3" && idStr != "4" && idStr != "5") {
            exchange.statusCode = 404
            return
        }

        val id: Int = idStr.toInt()

        val sb = StringBuilder()
        exchange.requestReceiver.receiveFullString { _, s ->
            sb.append(s)
        }

//        logger.info { "Body $sb" }

        var valorInt: Int = 0
        var tipo: String = ""
        var descricao: String = ""


        val regexValor =
            Pattern.compile("\"valor\": ?\"?\\W?([a-z-A-Z0-9.]+)\"?|\"tipo\": ?\"([cd])\"|\"descricao\": ?\"([\\w\\s]{1,10})\"")
        val matcher = regexValor.matcher(sb)


        try {
            if (matcher.find()) {
                val valor = matcher.group(1)

                if (valor.indexOf(".") > 0 || valor.contains(Regex("[a-zA-Z]"))) {
                    exchange.statusCode = 422
                    return
                }

                valorInt = valor.toInt()
            }
        } catch (thr: Throwable) {
//            logger.error(thr) { "Erro ao pegar o valor" }
            exchange.statusCode = 422
            return
        }

        try {
            if (!matcher.find()) {
                exchange.statusCode = 422
                return
            }

            tipo = matcher.group(2)

        } catch (thr: Throwable) {
//            logger.error(thr) { "Erro ao pegar o tipo" }

            exchange.statusCode = 422
            return

        }

        try {
            if (!matcher.find()) {
                exchange.statusCode = 422
                return
            }

            descricao = matcher.group(3)
        } catch (thr: Throwable) {
//            logger.error(thr) { "Erro ao pegar a descrição" }
            exchange.statusCode = 422
            return
        }


        val connection1 = hikariDataSource.connection


        connection1.use { connection2 ->
            var saldoAtual = 0

            connection2.prepareStatement("select s from t where c = ? order by r desc fetch first 1 rows only ")//for update wait 0.1
                .use { st ->
                    st.setInt(1, id)

                    st.executeQuery().use { rs ->
                        while (rs.next()) {
                            saldoAtual = rs.getInt("s")
                        }
                    }
                }

            var response = """{
                                            "limite": ${clientes[id]},
                                            """
            if (tipo == "c") {
                connection2.prepareStatement("insert into t (c, v, p, d, s) values ($id, $valorInt, 'c', '$descricao', ${saldoAtual + valorInt})")
                    .use { st ->
                        st.executeUpdate()
                    }

                response += """
                                            "saldo": ${saldoAtual + valorInt}
                                        }
                                    """.trimIndent()


            } else {

                if (saldoAtual - valorInt < (clientes[id]!! * -1)) {
                    connection2.commit()
                    exchange.statusCode = 422
                    return
                } else {
                    connection2.prepareStatement("insert into t (c, v, p, d, s) values ($id, $valorInt, 'd', '$descricao', ${saldoAtual - valorInt})")
                        .use { st ->
                            st.executeUpdate()
                        }
                    response += """
                                            "saldo": ${saldoAtual - valorInt}
                                        }
                                    """.trimIndent()

                }
            }
            exchange.responseSender.send(response)
        }
    }
}
