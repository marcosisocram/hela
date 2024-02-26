package com.marcosisocram

import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.PathTemplateMatch
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.redisson.api.RedissonClient
import java.util.regex.Pattern

private val logger = KotlinLogging.logger {}
private val clientes = mapOf(Pair(1, 100000), Pair(2, 80000), Pair(3, 1000000), Pair(4, 10000000), Pair(5, 500000))

class TransacaoHandle(private val hikariDataSource: HikariDataSource, private val redissonClient: RedissonClient) :
    HttpHandler {
    override fun handleRequest(exchange: HttpServerExchange) {


        val match = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY)

        val id: Int = (match.parameters["id"] ?: "0").toInt()
        if (id < 0 || id > 5) {
            exchange.statusCode = 404
            return
        }

        val sb = StringBuilder()
        exchange.requestReceiver.receiveFullString { _, s ->
            sb.append(s)
        }

        var valorInt: Int = 0
        var tipo: String = ""
        var descricao: String = ""


        val regexValor =
            Pattern.compile("\"valor\": ?\"?\\W?([0-9.]+)\"?|\"tipo\": ?\"([cd])\"|\"descricao\": ?\"([\\w\\s]{1,10})\"")
        val matcher = regexValor.matcher(sb)


        try {
            if (matcher.find()) {
                val valor = matcher.group(1)

                if (valor.indexOf(".") > 0) {
                    exchange.statusCode = 422
                    return
                }

                valorInt = valor.toInt()
            }
        } catch (thr: Throwable) {

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
            exchange.statusCode = 422
            return
        }


        val connection = hikariDataSource.connection

        val rLock = redissonClient.getLock("id_client_$id")

        rLock.lock()

        try {
            connection.use { itConn ->
                var saldoAtual = 0

                itConn.prepareStatement("select saldo from clientes where id = ?")
                    .use { itPrep ->
                        itPrep.setInt(1, id)

                        itPrep.executeQuery().use { itRs ->
                            while (itRs.next()) {
                                saldoAtual = itRs.getInt("saldo")
                            }
                        }
                    }

                var response = """{
                                            "limite": ${clientes[id]},
                                            """
                if (tipo == "c") {

                    val saldoNovo = saldoAtual + valorInt
                    itConn.prepareStatement("update clientes set saldo = ? where id = ?")
                        .use { itPrep ->
                            itPrep.setInt(1, saldoNovo)
                            itPrep.setInt(2, id)

                            itPrep.executeUpdate()
                        }

                    itConn.prepareStatement("insert into transacoes (id_cliente, valor, tipo, descricao) values ($id, $valorInt, 'c', '$descricao')")
                        .use { itPrep ->
                            itPrep.executeUpdate()
                        }

                    response += """
                                            "saldo": $saldoNovo
                                        }
                                    """.trimIndent()

                } else {

                    val novoSaldo = saldoAtual - valorInt

                    if (novoSaldo < (clientes[id]!! * -1)) {
                        exchange.statusCode = 422
                        return
                    } else {

                        itConn.prepareStatement("update clientes set saldo = ? where id = ?")
                            .use { itPrep ->
                                itPrep.setInt(1, novoSaldo)
                                itPrep.setInt(2, id)

                                itPrep.executeUpdate()
                            }

                        itConn.prepareStatement("insert into transacoes (id_cliente, valor, tipo, descricao) values ($id, $valorInt, 'd', '$descricao')")
                            .use { st ->
                                st.executeUpdate()
                            }

                        response += """
                                            "saldo": $novoSaldo
                                        }
                                    """.trimIndent()

                    }
                }

                exchange.responseSender.send(response)

            }.also {

                GlobalScope.launch {
                    val conn = hikariDataSource.connection

                    conn.use { itConn ->
                        itConn.prepareStatement("delete from transacoes where id_cliente = ? and id not in ( select id from transacoes where id_cliente = ? order by realizada_em desc limit 10)")
                            .use { it ->
                                it.setInt(1, id)
                                it.setInt(2, id)

                                it.executeUpdate()
                            }
                    }

                    logger.debug { "Limpeza $id" }
                }
            }
        } finally {
            rLock.unlock()
        }


    }
}
