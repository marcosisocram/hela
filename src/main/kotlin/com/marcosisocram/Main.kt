package com.marcosisocram

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.undertow.Handlers
import io.undertow.Undertow
import io.undertow.servlet.api.DeploymentInfo
import io.undertow.servlet.api.DeploymentManager
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.*
import java.net.Socket
import java.sql.Connection
import java.time.LocalDateTime
import java.util.concurrent.Executors
import java.util.regex.Pattern

private const val NEW_LINE = "\r\n"

private val clientes = mapOf(Pair(1, 100000), Pair(2, 80000), Pair(3, 1000000), Pair(4, 10000000), Pair(5, 500000))

private val logger = KotlinLogging.logger {}

class Main


@OptIn(DelicateCoroutinesApi::class)
fun main() = runBlocking {


    try {

        val port = System.getenv("PORT") ?: "8080"


        val jdbcUrl = System.getenv("DB_URL") ?: "jdbc:h2:tcp://localhost:9092/~/rinha"
        val username = System.getenv("DB_USER") ?: "sa"
        val password = System.getenv("DB_PASSWORD") ?: "sa"
        val poolsize = System.getenv("DB_POOLSIZE") ?: "10"

        val config = HikariConfig()
        config.username = username
        config.password = password
        config.jdbcUrl = jdbcUrl

        config.driverClassName = "org.h2.Driver"

        config.maximumPoolSize = poolsize.toInt()
        config.isAutoCommit= false
        config.transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ.toString()

        config.addDataSourceProperty("cachePrepStmts", "true")
        config.addDataSourceProperty("prepStmtCacheSize", "250")
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")

        val hikariDataSource = HikariDataSource(config)
        //./application -ifNotExists -tcp -tcpAllowOthers -tcpPort 9092 -web -webAllowOthers -webPort 8082

        val serverUnder = Undertow.builder()
            .addHttpListener(port.toInt(), "localhost")
            .setHandler(
                Handlers.path().addPrefixPath(
                    "/clientes", Handlers.routing()
                        .get("/{id}/extrato", ExtratoHandle(hikariDataSource))
                        .post("/{id}/transacoes", TransacaoHandle(hikariDataSource))
                )
            )
            .build()

        Runtime.getRuntime().addShutdownHook(Thread {
            serverUnder.stop()
            hikariDataSource.close()
        })

        launch(Dispatchers.Default) {
            val create = System.getenv("DB_CREATE") ?: "false"

            if (create.toBoolean()) {

                //pass criar banco
                val connection1 = hikariDataSource.connection
                connection1.use { connection2 ->
                    connection2.prepareStatement("drop table if exists t").executeUpdate()
                    connection2.prepareStatement(
                        "create global temporary table t (\n" +
                                "    i integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n" +
                                "    c integer not null,\n" +
                                "    v integer not null,\n" +
                                "    p char(1) not null,\n" +
                                "    d varchar(10) not null,\n" +
                                "    r timestamp default current_timestamp not null,\n" +
                                "    s integer not null\n" +
                                ")"
                    ).executeUpdate()
                    connection2.prepareStatement("CREATE INDEX t_c ON t(c desc)").executeUpdate()
                    connection2.prepareStatement("CREATE INDEX t_r ON t(r desc)").executeUpdate()
                    connection2.prepareStatement("CREATE INDEX t_cr ON t(c desc, r desc)").executeUpdate()
                    connection2.prepareStatement("CREATE INDEX t_sr ON t(s, r desc)").executeUpdate()
                }

                logger.info { "Tabela criada" }
            }
        }

        serverUnder.start()

    } catch (thr: Throwable) {
        logger.error(thr) { "Could not start server ${thr.message}" }
    }
}