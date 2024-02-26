package com.marcosisocram

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.undertow.Handlers
import io.undertow.Undertow
import kotlinx.coroutines.*
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config

private val logger = KotlinLogging.logger {}

class Main

fun main() = runBlocking {

    try {

        val port = System.getenv("PORT") ?: "9999"

        val jdbcUrl = System.getenv("DB_URL") ?: "jdbc:h2:tcp://localhost:9092/~/rinha"
        val username = System.getenv("DB_USER") ?: "sa"
        val password = System.getenv("DB_PASSWORD") ?: "sa"
        val poolsize = System.getenv("DB_POOLSIZE") ?: "10"

        val hikariDataSourceAsync = async<HikariDataSource>(Dispatchers.Default) {
            val config = HikariConfig()
            config.username = username
            config.password = password
            config.jdbcUrl = jdbcUrl

            config.driverClassName = "org.h2.Driver"

            config.maximumPoolSize = poolsize.toInt()

            config.addDataSourceProperty("cachePrepStmts", "true")
            config.addDataSourceProperty("prepStmtCacheSize", "250")
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")

            HikariDataSource(config)
        }


        val redissonClientAsync = async<RedissonClient>(Dispatchers.Default) {

            val redisUrl = System.getenv("REDIS_URL") ?: "redis://127.0.0.1:6379"

            val configRedis = Config()
            configRedis.useSingleServer()
                .address = redisUrl

            Redisson.create(configRedis)
        }

        val redissonClient = redissonClientAsync.await()
        val hikariDataSource = hikariDataSourceAsync.await()

        val serverUnder = Undertow.builder()
            .addHttpListener(port.toInt(), "localhost")
            .setHandler(
                Handlers.path().addPrefixPath(
                    "/clientes", Handlers.routing()
                        .get("/{id}/extrato", ExtratoHandle(hikariDataSource))
                        .post("/{id}/transacoes", TransacaoHandle(hikariDataSource, redissonClient))
                )
            )
            .build()

        Runtime.getRuntime().addShutdownHook(Thread {
            serverUnder.stop()
            redissonClient.shutdown()
            hikariDataSource.close()
        })


        launch(Dispatchers.Default) {
            val create = System.getenv("DB_CREATE_TRANSACAO") ?: "false"

            if (create.toBoolean()) {

                val connection = hikariDataSource.connection
                connection.use {

                    it.prepareStatement("drop table if exists transacoes").executeUpdate()
                    it.prepareStatement(
                        "create table transacoes (\n" +
                                "    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n" +
                                "    id_cliente integer not null,\n" +
                                "    valor integer not null,\n" +
                                "    tipo char(1) not null,\n" +
                                "    descricao varchar(10) not null,\n" +
                                "    realizada_em timestamp default current_timestamp not null\n" +
                                ")"
                    ).executeUpdate()

                    it.prepareStatement("CREATE INDEX t_c ON transacoes(id_cliente desc)").executeUpdate()
                    it.prepareStatement("CREATE INDEX t_cr ON transacoes(id_cliente desc, realizada_em desc)")
                        .executeUpdate()

                }

                logger.info { "Tabela transacoes criada" }
            }
        }

        launch(Dispatchers.Default) {

            val create = System.getenv("DB_CREATE_CLIENTE") ?: "false"

            if (create.toBoolean()) {

                val connection = hikariDataSource.connection
                connection.use {

                    it.prepareStatement("drop table if exists clientes").executeUpdate()
                    it.prepareStatement(
                        "create table clientes (\n" +
                                "                id integer not null,\n" +
                                "                saldo integer not null,\n" +
                                "                limite integer not null\n" +
                                "            )"
                    ).executeUpdate()

                    it.prepareStatement("CREATE INDEX c_i ON clientes(id)").executeUpdate()

                    it.prepareStatement("insert into clientes (id, saldo, limite) values (1, 0, 100000)")
                        .executeUpdate()
                    it.prepareStatement("insert into clientes (id, saldo, limite) values (2, 0, 80000)").executeUpdate()
                    it.prepareStatement("insert into clientes (id, saldo, limite) values (3, 0, 1000000)")
                        .executeUpdate()
                    it.prepareStatement("insert into clientes (id, saldo, limite) values (4, 0, 10000000)")
                        .executeUpdate()
                    it.prepareStatement("insert into clientes (id, saldo, limite) values (5, 0, 500000)")
                        .executeUpdate()
                }
                logger.info { "Tabela clientes criada" }
            }
        }

        serverUnder.start()

    } catch (thr: Throwable) {
        logger.error(thr) { "Could not start server ${thr.message}" }
    }
}