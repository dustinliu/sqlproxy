package sqlproxy.server.service

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import sqlproxy.server.SQLProxyConfig
import java.sql.Connection


object DataSource {
    private val ds:  HikariDataSource

    init {
        val config = HikariConfig()
        config.jdbcUrl = SQLProxyConfig.database.jdbcUrl
        config.maxLifetime = SQLProxyConfig.database.connection.maxLifetime.toMillis()

        ds = HikariDataSource(config)
    }

    fun getConnection(): Connection = ds.connection
}
