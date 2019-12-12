package ysqlrelay.server

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection


object DataSource {
    private val config = HikariConfig()
    val ds by lazy {
        with(config) {
            jdbcUrl = "jdbc_url"
            username = "database_username"
            password = "database_password"
            addDataSourceProperty( "cachePrepStmts" , "true" )
            addDataSourceProperty( "prepStmtCacheSize" , "250" )
            addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" )
        }
        HikariDataSource(config)
    }

    fun getConnection() = ds.getConnection()
    fun killConnection(conn: Connection) = ds.evictConnection(conn)
}
