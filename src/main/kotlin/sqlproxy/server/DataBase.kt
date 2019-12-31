package sqlproxy.server

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection


object DataSource {
    private val config = HikariConfig()
    private val ds by lazy {
        with(config) {
            jdbcUrl = "jdbc:sqlite::memory:"
//            username = "database_usernam"
//            password = "database_password"
//            addDataSourceProperty( "cachePrepStmts" , "true" )
//            addDataSourceProperty( "prepStmtCacheSize" , "250" )
//            addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" )
        }
        HikariDataSource(config)
    }

    fun getConnection() = ds.connection
    fun killConnection(conn: Connection) = ds.evictConnection(conn)
}
