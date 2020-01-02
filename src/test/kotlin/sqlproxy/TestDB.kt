package sqlproxy

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection


object DataSource {
    private val config = HikariConfig()
    val ds by lazy {
        with(config) {
            jdbcUrl = "jdbc:sqlite:sqlproxy.db"
            jdbcUrl = "jdbc:sqlite::memory:"
        }
        HikariDataSource(config)
    }

    fun getConnection(): Connection = ds.connection
    fun killConnection(conn: Connection) = ds.evictConnection(conn)
}
