package sqlproxy.server

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.kodein.di.generic.instance
import javax.sql.DataSource

internal fun getConfigDataSource(): DataSource {
    val config = HikariConfig()
    with(config) {
        jdbcUrl = "jdbc:sqlite::test.db:"
//        username = "database_usernam"
//        password = "database_password"
//        addDataSourceProperty("cachePrepStmts", "true")
//        addDataSourceProperty("prepStmtCacheSize", "250")
//        addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    }
    return HikariDataSource(config)
}

object DataSourceFactory {
    val dataSource: DataSource by kodein.instance()
}
