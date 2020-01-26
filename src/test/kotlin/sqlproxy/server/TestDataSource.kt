package sqlproxy.server

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.kodein.di.Kodein
import org.kodein.di.generic.bind
import org.kodein.di.generic.provider
import java.nio.file.Paths
import javax.sql.DataSource

private val dataSource by lazy {
    val config = HikariConfig()
    val dbFullName = Paths.get(System.getProperty("java.io.tmpdir"), "test.db").toString()
    with(config) {
        jdbcUrl = "jdbc:sqlite:$dbFullName"
    }
    HikariDataSource(config)
}

internal fun initTestDataSource(d: DataSource? = null) {
    kodein = Kodein {
        extend(defaultKodein)
        bind<DataSource>(overrides = true) with provider { d?:dataSource }
    }
}