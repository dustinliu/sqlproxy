package sqlproxy.server

import com.typesafe.config.Config
import org.kodein.di.generic.instance

object ServerConfig {
    private val config: Config by kodein.instance()

    val port by lazy { config.getInt("port") }
    val address: String? = nullifyException { config.getString("address") }

    val database = DataBase(config.getConfig("database"))

    class DataBase(private val config: Config) {
        val jdbcUrl: String by lazy { config.getString("jdbcUrl") }
    }
}

inline fun <T> nullifyException(block: () -> T) = try { block() } catch(_:Exception) { null }
