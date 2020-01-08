package sqlproxy.server

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.kodein.di.Kodein
import org.kodein.di.generic.bind
import org.kodein.di.generic.provider

internal class ServerConfigTest {
    @Test
    fun `test address and port`() {
        kodein = defaultKodein
        assertEquals(8888, ServerConfig.port)
        assertEquals("127.0.0.1", ServerConfig.address)
    }

    @Test
    @Disabled
    fun `address not set`() {
        fun getConfig(): Config {
            return ConfigFactory.defaultApplication()
                    .withValue("database.jdbcUrl", ConfigValueFactory.fromAnyRef(null))
        }
        val localKodein = Kodein {
            extend(defaultKodein, allowOverride = true)
            bind<Config>(overrides = true) with provider { getConfig() }
        }

        kodein = localKodein
        assertNull(ServerConfig.database.jdbcUrl)
    }

    @Test
    fun `jdbc url`() {
        kodein = defaultKodein
        assertEquals("jdbc:sqlite::memory:", ServerConfig.database.jdbcUrl)
    }
}
