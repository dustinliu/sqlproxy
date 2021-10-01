package sqlproxy.server

import com.sksamuel.hoplite.ConfigLoader
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class ConfigKtTest {
    @Test
    fun loadConfig() {
        val config = ConfigLoader().loadConfigOrThrow<SQLProxyConfigObj>("/sqlproxy.yml")
        assertEquals("dev", config.env)
        assertEquals(8888, config.server.port)
        assertEquals(1800000, config.database.connection.maxLifetime.toMillis())
    }
}
