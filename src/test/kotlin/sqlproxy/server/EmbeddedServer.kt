package sqlproxy.server

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll

internal open class EmbeddedServer {
    private val proxy = SQLProxy()

    @BeforeAll
    fun init() {
        Thread { proxy.start() }.start()
        proxy.awaitStart()
    }

    @AfterAll
    fun destroy() {
        proxy.stop()
    }
}
