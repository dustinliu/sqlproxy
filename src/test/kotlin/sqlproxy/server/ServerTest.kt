package sqlproxy.server

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import sqlproxy.client.SQLProxyDriver
import java.sql.DriverManager
import java.util.concurrent.CountDownLatch

internal class ServerTest {
    private val counter = CountDownLatch(1)
    private val proxy = SQLProxy(counter)
    private val url = "jdbc:sqlproxy://localhost:8888"

    @BeforeAll
    fun init() {
        DriverManager.registerDriver(SQLProxyDriver())
        Thread { proxy.start() }.start()
        counter.await()
    }

    @AfterAll()
    fun destroy() {
        proxy.stop()
    }

    @Test
    fun `normal connect and close test`() {
        val conn = DriverManager.getConnection(url)
        assertEquals(1, proxy.getSessions().size)
        conn.close()
        assertEquals(0, proxy.getSessions().size)
    }
}
