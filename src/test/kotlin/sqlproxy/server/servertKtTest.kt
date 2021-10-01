package sqlproxy.server

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import sqlproxy.protocol.SQLProxyResponse
import sqlproxy.testclient.SimpleClient

internal class ServerKtTest: EmbeddedServer() {
    @Test
    fun connectAndClose() {
        val client = SimpleClient()
        val response1 = client.connect("localhost", 8888)
        assertTrue(response1.status is SQLProxyResponse.Status.Success)

        val response2 = client.close()
        assertTrue(response2.status is SQLProxyResponse.Status.Success)
    }
}
