package sqlproxy.protocol

import org.junit.jupiter.api.Test
import sqlproxy.proto.Common.Meta
import sqlproxy.proto.RequestOuterClass.Request
import org.junit.jupiter.api.Assertions.*

internal class ProxyResponseTest {
    @Test
    fun `test successResponse  with session id`() {
        val meta = Meta.newBuilder().setRequestId(1).setSessionId(2).build()
        val request = ProxyRequest(Request.newBuilder().setMeta(meta).build())
        val response = ProxyResponse.successResponse(request)
        assertEquals(1, response.requestId)
        assertEquals(2, response.sessionId)
    }

    @Test
    fun `test newSuccessBuilder without session id`() {
        val meta = Meta.newBuilder().setRequestId(1).setSessionId(2).build()
        val request = ProxyRequest(Request.newBuilder().setMeta(meta).build())
        val response = ProxyResponse.successResponse(request)
        assertEquals(1, response.requestId)
        assertEquals(2, response.sessionId)
    }
}
