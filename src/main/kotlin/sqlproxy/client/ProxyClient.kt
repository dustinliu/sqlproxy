package sqlproxy.client

import mu.KotlinLogging
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.ResponseOuterClass.Response
import sqlproxy.protocol.newRequestBuilder
import java.net.InetSocketAddress
import java.net.Socket


data class Result(val request: Request, val response: Response)

class ProxyClient {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val socket = Socket()
    private var sessionId: String? = null

    fun connect(host: String, port: Int): Result {
        val isa = InetSocketAddress(host, port)
        socket.connect(isa, 5000)

        val request = newRequestBuilder(Request.Event.CONNECT).build()
        writeRequest(request)
        val response = readResponse()
        sessionId = response.meta.session
        return Result(request, response)
    }

    fun close(): Result {
        val request = newRequestBuilder(Request.Event.CLOSE, sessionId).build()
        writeRequest(request)
        val response = readResponse()
        socket.close()
        return Result(request, response)
    }

    fun createStmt(): Result {
        val request = newRequestBuilder(Request.Event.CREATE_STMT, sessionId).build()
        writeRequest(request)
        val response = readResponse()
        return Result(request, response)
    }

    fun execUpdate(stmtId: String, sql: String): Result {
        val builder = newRequestBuilder(Request.Event.SQL_UPDATE, sessionId, stmtId)
        val request= builder.setSql(sql).build()
        writeRequest(request)
        val response = readResponse()
        return Result(request, response)
    }

    private fun writeRequest(request: Request) {
        logger.debug { "request id: ${request.meta.id}" }
        logger.debug { "request session: ${request.meta.session}" }
        request.writeDelimitedTo(socket.getOutputStream())
        logger.debug { "request sent"}
    }

    private fun readResponse(): Response {
        val response =  Response.parseDelimitedFrom(socket.getInputStream())
        logger.debug{ "response received" }
        return response
    }
}


fun main() {
    val client = ProxyClient()
    client.connect("localhost", 8888)
    client.close()
}
