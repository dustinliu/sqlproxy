package sqlproxy.client

import com.google.protobuf.MessageLite
import mu.KotlinLogging
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.ResponseOuterClass.Response
import sqlproxy.proto.RequestOuterClass.Request.Event.CLOSE
import sqlproxy.proto.RequestOuterClass.Request.Event.CONNECT
import sqlproxy.proto.RequestOuterClass.Request.Event.CREATE_STMT
import sqlproxy.proto.RequestOuterClass.Request.Event.SQL_QUERY
import sqlproxy.proto.RequestOuterClass.Request.Event.SQL_UPDATE
import sqlproxy.proto.ResponseOuterClass.RowResponse
import java.net.InetSocketAddress
import java.net.Socket
import kotlin.system.measureTimeMillis


data class Result<T: MessageLite>(val request: Request, val response: T)

class ProxyClient {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private lateinit var socket: Socket
    private var sessionId: String? = null

    fun connect(host: String, port: Int): Result<Response> {
        socket = Socket()
        val isa = InetSocketAddress(host, port)
        socket.connect(isa, 5000)

        val request = newRequestBuilder(CONNECT).build()
        writeRequest(request)
        val response = readResponse()
        sessionId = response.meta.session
        return Result(request, response)
    }

    fun close(): Result<Response> {
        val request = newRequestBuilder(CLOSE, sessionId).build()
        writeRequest(request)
        val response = readResponse()
        socket.close()
        return Result(request, response)
    }

    fun createStmt(): Result<Response> {
        val request = newRequestBuilder(CREATE_STMT, sessionId).build()
        writeRequest(request)
        val response = readResponse()
        return Result(request, response)
    }

    fun execUpdate(stmtId: String, sql: String): Result<Response> {
        val builder = newRequestBuilder(SQL_UPDATE, sessionId, stmtId)
        val request= builder.setSql(sql).build()
        writeRequest(request)
        val response = readResponse()
        return Result(request, response)
    }

    fun execQuery(stmtId: String, sql: String): List<Result<MessageLite>> {
        val res = mutableListOf<Result<MessageLite>>()
        val builder = newRequestBuilder(SQL_QUERY, sessionId, stmtId)
        val request = builder.setSql(sql).build()
        writeRequest(request)
        val response = readResponse()
        res.add(Result(request, response))

        var rowResponse = RowResponse.parseDelimitedFrom(socket.getInputStream())
        while (rowResponse.hasData) {
            res.add(Result(request, rowResponse))
            rowResponse = RowResponse.parseDelimitedFrom(socket.getInputStream())
        }
        return res
    }

    private fun writeRequest(request: Request) {
        logger.debug { "request id: ${request.meta.id}" }
        logger.debug { "request session: ${request.meta.session}" }
        request.writeDelimitedTo(socket.getOutputStream())
        logger.debug { "request sent" }
    }

    private fun readResponse(): Response {
        val response = Response.parseDelimitedFrom(socket.getInputStream())
        logger.debug{ "response received" }
        return response
    }
}

fun main() {
    val client = ProxyClient()
    repeat(1000) {
        val t = measureTimeMillis {
            client.connect("localhost", 8888)
            client.close()
        }
        println(t)
    }
}
