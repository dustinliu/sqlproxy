package sqlproxy.client

import com.google.protobuf.MessageLite
import mu.KotlinLogging
import sqlproxy.proto.Common.Statement
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.RequestOuterClass.Request.Event.CLOSE
import sqlproxy.proto.RequestOuterClass.Request.Event.CONNECT
import sqlproxy.proto.RequestOuterClass.Request.Event.CREATE_STMT
import sqlproxy.proto.RequestOuterClass.Request.Event.SQL_QUERY
import sqlproxy.proto.RequestOuterClass.Request.Event.SQL_UPDATE
import sqlproxy.proto.RequestOuterClass.SQLRequest
import sqlproxy.proto.ResponseOuterClass.Response
import sqlproxy.proto.ResponseOuterClass.RowResponse
import sqlproxy.protocol.ProxyRequest
import sqlproxy.protocol.ProxyRequest.Companion
import sqlproxy.protocol.ProxySQLRequest
import java.net.InetSocketAddress
import java.net.Socket
import kotlin.system.measureTimeMillis


data class Result<T: MessageLite>(val request: Request, val response: T)

class ProxyClient {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private lateinit var socket: Socket
    private var sessionId: Long? = null

    fun connect(host: String, port: Int): Result<Response> {
        socket = Socket()
        val isa = InetSocketAddress(host, port)
        socket.connect(isa, 5000)

        val request = ProxyRequest.newBuilder().setEvent(CONNECT).build().toProtoBuf()
        writeRequest(request)
        val response = readResponse()
        sessionId = response.meta.session
        return Result(request, response)
    }

    fun close(): Result<Response> {
        val request = ProxyRequest.newBuilder()
                .setSessionId(sessionId!!)
                .setEvent(CLOSE)
                .build().toProtoBuf()
        writeRequest(request)
        val response = readResponse()
        socket.close()
        return Result(request, response)
    }

    fun createStmt(): Result<Response> {
        val request = ProxyRequest.newBuilder()
                .setSessionId(sessionId!!)
                .setEvent(CREATE_STMT)
                .build().toProtoBuf()
        writeRequest(request)
        val response = readResponse()
        return Result(request, response)
    }

    fun execUpdate(stmtId: Int, sql: String): Result<Response> {
        val sqlRequest = ProxySQLRequest(stmtId, sql)
        val request = ProxyRequest.newBuilder()
                .setSessionId(sessionId!!)
                .setEvent(SQL_UPDATE)
                .setSubRequest(sqlRequest).build().toProtoBuf()
        writeRequest(request)
        val response = readResponse()
        return Result(request, response)
    }

    fun execQuery(stmtId: Int, sql: String): List<Result<MessageLite>> {
        val res = mutableListOf<Result<MessageLite>>()
        val sqlRequest = ProxySQLRequest(stmtId, sql)
        val request = ProxyRequest.newBuilder()
                .setSessionId(sessionId!!)
                .setEvent(SQL_UPDATE)
                .setSubRequest(sqlRequest).build().toProtoBuf()
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
