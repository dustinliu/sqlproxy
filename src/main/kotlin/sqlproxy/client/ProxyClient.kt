package sqlproxy.client

import mu.KotlinLogging
import sqlproxy.proto.Common.Statement
import sqlproxy.proto.RequestOuterClass.Request.Event
import sqlproxy.proto.ResponseOuterClass.Response
import sqlproxy.protocol.ProxyMessage
import sqlproxy.protocol.ProxyRequest
import sqlproxy.protocol.ProxyResponse
import sqlproxy.protocol.ProxySQLRequest
import java.net.InetSocketAddress
import java.net.Socket
import kotlin.system.measureTimeMillis


data class Result<T: ProxyMessage>(val request: ProxyRequest, val response: T)

class ProxyClient {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private lateinit var socket: Socket
    private var sessionId: Long? = null

    fun connect(host: String, port: Int): Result<ProxyResponse> {
        socket = Socket()
        val isa = InetSocketAddress(host, port)
        socket.connect(isa, 5000)

        val request = ProxyRequest.newInstance(Event.CONNECT)
        writeMessage(request)
        val response = readResponse()
        sessionId = response.sessionId
        return Result(request, response)
    }

    fun close(): Result<ProxyResponse> {
        val request = ProxyRequest.newInstance(Event.CLOSE, sessionId)
        writeMessage(request)
        val response = readResponse()
        socket.close()
        return Result(request, response)
    }

    fun createStmt(): Result<ProxyResponse> {
        val request = ProxyRequest.newInstance(Event.CREATE_STMT, sessionId)
        writeMessage(request)
        val response = readResponse()
        return Result(request, response)
    }

    fun execUpdate(stmtId: Int, sql: String): Result<ProxyResponse> {
        val sqlRequest = ProxySQLRequest(stmtId, Statement.Type.STATEMENT, sql)
        val request = ProxyRequest.newInstance(Event.SQL_UPDATE, sessionId, sqlRequest)
        writeMessage(request)
        val response = readResponse()
        return Result(request, response)
    }

//    fun execQuery(stmtId: Int, sql: String): List<Result<MessageLite>> {
//        val res = mutableListOf<Result<MessageLite>>()
//        val sqlRequest = ProxySQLRequest(stmtId, sql)
//        val request = ProxyRequest.newBuilder()
//                .setSessionId(sessionId!!)
//                .setEvent(SQL_UPDATE)
//                .setSubRequest(sqlRequest).build().toProtoBuf()
//        writeMessage(request)
//        val response = readResponse()
//        res.add(Result(request, response))
//
//        var rowResponse = RowResponse.parseDelimitedFrom(socket.getInputStream())
//        while (rowResponse.hasData) {
//            res.add(Result(request, rowResponse))
//            rowResponse = RowResponse.parseDelimitedFrom(socket.getInputStream())
//        }
//        return res
//    }

    private fun writeMessage(request: ProxyRequest) {
        logger.debug { "request id: ${request.requestId}" }
        logger.debug { "request session: ${request.sessionId}" }
        request.toProtoBuf().writeDelimitedTo(socket.getOutputStream())
        logger.debug { "request sent" }
    }

    private fun readResponse(): ProxyResponse {
        val response = Response.parseDelimitedFrom(socket.getInputStream())
        logger.debug{ "response received" }
        return ProxyResponse.fromProtoBuf(response)
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
