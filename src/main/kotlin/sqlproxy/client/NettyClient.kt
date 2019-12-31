package sqlproxy.client

import mu.KotlinLogging
import sqlproxy.proto.Request
import sqlproxy.proto.Response
import ysqlrelay.proto.Common
import java.net.InetSocketAddress
import java.net.Socket
import java.util.*


class ProtoBufClient {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val socket = Socket()
    private lateinit var sessionId: String

    fun connect(host: String, port: Int) {
        val isa = InetSocketAddress(host, port)
        socket.connect(isa, 5000)
        val request = Request.SqlRequest.newBuilder().apply {
            meta = Common.Meta.newBuilder().setId(UUID.randomUUID().toString()).build()
            event = Request.SqlRequest.Event.CONNECT
        }.build()

        logger.trace { "request id: ${request.meta.id}" }
        logger.trace { "request session: ${request.meta.session}" }
        request.writeDelimitedTo(socket.getOutputStream())

        val response = Response.SqlResponse.parseDelimitedFrom(socket.getInputStream())
        sessionId = response.meta.session
        logger.trace { "response id: ${response.meta.id}" }
        logger.trace { "response session: ${response.meta.session}" }
    }

    fun close() {
        val request = Request.SqlRequest.newBuilder().apply {
            meta = Common.Meta.newBuilder().setId(UUID.randomUUID().toString()).setSession(sessionId).build()
            event = Request.SqlRequest.Event.CONNECT
        }.build()

        logger.trace { "request id: ${request.meta.id}" }
        logger.trace { "request session: ${request.meta.session}" }
        request.writeDelimitedTo(socket.getOutputStream())

        val response = Response.SqlResponse.parseDelimitedFrom(socket.getInputStream())
        sessionId = response.meta.session
        logger.trace { "response id: ${response.meta.id}" }
        logger.trace { "response session: ${response.meta.session}" }
        socket.close()
    }
}



fun main() {
    val client = ProtoBufClient()
    client.connect("localhost", 8888)
    client.close()
}

