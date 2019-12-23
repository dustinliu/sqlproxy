package sqlproxy.client

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import sqlproxy.grpc.SQLProxyGrpc
import sqlproxy.grpc.Sqlproxy
import sqlproxy.protocol.getMeta
import java.util.*


class SQLProxyClient(host: String, port: Int) {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)
    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    private val blockingStub: SQLProxyGrpc.SQLProxyBlockingStub by lazy { SQLProxyGrpc.newBlockingStub(channel); }
//    private val asyncStub: SQLProxyGrpc.SQLProxyStub by lazy { SQLProxyGrpc.newStub(channel) }
    private var sessionId: String? = null

    init {
        val id = generateId()
        val request = Sqlproxy.ConnectRequest.newBuilder()
            .setMeta(getMeta(id)).build()
        val response = blockingStub.connect(request)

        validateMeta(request.meta, response.meta)
        sessionId = response.meta.session
        logger.trace("request id: $id")
        logger.trace("response id: ${response.meta.id}")
        logger.trace("session: ${response.meta.session}")
    }

    fun close() {
        val id = generateId()
        val request = Sqlproxy.CloseRequest.newBuilder().setMeta(getMeta(id, sessionId)).build()
        val response = blockingStub.close(request)
        validateMeta(request.meta, response.meta)
        logger.trace("request id: $id")
        logger.trace("response id: ${response.meta.id}")
        logger.trace("session: ${response.meta.session}")
    }

    private fun generateId() = UUID.randomUUID().toString()

    private fun validateMeta(requestMeta: Sqlproxy.Meta, responseMeta: Sqlproxy.Meta) {
        if (requestMeta.id != responseMeta.id) {
            throw RuntimeException("meta is not match")
        }
    }
}

fun main() {
    val client = SQLProxyClient("127.0.0.1", 8888)
    client.close()
}
