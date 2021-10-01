package sqlproxy.testclient

import sqlproxy.proto.Response
import sqlproxy.protocol.*
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.atomic.AtomicLong


internal class SimpleClient {
    private lateinit var socket: Socket
    private var sessionId: String? = null

    object idGenerator {
        private val atomicId = AtomicLong()

       fun getId() = atomicId.incrementAndGet()
    }

    fun connect(host: String, port: Int): ConnectionResponse {
        socket = Socket()
        socket.connect(InetSocketAddress(host, port), 5000)

        writeRequest(ConnectionRequest.InitRequest(idGenerator.getId()))
        return readResponse() as ConnectionResponse
    }

    fun close(): ConnectionResponse {
        writeRequest(ConnectionRequest.CloseRequest(idGenerator.getId()))
        return readResponse() as ConnectionResponse
    }

    private fun writeRequest(request: SQLProxyRequest) {
       request.serialize().writeDelimitedTo(socket.getOutputStream())
    }

    private fun readResponse(): SQLProxyResponse {
        return Response.ProtobufResponse.parseDelimitedFrom(socket.getInputStream()).deserialize()
    }
}
