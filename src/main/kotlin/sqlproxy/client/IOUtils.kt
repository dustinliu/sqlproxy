package sqlproxy.client

import sqlproxy.proto.Response
import sqlproxy.protocol.SQLProxyRequest
import sqlproxy.protocol.SQLProxyResponse
import sqlproxy.protocol.deserialize
import sqlproxy.protocol.serialize
import java.net.Socket

internal fun Socket.sendRequest(r: SQLProxyRequest) {
    r.serialize().writeDelimitedTo(this.getOutputStream())
}

internal fun Socket.receiveResponse(): SQLProxyResponse {
    return Response.ProtobufResponse.parseDelimitedFrom(this.getInputStream()).deserialize()
}

internal inline fun <reified T: SQLProxyResponse> Socket.sendAndReceive(r: SQLProxyRequest): T {
    this.sendRequest(r)
    return this.receiveResponse() as T
}
