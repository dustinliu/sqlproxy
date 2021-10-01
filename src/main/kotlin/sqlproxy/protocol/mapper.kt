package sqlproxy.protocol

import sqlproxy.proto.Common
import sqlproxy.proto.Common.ConnectionCommand
import sqlproxy.proto.Request
import sqlproxy.proto.Request.ProtobufRequest
import sqlproxy.proto.Response
import sqlproxy.proto.Response.ProtobufResponse
import sqlproxy.proto.Response.ProtobufResponse.StatusCode
import sqlproxy.proto.Common.Meta.Type as MetaType

fun ProtobufResponse.Status.toSQLProxyStatus() : SQLProxyResponse.Status =
    when(this.code) {
        StatusCode.SUCCESS -> SQLProxyResponse.Status.Success(this.description)
        StatusCode.ERROR -> SQLProxyResponse.Status.Failure(this.description)
        StatusCode.UNRECOGNIZED, StatusCode.UNUSED, null ->
            throw IllegalArgumentException("unknown response")
    }

fun SQLProxyResponse.Status.toProtobufStatus(): ProtobufResponse.Status =
    when(this) {
        is SQLProxyResponse.Status.Success -> ProtobufResponse.Status.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setDescription(this.description)
            .build()
        is SQLProxyResponse.Status.Failure -> ProtobufResponse.Status.newBuilder()
            .setCode(StatusCode.ERROR)
            .setDescription(this.description)
            .build()
    }



inline fun <reified T: SQLProxyRequest> ProtobufRequest.deserialize(): T =
    when (this.meta.type) {
        MetaType.CONNECTION -> composeConnectionReq(this) as T
        MetaType.UNKNOWN, Common.Meta.Type.UNRECOGNIZED, null ->
            throw IllegalStateException("known request")
    }

inline fun <reified T: SQLProxyResponse> ProtobufResponse.deserialize(): T =
    when(this.meta.type) {
        MetaType.CONNECTION -> composeConnectionRes(this) as T
        MetaType.UNRECOGNIZED, Common.Meta.Type.UNKNOWN, null ->
            throw IllegalArgumentException("unknown protobuf response")
    }

fun composeConnectionReq(msg: ProtobufRequest): SQLProxyRequest =
    when (msg.connection.command) {
        ConnectionCommand.INIT -> ConnectionRequest.InitRequest(msg.meta.requestId)
        ConnectionCommand.CLOSE -> ConnectionRequest.CloseRequest(msg.meta.requestId)
        ConnectionCommand.VALID -> ConnectionRequest.ValidRequest(
            msg.meta.requestId, msg.connection.validTimeout)
        ConnectionCommand.UNRECOGNIZED, ConnectionCommand.UNKNOWN, null ->
            throw IllegalArgumentException("unknown request")
    }

fun composeConnectionRes(msg: ProtobufResponse) =
    when(msg.connection.command) {
        ConnectionCommand.INIT -> ConnectionResponse.InitResponse(msg.meta.requestId,
            msg.status.toSQLProxyStatus())
        ConnectionCommand.CLOSE -> ConnectionResponse.CloseResponse(msg.meta.requestId,
            msg.status.toSQLProxyStatus())
        ConnectionCommand.VALID -> ConnectionResponse.ValidResponse(msg.meta.requestId,
            msg.connection.isValid, msg.status.toSQLProxyStatus())
        ConnectionCommand.UNRECOGNIZED, ConnectionCommand.UNKNOWN, null ->
            throw IllegalArgumentException("unknown protobuf response")
    }

fun SQLProxyRequest.serialize() =
    when(this) {
        is ConnectionRequest.InitRequest -> simpleConnectionReq(this, ConnectionCommand.INIT)
        is ConnectionRequest.CloseRequest -> simpleConnectionReq(this, ConnectionCommand.CLOSE)
        is ConnectionRequest.ValidRequest -> simpleConnectionReq(this, ConnectionCommand.VALID,
            this.timeout)
    }

fun SQLProxyResponse.serialize() =
    when(this) {
        is ConnectionResponse.CloseResponse -> simpleConnectionRes(this, ConnectionCommand.CLOSE)
        is ConnectionResponse.InitResponse -> simpleConnectionRes(this, ConnectionCommand.INIT)
        is ConnectionResponse.ValidResponse -> simpleConnectionRes(this, ConnectionCommand.VALID,
            this.isValid)
    }

private fun newMeta(requestId: Long, type: MetaType) =
    Common.Meta.newBuilder().setRequestId(requestId).setType(type).build()

private fun simpleConnectionReq(msg: ConnectionRequest,
                                command: ConnectionCommand,
                                timeout: Int? = null)
: ProtobufRequest {
    val connReq = Request.ConnectionRequest.newBuilder().setCommand(command)
    timeout?.apply { connReq.validTimeout = timeout }
    return ProtobufRequest.newBuilder().setMeta(newMeta(msg.requestId, MetaType.CONNECTION))
        .setConnection(connReq).build()
}

private fun simpleConnectionRes(msg: ConnectionResponse,
                                command: ConnectionCommand,
                                valid:Boolean? = null)
: ProtobufResponse {
    val connRes = Response.ConnectionResponse.newBuilder().setCommand(command)
    valid?.apply { connRes.isValid = valid }
    return ProtobufResponse.newBuilder().setMeta(newMeta(msg.requestId, MetaType.CONNECTION))
        .setConnection(connRes).setStatus(msg.status.toProtobufStatus())
        .build()
}
