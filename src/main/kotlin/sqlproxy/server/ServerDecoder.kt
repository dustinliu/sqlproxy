package sqlproxy.server

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder
import sqlproxy.proto.Request
import sqlproxy.protocol.ConnectionRequest
import sqlproxy.protocol.deserialize
import sqlproxy.server.service.ConnectionCloseService
import sqlproxy.server.service.ConnectionInitService
import sqlproxy.server.service.ConnectionValidService
import sqlproxy.proto.Common.Meta.Type as MetaType

class ServerDecoder: MessageToMessageDecoder<Request.ProtobufRequest>() {
    override fun acceptInboundMessage(msg: Any?): Boolean =
        super.acceptInboundMessage(msg) && msg is Request.ProtobufRequest

    override fun decode(
        ctx: ChannelHandlerContext?,
        msg: Request.ProtobufRequest?,
        out: MutableList<Any>?
    ) {
        val request = when (msg?.meta?.type) {
            MetaType.CONNECTION -> connectionDecode(msg.deserialize())
            MetaType.UNRECOGNIZED, MetaType.UNKNOWN, null ->
                throw IllegalArgumentException(
                    "can't decode protobuf message, unknown request or request is null")
        }
        out!!.add(request)
    }

    private fun connectionDecode(msg: ConnectionRequest) =
        when (msg) {
            is ConnectionRequest.InitRequest -> ConnectionInitService(msg)
            is ConnectionRequest.CloseRequest -> ConnectionCloseService(msg)
            is ConnectionRequest.ValidRequest -> ConnectionValidService(msg, msg.timeout)
        }
}
