package sqlproxy.server

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder
import sqlproxy.protocol.SQLProxyResponse
import sqlproxy.protocol.serialize

class ServerEncoder: MessageToMessageEncoder<SQLProxyResponse>() {
    override fun acceptOutboundMessage(msg: Any?): Boolean =
        super.acceptOutboundMessage(msg) && msg is SQLProxyResponse

    override fun encode(
        ctx: ChannelHandlerContext?,
        msg: SQLProxyResponse?,
        out: MutableList<Any>?
    ) {
        out!!.add(msg!!.serialize())
    }
}
