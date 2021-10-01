package sqlproxy.testclient

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import mu.KotlinLogging
import sqlproxy.protocol.SQLProxyResponse

private val logger = KotlinLogging.logger {}

class SimpleClientHandler : ChannelInboundHandlerAdapter() {
    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        require(msg is SQLProxyResponse) {
            "unknown request, expect SQLProxyResponse, actual ${msg::class}"
        }

        logger.debug { "handler receive response ${msg::class}, data: $msg" }
    }

    override fun channelActive(ctx: ChannelHandlerContext?) {
        logger.trace {"client ${ctx?.channel()?.remoteAddress()} connected"}
        super.channelActive(ctx)
    }

    override fun channelUnregistered(ctx: ChannelHandlerContext?) {
        logger.trace {"connection closed, client ${ctx?.channel()?.remoteAddress()}"}
        super.channelUnregistered(ctx)
    }

    //TODO: send error message but not close the channel
    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close()
    }
}
