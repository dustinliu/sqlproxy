package sqlproxy.server

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import mu.KotlinLogging
import sqlproxy.proto.Request


class NettyServerHandler: ChannelInboundHandlerAdapter() {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        val request = msg as Request.SqlRequest
        ctx.writeAndFlush(handleRequest(request))
        super.channelRead(ctx, msg)
    }

    override fun channelActive(ctx: ChannelHandlerContext?) {
        logger.trace("client ${ctx?.channel()?.remoteAddress()} connected")
        super.channelActive(ctx)
    }

    override fun channelUnregistered(ctx: ChannelHandlerContext?) {
        logger.trace("connection closed, client ${ctx?.channel()?.remoteAddress()}")
        super.channelUnregistered(ctx)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close()
    }
}
