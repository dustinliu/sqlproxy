package sqlproxy.server

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import mu.KotlinLogging
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.protocol.ProxyRequest


class NettyServerHandler: ChannelInboundHandlerAdapter() {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        if (msg is Request) {
            val responseHolder = ServiceProvider.getService(msg.event)
                    .handleRequest(ProxyRequest(msg))
            responseHolder.write { ctx.write(it.toProtoBuf()) }
            ctx.flush()
            logger.trace { "netty channel flush done"}
        }
        super.channelRead(ctx, msg)
        logger.trace { "channelRead done" }
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
