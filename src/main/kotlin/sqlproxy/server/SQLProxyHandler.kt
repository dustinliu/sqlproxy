package sqlproxy.server

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import mu.KotlinLogging
import sqlproxy.protocol.serialize
import sqlproxy.server.service.SQLProxyService
import sqlproxy.server.service.ServerRequestService

private val logger = KotlinLogging.logger {}

class SQLProxyHandler: ChannelInboundHandlerAdapter() {
    val service = SQLProxyService()

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        logger.trace {"message $msg received"}
        require(msg is ServerRequestService) {
            "unknown request, expect SQLProxyRequest, actual ${msg::class}"
        }
        val response = service.invoke(msg)
        ctx.writeAndFlush(response.serialize())
        ctx.channel().metadata()
    }

    override fun channelActive(ctx: ChannelHandlerContext?) {
        logger.trace {"client ${ctx?.channel()?.remoteAddress()} connected"}
        super.channelActive(ctx)
    }

    override fun channelUnregistered(ctx: ChannelHandlerContext?) {
        super.channelUnregistered(ctx)
        if (service.isActive()) {
            logger.warn {
                "client close connection unexpected, client ${ctx?.channel()?.remoteAddress()}"
            }
            service.finish()
        }

        logger.trace {"connection closed, client ${ctx?.channel()?.remoteAddress()}"}
        ctx!!.close()
    }

    //TODO: send error message but not close the channel
    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close()
    }
}
