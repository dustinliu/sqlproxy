package sqlproxy.client

import io.netty.bootstrap.Bootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.protobuf.ProtobufDecoder
import io.netty.handler.codec.protobuf.ProtobufEncoder
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender
import mu.KotlinLogging
import sqlproxy.proto.Request
import sqlproxy.proto.Response
import ysqlrelay.proto.Common
import java.util.*

private val group = NioEventLoopGroup();

class ProtoBufClient {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    lateinit var channel: Channel

    fun connect(host: String, port: Int) {
        val b = Bootstrap();
        b.group(group).channel(NioSocketChannel::class.java).option(ChannelOption.TCP_NODELAY, true)
            .handler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(ProtobufVarint32FrameDecoder())
                    ch.pipeline().addLast(ProtobufDecoder(Response.SqlResponse.getDefaultInstance()))
                    ch.pipeline().addLast(ProtobufVarint32LengthFieldPrepender())
                    ch.pipeline().addLast(ProtobufEncoder())
                    ch.pipeline().addLast(ClientHandler())
                }
            })

        channel =  b.connect(host, port).channel()
    }

    fun close() {
        val builder = Request.SqlRequest.newBuilder().apply {
            meta = Common.Meta.newBuilder().setId(UUID.randomUUID().toString()).build()
            event = Request.SqlRequest.Event.CLOSE
        }
        channel.writeAndFlush(builder.build())
    }
}


class ClientHandler: ChannelInboundHandlerAdapter() {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    override fun channelActive(ctx: ChannelHandlerContext) {
        val builder = Request.SqlRequest.newBuilder().apply {
            meta = Common.Meta.newBuilder().setId(UUID.randomUUID().toString()).build()
            event = Request.SqlRequest.Event.CONNECT
        }

        ctx.writeAndFlush(builder.build())
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        logger.trace("===============================================")
        val res = msg as Response.SqlResponse
        logger.trace {"id: ${res.meta.id}"}
        logger.trace {"session: ${res.meta.session}"}
    }

    override fun channelUnregistered(ctx: ChannelHandlerContext) {
        group.shutdownGracefully()
        ctx.close()
    }


    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close();
    }
}

fun main() {
    val client = ProtoBufClient()
    client.connect("localhost", 8888)
    client.close()
}

