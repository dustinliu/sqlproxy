package ysqlrelay.client

import io.netty.bootstrap.Bootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.protobuf.ProtobufDecoder
import io.netty.handler.codec.protobuf.ProtobufEncoder
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender
import ysqlrelay.proto.Request
import ysqlrelay.proto.Response
import java.util.*

class ProtoBufClient {
    fun connect(port: Int, host: String) {
        val group = NioEventLoopGroup();
        try {
            val b = Bootstrap();
            b.group(group).channel(NioSocketChannel::class.java).option(ChannelOption.TCP_NODELAY, true)
                    .handler(object : ChannelInitializer<SocketChannel>() {
                        override fun initChannel(ch: SocketChannel) {
                            ch.pipeline().addLast(ProtobufVarint32FrameDecoder())
                            ch.pipeline().addLast(ProtobufDecoder(Response.SqlResponse.getDefaultInstance()))
                            ch.pipeline().addLast(ProtobufVarint32LengthFieldPrepender())
                            ch.pipeline().addLast(ProtobufEncoder())
                            ch.pipeline().addLast(ProtoBufClientHandler())
                        }
                    })

            val f = b.connect(host, port).sync()

            f.channel().closeFuture().sync()
        } finally {
            group.shutdownGracefully()
        }
    }
}

class ProtoBufClientHandler: ChannelInboundHandlerAdapter() {
    override fun channelActive(ctx: ChannelHandlerContext) {
        val builder = Request.SqlRequest.newBuilder().apply {
            id = UUID.randomUUID().toString()
            cmd = Request.SqlRequest.Command.CONNECT
        }

        ctx.writeAndFlush(builder.build())
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        println("===============================================")
        val res = msg as Response.SqlResponse
        println("id: ${res.id}")
        println("content: ${res.content}")
    }


    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace();
        ctx.close();
    }
}

fun main(args: Array<String>) {
    ProtoBufClient().connect(8888, "localhost")
}

