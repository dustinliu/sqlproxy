package ysqlrelay.server


import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.*
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.protobuf.ProtobufDecoder
import io.netty.handler.codec.protobuf.ProtobufEncoder
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender
import org.apache.commons.lang3.SystemUtils
import ysqlrelay.proto.Request
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger


class SQLRelayThreadFactory(private val name: String) : ThreadFactory{
    private var count = AtomicInteger()

    override fun newThread(r: Runnable): Thread {
        return Thread(r, name + "-Thread_" + count.addAndGet(1))
    }
}

class SQLRelay {
    fun main(args: Array<String>) {
        val bossGroup =
            if (SystemUtils.IS_OS_LINUX) EpollEventLoopGroup(0, SQLRelayThreadFactory("boss"))
            else NioEventLoopGroup(0, SQLRelayThreadFactory("boss"))
        val workerGroup =
            if (SystemUtils.IS_OS_LINUX) EpollEventLoopGroup(200, SQLRelayThreadFactory("worker"))
            else NioEventLoopGroup(200, SQLRelayThreadFactory("worker"))

        try {
            val b = ServerBootstrap()
            b.group(bossGroup, workerGroup)
            b.channel(NioServerSocketChannel::class.java)
            b.childHandler(ProtoServerInitializer())
            b.option<Int>(ChannelOption.SO_BACKLOG, 1024)
            b.childOption<Boolean>(ChannelOption.SO_KEEPALIVE, true)

            val f: ChannelFuture = b.bind(8888).sync()
            f.channel().closeFuture().sync()
        } finally {
            workerGroup.shutdownGracefully()
            bossGroup.shutdownGracefully()
        }
    }
}

class ProtoServerInitializer : ChannelInitializer<SocketChannel>() {
    override fun initChannel(ch: SocketChannel) {
        val pipeline = ch.pipeline()
        pipeline.addLast(ProtobufVarint32FrameDecoder())
        pipeline.addLast(ProtobufDecoder(Request.SqlRequest.getDefaultInstance()))
        pipeline.addLast(ProtobufVarint32LengthFieldPrepender())
        pipeline.addLast(ProtobufEncoder())
        pipeline.addLast(NettyServerHandler())
    }
}


fun main(args: Array<String>) {
    SQLRelay().main(args)
}
