package sqlproxy.server

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.protobuf.ProtobufDecoder
import io.netty.handler.codec.protobuf.ProtobufEncoder
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender
import io.netty.util.concurrent.DefaultEventExecutorGroup
import mu.KotlinLogging
import org.apache.commons.lang3.SystemUtils
import sqlproxy.proto.Request
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

private val logger = KotlinLogging.logger {}

class SQLProxyThreadFactory(private val name: String) : ThreadFactory{
    private var count = AtomicInteger()

    override fun newThread(r: Runnable): Thread {
        return Thread(r, "sqlproxy_" + name + count.addAndGet(1))
    }
}

class SQLProxy {
    private val bossGroup =
        if (SystemUtils.IS_OS_LINUX) EpollEventLoopGroup(0, SQLProxyThreadFactory("boss"))
        else NioEventLoopGroup(0, SQLProxyThreadFactory("boss"))
    private val workerGroup =
        if (SystemUtils.IS_OS_LINUX) EpollEventLoopGroup(200, SQLProxyThreadFactory("worker"))
        else NioEventLoopGroup(200, SQLProxyThreadFactory("worker"))

    private val startCountDown = CountDownLatch(1)
    private val stopCountDown = CountDownLatch(1)

    fun start() {
        logger.debug { SQLProxyConfig }
        val b = ServerBootstrap()
        b.group(bossGroup, workerGroup)
        b.channel(
            if (SystemUtils.IS_OS_LINUX) EpollServerSocketChannel::class.java
            else NioServerSocketChannel::class.java
        )
        b.childHandler(ProtoServerInitializer())
        b.option(ChannelOption.SO_BACKLOG, 1024)
        b.childOption(ChannelOption.SO_KEEPALIVE, true)

        val f = b.bind(SQLProxyConfig.server.address, SQLProxyConfig.server.port).sync()
        startCountDown.countDown()
        logger.info {"sqlproxy started"}
        f.channel().closeFuture().sync()
    }

    fun awaitStart() {
        startCountDown.await()
    }

    fun stop() {
        workerGroup.shutdownGracefully()
        bossGroup.shutdownGracefully()
        logger.info {"sqlproxy shutdown"}
        stopCountDown.countDown()
    }

    fun awaitStop() {
        stopCountDown.await()
    }
}

class ProtoServerInitializer : ChannelInitializer<SocketChannel>() {
    companion object {
        private val eventExecutorGroup = DefaultEventExecutorGroup(
            SQLProxyConfig.server.numOfServiceThread,
            SQLProxyThreadFactory("service")
        )

        fun initPipeline(ch : Channel) {
            val pipeline = ch.pipeline()
            pipeline.addLast(ProtobufVarint32FrameDecoder())
            pipeline.addLast(ProtobufDecoder(Request.ProtobufRequest.getDefaultInstance()))
            pipeline.addLast(ServerDecoder())
            pipeline.addLast(ProtobufVarint32LengthFieldPrepender())
            pipeline.addLast(ProtobufEncoder())
            pipeline.addLast(ServerEncoder())
            pipeline.addLast(eventExecutorGroup, SQLProxyHandler())
        }
    }

    override fun initChannel(ch: SocketChannel) {
        initPipeline(ch)
    }
}

private fun initChannel(ch: Channel) {

}

fun main() {
    val proxy = SQLProxy()
    Thread { proxy.start() }.start()
}
