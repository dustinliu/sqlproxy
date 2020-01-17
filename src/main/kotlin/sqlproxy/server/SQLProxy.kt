package sqlproxy.server

import io.netty.bootstrap.ServerBootstrap
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
import mu.KotlinLogging
import org.apache.commons.lang3.SystemUtils
import sqlproxy.proto.RequestOuterClass
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class SQLProxyThreadFactory(private val name: String) : ThreadFactory{
    private var count = AtomicInteger()

    override fun newThread(r: Runnable): Thread {
        return Thread(r, "sqlproxy_" + name + count.addAndGet(1))
    }
}

class SQLProxy {
    companion object {
        val logger = KotlinLogging.logger {}
    }

    private val bossGroup =
        if (SystemUtils.IS_OS_LINUX) EpollEventLoopGroup(0, SQLProxyThreadFactory("boss"))
        else NioEventLoopGroup(0, SQLProxyThreadFactory("boss"))
    private val workerGroup =
        if (SystemUtils.IS_OS_LINUX) EpollEventLoopGroup(200, SQLProxyThreadFactory("worker"))
        else NioEventLoopGroup(200, SQLProxyThreadFactory("worker"))

    private val startCountDown = CountDownLatch(1)
    private val stopCountDown = CountDownLatch(1)

    fun start() {
        val b = ServerBootstrap()
        b.group(bossGroup, workerGroup)
        b.channel(
            if (SystemUtils.IS_OS_LINUX) EpollServerSocketChannel::class.java
            else NioServerSocketChannel::class.java
        )
        b.childHandler(ProtoServerInitializer())
        b.option<Int>(ChannelOption.SO_BACKLOG, 1024)
        b.childOption<Boolean>(ChannelOption.SO_KEEPALIVE, true)

        val f = b.bind(8888).sync()
        startCountDown.countDown()
        logger.info("sqlproxy started")
        f.channel().closeFuture().sync()
    }

    fun awaitStart() {
        startCountDown.await()
    }

    fun stop() {
        workerGroup.shutdownGracefully()
        bossGroup.shutdownGracefully()
        logger.info("sqlproxy shtdown")
        stopCountDown.countDown()
    }

    fun awaitStop() {
        stopCountDown.await()
    }

    fun resetSessions() {
        SessionFactory.resetAll()
    }

    internal fun getSessions() = SessionFactory.getSessions()
}

class ProtoServerInitializer : ChannelInitializer<SocketChannel>() {
    override fun initChannel(ch: SocketChannel) {
        val pipeline = ch.pipeline()
        pipeline.addLast(ProtobufVarint32FrameDecoder())
        pipeline.addLast(ProtobufDecoder(RequestOuterClass.Request.getDefaultInstance()))
        pipeline.addLast(ProtobufVarint32LengthFieldPrepender())
        pipeline.addLast(ProtobufEncoder())
        pipeline.addLast(NettyServerHandler())
    }
}

fun main() {
    val proxy = SQLProxy()
    kodein = defaultKodein
    Thread { proxy.start() }.start()
}
