package sqlproxy.server

import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import sqlproxy.grpc.SQLProxyGrpc
import sqlproxy.grpc.Sqlproxy
import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit
import javax.management.ObjectName


class SQLProxyServer(private val port: Int) {
    private val logger by lazyLogger()
    private val server: Server = ServerBuilder.forPort(port).addService(SQLProxyService()).build()

    fun start() {
        server.start()
        Runtime.getRuntime().addShutdownHook(object: Thread() {
            override fun run() {
                stop()
            }
        })
        logger.info("sqlproxy start successfully in port $port")
    }

    fun stop() {
        server.shutdown()?.awaitTermination(30, TimeUnit.SECONDS)
        logger.info("sqlproxy shutdown successfully")
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }
}

class SQLProxyService: SQLProxyGrpc.SQLProxyImplBase() {
    override fun connect(
        request: Sqlproxy.ConnectRequest,
        responseObserver: StreamObserver<Sqlproxy.ConnectResponse>
    ) {
        responseObserver.onNext(ConnectHandler().handleRequest(request))
        responseObserver.onCompleted()
    }

    override fun close(
        request: Sqlproxy.CloseRequest,
        responseObserver: StreamObserver<Sqlproxy.CloseResponse>
    ) {
        responseObserver.onNext(CloseHandler().handleRequest(request))
        responseObserver.onCompleted()
    }
}

fun main() {
    val name = ObjectName("sqlproxy.server:type=Session")
    val mbean = SessionInfo()
    ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, name)

    val server = SQLProxyServer(8888)
    server.start()
    server.blockUntilShutdown()
}
