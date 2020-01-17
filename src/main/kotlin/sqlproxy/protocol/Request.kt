package sqlproxy.protocol

import com.google.protobuf.GeneratedMessageV3
import sqlproxy.proto.Common.Meta
import sqlproxy.proto.Common.Statement
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.RequestOuterClass.Request.Event
import sqlproxy.proto.RequestOuterClass.SQLRequest
import java.util.concurrent.atomic.AtomicLong

class ProxyRequest<T: SubRequest> private constructor(meta: Meta,
                                       val event: Event?,
                                       val subRequest: T? = null)
    : PrimaryMessage(meta) {

    constructor(request: Request) : this(
            request.meta,
            request.event,
            when {
                request.hasSqlRequest() -> ProxySQLRequest(request.sqlRequest)
                else -> null
            }
    )

    companion object {
        fun newBuilder() = Builder()
    }

    override fun toProtoBuf(): GeneratedMessageV3 {
        var builder = Request.newBuilder().setMeta(meta).setEvent(event)
        builder = when (subRequest) {
            is ProxySQLRequest -> builder.setSqlRequest(subRequest.toProtoBuf())
            null -> builder
        }
        return builder.build()
    }

    class Builder {
        private val metaBuilder = Meta.newBuilder()
        private var event: Event? = null
        private var subRequest: SubRequest? = null

        fun setSessionId(sessionId: Long) = this.also { metaBuilder.session = sessionId }

        fun setEvent(e: Event) = this.apply { event = e }

        fun setSubRequest(request: SubRequest) = this.apply { subRequest = request }

        fun build() {
            ProxyRequest(metaBuilder.build(), event, subRequest)
        }

        companion object { private val atomicIdHolder = AtomicLong(1) }
    }
}


sealed class SubRequest: SubMessage

class ProxySQLRequest constructor(stmtId: Int, val sql: String): SubRequest() {
    private val stmt: Statement = Statement.newBuilder().setId(stmtId).build()
    val stmtId = stmt.id

    constructor(request: SQLRequest) : this(request.stmt.id, request.sql)

    override fun toProtoBuf(): SQLRequest =
            SQLRequest.newBuilder().setStmt(stmt).setSql(sql).build()
}
