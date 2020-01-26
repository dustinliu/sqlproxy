package sqlproxy.protocol

import sqlproxy.proto.Common.Meta
import sqlproxy.proto.Common.Statement
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.RequestOuterClass.Request.Event
import sqlproxy.proto.RequestOuterClass.Request.QueryCase.NEXTREQUEST
import sqlproxy.proto.RequestOuterClass.Request.QueryCase.QUERY_NOT_SET
import sqlproxy.proto.RequestOuterClass.Request.QueryCase.SQLREQUEST
import sqlproxy.proto.RequestOuterClass.SQLRequest
import java.util.concurrent.atomic.AtomicLong

class ProxyRequest(private val request: Request): PrimaryMessage {
    override val requestId: Long = request.meta.requestId
    override val sessionId: Long = request.meta.sessionId
    val event: Event = request.event
    val subRequest: SubRequest? = when (request.queryCase) {
        SQLREQUEST -> ProxySQLRequest.fromProtoBuf(request.sqlRequest)
        NEXTREQUEST -> TODO()
        null, QUERY_NOT_SET -> null
    }

    inline fun <reified T: SubRequest> getSubRequestByType(): T =
            if (subRequest is T) subRequest
            else throw IllegalArgumentException("sub request is not a instance of ${T::class.java}")

    companion object {
        private val atomicId = AtomicLong(0)
        fun newInstance(event: Event, sessionId: Long? = null,
                        subRequest: SubRequest? = null): ProxyRequest {
            val meta = Meta.newBuilder().setRequestId(atomicId.getAndIncrement()).apply {
                sessionId?.let { setSessionId(sessionId) }
            }.build()
            val request =  Request.newBuilder().setMeta(meta).setEvent(event).apply {
                when (subRequest) {
                    is ProxySQLRequest-> sqlRequest = subRequest.toProtoBuf()
                }
            }.build()
            return ProxyRequest(request)
        }
    }

    override fun toProtoBuf() = request
}


sealed class SubRequest: ProxyMessage

class ProxySQLRequest(val statement: ProxyStatement, val sql: String): SubRequest() {
    constructor(stmtId: Int, stmtType: Statement.Type, sql: String)
            : this(ProxyStatement(stmtId, stmtType), sql)

    val stmtId = statement.id

    private val request: SQLRequest by lazy {
        SQLRequest.newBuilder().setStmt(statement.toProtoBuf()).setSql(sql).build()
    }

    override fun toProtoBuf() = request

    companion object {
        fun fromProtoBuf(request: SQLRequest) =
                ProxySQLRequest(ProxyStatement.fromProtoBuf(request.stmt), request.sql)
    }
}

