package sqlproxy.client

import sqlproxy.proto.Common
import sqlproxy.proto.Common.Statement
import sqlproxy.proto.RequestOuterClass.Request
import sqlproxy.proto.RequestOuterClass.SQLRequest
import java.util.UUID

fun newRequestBuilder(event: Request.Event, sessionId: Long?=null): Request.Builder {
    val meta = Common.Meta.newBuilder().setId(UUID.randomUUID().toString()).also {
        sessionId?.run { it.setSession(sessionId) }
    }.build()

    return Request.newBuilder().setMeta(meta).setEvent(event)
}

fun newSQLRequest(stmtId: Int, sql: String) {
    val stmt = Statement.newBuilder().setId(stmtId).build()
    val sqlRequest = SQLRequest.newBuilder().setStmt(stmt).setSql(sql).build()
}
