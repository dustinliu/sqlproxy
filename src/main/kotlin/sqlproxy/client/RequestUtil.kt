package sqlproxy.client

import sqlproxy.proto.Common
import sqlproxy.proto.RequestOuterClass.Request
import java.util.*

fun newRequestBuilder(event: Request.Event, sessionId: String?=null, stmtId: String?=null): Request.Builder {
    val meta = Common.Meta.newBuilder().setId(UUID.randomUUID().toString()).also {
        sessionId?.run { it.setSession(sessionId) }
    }.also {
        stmtId?.run { it.setStmt(stmtId)}
    }.build()

    return Request.newBuilder().setMeta(meta).setEvent(event)
}
