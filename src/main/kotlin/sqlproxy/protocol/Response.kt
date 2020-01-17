package sqlproxy.protocol

import com.google.protobuf.GeneratedMessageV3
import sqlproxy.proto.Common.Meta
import sqlproxy.proto.ResponseOuterClass.Response
import sqlproxy.proto.ResponseOuterClass.Status
import sqlproxy.proto.ResponseOuterClass.StmtResponse

class ProxyResponse<T: SubResponse>(meta: Meta, subRes: T?, private val status: Status)
    : PrimaryMessage(meta, subResponse) {

    fun isSuccess() = status.code == Status.StatusCode.SUCCESS
    fun getStatusMessage(): String? = status.message
    fun getSubResponse() = subMessage

    override fun toProtoBuf(): GeneratedMessageV3 {
        var builder = Response.newBuilder().setMeta(meta).setStatus(status)
        builder = when (subMessage) {
            is ProxyStmtResponse-> builder.setStmt(subMessage.toProtoBuf())
            null -> builder
            else -> builder
        }
        return builder.build()
    }
}

sealed class SubResponse: SubMessage

class ProxyStmtResponse(val stmtId: Int): SubResponse() {
    override fun toProtoBuf(): StmtResponse = StmtResponse.newBuilder().setId(stmtId).build()
}
