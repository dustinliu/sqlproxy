package sqlproxy.protocol

import com.google.protobuf.GeneratedMessageV3
import sqlproxy.proto.Common.Meta
import sqlproxy.proto.ResponseOuterClass.Response
import sqlproxy.proto.ResponseOuterClass.Status
import sqlproxy.proto.ResponseOuterClass.StmtResponse

class ProxyResponse(meta: Meta,
                    val subResponse: SubResponse?,
                    private val status: Status)
    : PrimaryMessage(meta) {

    fun isSuccess() = status.code == Status.StatusCode.SUCCESS
    fun getStatusMessage(): String? = status.message

    override fun toProtoBuf(): GeneratedMessageV3 {
        var builder = Response.newBuilder().setMeta(meta).setStatus(status)
        builder = when (subResponse) {
            is ProxyStmtResponse-> builder.setStmt(subResponse.toProtoBuf())
            null -> builder
        }
        return builder.build()
    }

    class Builder {
        private val metaBuilder = Meta.newBuilder()
        private var status: Status? = null
        private var subResponse: SubResponse? = null

        fun setRequestId(id: Long) = this.also { metaBuilder.id = id }
        fun setSessionID(id: Long) = this.also { metaBuilder.session = id }
        fun setStatus(st: Status) = this.apply { status = st }
        fun setSubResponse(response: SubResponse) = this.apply { subResponse = response }

        fun build() =
                ProxyResponse(metaBuilder.build(), subResponse,
                        status?:throw IllegalStateException(""))
        }
    }
}

sealed class SubResponse: ProxyMessage

class ProxyStmtResponse(val stmtId: Int): SubResponse() {
    override fun toProtoBuf(): StmtResponse = StmtResponse.newBuilder().setId(stmtId).build()
}

fun makeFailedResponse(requestId: Long, )
