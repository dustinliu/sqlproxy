package sqlproxy.protocol

import com.google.protobuf.GeneratedMessageV3
import sqlproxy.proto.Common.Meta
import sqlproxy.proto.Common.Statement
import sqlproxy.proto.ResponseOuterClass.Response

interface ProxyMessage {
    fun toProtoBuf(): GeneratedMessageV3
}

interface PrimaryMessage: ProxyMessage {
    val requestId: Long
    val sessionId: Long
}

class ProxyStatement(val id: Int, val type: Statement.Type): ProxyMessage {
    private val statement: Statement by lazy {
       Statement.newBuilder().setId(id).setType(type).build()
    }

    override fun toProtoBuf(): Statement = statement

    companion object {
        fun fromProtoBuf(statement: Statement) = ProxyStatement(statement.id, statement.type)
    }
}

class ProxyMetaMessage(override val requestId: Long, override val sessionId: Long): PrimaryMessage {
    constructor(meta: Meta) : this(meta.requestId, meta.sessionId)

    private val meta by lazy {
        Meta.newBuilder().setRequestId(requestId).setSessionId(sessionId).build()
    }
    override fun toProtoBuf(): Meta = meta
}
