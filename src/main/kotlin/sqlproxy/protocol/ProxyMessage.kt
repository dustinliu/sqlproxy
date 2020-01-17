package sqlproxy.protocol

import com.google.protobuf.GeneratedMessageV3
import sqlproxy.proto.Common.Meta

interface ProxyMessage {
    fun toProtoBuf(): GeneratedMessageV3
}

abstract class PrimaryMessage(protected val meta: Meta): ProxyMessage {
    val requestId = meta.id
    val sessionId = meta.session
}


inline fun <reified T: SubRequest> castOrNull(message: ProxyMessage?) : T? {
    return if (message != null && message is T) message
    else null
}
