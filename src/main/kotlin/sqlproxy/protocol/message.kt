package sqlproxy.protocol


interface SQLProxyMessage {
    val requestId: Long
}

sealed interface SQLProxyRequest: SQLProxyMessage

sealed interface SQLProxyResponse: SQLProxyMessage {
    companion object {
        val successStatus = Status.Success("success")
    }

    sealed class Status (val description: String ) {
        class Success (desc: String): Status(desc)
        class Failure(desc: String): Status(desc)
    }

    val status: Status
}

sealed class ConnectionRequest(
    override val requestId: Long,
) : SQLProxyRequest {
    open class InitRequest(requestId: Long): ConnectionRequest(requestId)
    open class CloseRequest(requestId: Long): ConnectionRequest(requestId)
    open class ValidRequest( requestId: Long, val timeout: Int ): ConnectionRequest(requestId)
}

sealed class ConnectionResponse(
    override val requestId: Long,
    override val status: SQLProxyResponse.Status
): SQLProxyResponse {
    open class InitResponse(requestId: Long,
                            status: SQLProxyResponse.Status
    ): ConnectionResponse(requestId, status)

    open class CloseResponse(requestId: Long,
                            status: SQLProxyResponse.Status
    ): ConnectionResponse(requestId, status)

    open class ValidResponse(requestId: Long,
                             val isValid: Boolean,
                             status: SQLProxyResponse.Status
    ): ConnectionResponse(requestId, status)
}
