package sqlproxy.protocol


interface SQLProxyMessage {
    enum class Type(val code: Int) {
        UNKNOWN(0),
        CONNECTION(1),
    }

    val requestId: Long
    val type: Type
}

sealed interface SQLProxyRequest  : SQLProxyMessage

sealed interface SQLProxyResponse: SQLProxyMessage {
    enum class StatusCode(val code: Int) {
        UNUSED(0),
        SUCCESS(200),
        ERROR(500)
    }

    companion object {
        val successStatus = ResponseStatus(StatusCode.SUCCESS, "success")
    }

    data class ResponseStatus ( val code: StatusCode, val description: String )

    val status: ResponseStatus
}

enum class ConnectionCommand(val code: Int) {
    UNKNOWN(0),
    INIT(1),
    CLOSE(2),
    VALID(3),
}

sealed class ConnectionRequest(
    override val requestId: Long,
    val command: ConnectionCommand,
) : SQLProxyRequest {

    override val type = SQLProxyMessage.Type.CONNECTION

    open class InitRequest(requestId: Long): ConnectionRequest(requestId, ConnectionCommand.INIT)

    open class CloseRequest(requestId: Long): ConnectionRequest(requestId, ConnectionCommand.CLOSE)

    open class ValidRequest(
        requestId: Long,
        val timeout: Int
    ): ConnectionRequest(requestId, ConnectionCommand.VALID)
}

sealed class ConnectionResponse(
    override val requestId: Long,
    val command: ConnectionCommand,
    override val status: SQLProxyResponse.ResponseStatus
): SQLProxyResponse {
    override val type = SQLProxyMessage.Type.CONNECTION

    open class InitResponse(requestId: Long,
                            status: SQLProxyResponse.ResponseStatus
    ): ConnectionResponse(requestId, ConnectionCommand.INIT, status)

    open class CloseResponse(requestId: Long,
                            status: SQLProxyResponse.ResponseStatus
    ): ConnectionResponse(requestId, ConnectionCommand.CLOSE, status)

    open class ValidResponse(requestId: Long,
                             val isValid: Boolean,
                             status: SQLProxyResponse.ResponseStatus
    ): ConnectionResponse(requestId, ConnectionCommand.VALID, status)
}
