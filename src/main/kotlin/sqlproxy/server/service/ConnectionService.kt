package sqlproxy.server.service

import mu.KotlinLogging
import sqlproxy.protocol.ConnectionRequest
import sqlproxy.protocol.ConnectionResponse
import sqlproxy.protocol.SQLProxyResponse

private val logger = KotlinLogging.logger {}

class ConnectionInitService(
    msg: ConnectionRequest,

) : ConnectionRequest.InitRequest(msg.requestId), ServerRequestService {
    override fun process(session: Session): ConnectionResponse.InitResponse {
        return ConnectionResponse.InitResponse(this.requestId, SQLProxyResponse.successStatus)
    }
}

class ConnectionCloseService(
    msg: ConnectionRequest,
) : ConnectionRequest.CloseRequest(msg.requestId), ServerRequestService {
    override fun process(session: Session): ConnectionResponse.CloseResponse {
        session.close()
        return ConnectionResponse.CloseResponse(this.requestId, SQLProxyResponse.successStatus)
    }
}

class ConnectionValidService(
    msg: ConnectionRequest,
    timeout: Int
) : ConnectionRequest.ValidRequest(msg.requestId, timeout), ServerRequestService {
    override fun process(session: Session): ConnectionResponse.ValidResponse {
        val r = session.isActive() && session.connection.isValid(timeout)
        return ConnectionResponse.ValidResponse(this.requestId, true,
            SQLProxyResponse.successStatus)
    }
}
