package sqlproxy.server.service

import mu.KotlinLogging
import sqlproxy.protocol.SQLProxyResponse

private val logger = KotlinLogging.logger {}

class SQLProxyService {
    private val session: Session = Session()

    fun invoke(msg: ServerRequestService) = msg.process(session)
    fun finish() = session.close()
    fun isActive() = session.isActive()
}

interface  ServerRequestService {
//    fun process(session: Session): SQLProxyResponse = try {
//        process0(session)
//    } catch (e: SQLException) {
//        throw e
//    } catch (e: Throwable) {
//       throw e
//    }

    fun process(session: Session): SQLProxyResponse
}
