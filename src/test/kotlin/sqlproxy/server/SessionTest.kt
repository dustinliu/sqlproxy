package sqlproxy.server

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

internal class SessionTest {

    @BeforeAll
    fun init() {
        initTestDataSource()
    }

    @Test
    fun `test new session`() {
        val session = Session()
        assertNotEquals(0, session.id)
    }
}