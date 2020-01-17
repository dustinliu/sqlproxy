package sqlproxy.server

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.NoSuchElementException

internal class SessionFactoryTest {
    @Test
    fun `get nonexisting session`() {
        assertThrows(NoSuchElementException::class.java)
            {SessionFactory.getSession(343)}
    }
}
