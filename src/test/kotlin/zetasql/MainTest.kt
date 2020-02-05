package zetasql

import org.junit.Test
import zetasql.Main
import kotlin.test.assertEquals

class MainTest {
    @Test
    fun testF() {
        val s = Main.f("SELECT * FROM (SELECT 1 AS x)")
        assertEquals("x:INT64", s)
        assertEquals("x:STRING", s)
    }
}