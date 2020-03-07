package zetasql

import org.junit.Test
import kotlin.test.assertEquals

class MainTest {
    @Test
    fun testF() {
        val s = Main.analyze("SELECT * FROM (SELECT 1 AS x)")
        assertEquals("x:INT64", s)
    }

    @Test
    fun testExtractTable() {
        val s = Main.extractTable("SELECT * FROM `project.dataset.table`")
        assertEquals("project.dataset.table", s)
    }
}