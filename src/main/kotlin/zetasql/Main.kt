package zetasql

import com.google.zetasql.Analyzer
import com.google.zetasql.AnalyzerOptions
import com.google.zetasql.SimpleCatalog
import com.google.zetasql.resolvedast.ResolvedNodes

object Main {
    fun f(sql: String): String {
        val analyzerOptions = AnalyzerOptions()
        val catalog = SimpleCatalog("global")
        val resolvedStatement = Analyzer(analyzerOptions, catalog).analyzeStatement(sql)
        println(resolvedStatement.nodeKindString())
        if (resolvedStatement !is ResolvedNodes.ResolvedQueryStmt) {
            throw Exception("resolvedStatement is ${resolvedStatement.nodeKindString()}")
        }
        return resolvedStatement.outputColumnList.map { s -> "${s.name}:${s.column.type}" }.joinToString(separator=",")
    }

    @JvmStatic
    fun main(args: Array<String>) {
        try {
            println(f(args[0]))
        } catch(e: Exception) {
            println(e)
        }
        /*
        resolvedStatement.accept(object: Visitor() {
            override fun visit(node: ResolvedNodes.ResolvedQueryStmt) {
                node.outputColumnList
            }
        })
         */
    }
}
