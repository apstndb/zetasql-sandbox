package zetasql

import com.google.zetasql.Analyzer
import com.google.zetasql.AnalyzerOptions
import com.google.zetasql.LanguageOptions
import com.google.zetasql.SimpleCatalog
import com.google.zetasql.resolvedast.ResolvedNodes

object Main {
    fun f(sql: String): String {
        val analyzerOptions = AnalyzerOptions()
        val languageOptions = LanguageOptions()
        languageOptions.setSupportsAllStatementKinds()
        analyzerOptions.languageOptions = languageOptions
        val catalog = SimpleCatalog("global")
        val resolvedStatement = Analyzer(analyzerOptions, catalog).analyzeStatement(sql)
        println(resolvedStatement.nodeKindString())
        if (resolvedStatement is ResolvedNodes.ResolvedQueryStmt) {
            return resolvedStatement.outputColumnList.map { s -> "${s.name}:${s.column.type}" }.joinToString(separator=",")
        } else if (resolvedStatement is ResolvedNodes.ResolvedCreateTableStmtBase) {
            return resolvedStatement.columnDefinitionList.map { column -> "${column.name}:${column.type}" }.joinToString(separator = ",")
        }
        throw Exception("resolvedStatement is ${resolvedStatement.nodeKindString()}")
    }

    @JvmStatic
    fun main(args: Array<String>) {
        try {
            println(f(args[0]))
        } catch(e: Exception) {
            println(e)
        }
    }
}
