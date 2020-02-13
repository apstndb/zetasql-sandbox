package zetasql

import com.google.zetasql.*
import com.google.zetasql.resolvedast.ResolvedNodes

object Main {
    fun format(sql: String): String {
        return SqlFormatter().formatSql(sql);
    }

    fun f(sql: String): String {
        val analyzerOptions = AnalyzerOptions()
        val languageOptions = LanguageOptions()
        languageOptions.setSupportsAllStatementKinds()
        analyzerOptions.languageOptions = languageOptions
        val catalog = SimpleCatalog("global")
        val analyzer = Analyzer(analyzerOptions, catalog)
        val parseResumeLocation = ParseResumeLocation(sql)
        val resolvedStatements = ArrayList<ResolvedNodes.ResolvedStatement>()
        // TODO: justify loop condition
        while (parseResumeLocation.bytePosition != sql.length) {
            resolvedStatements.add(analyzer.analyzeNextStatement(parseResumeLocation))
        }
        return resolvedStatements.joinToString("\n") { resolvedStmt -> toString(resolvedStmt)}
    }

    private fun toString(resolvedStatement: ResolvedNodes.ResolvedStatement): String {
        println(resolvedStatement.nodeKindString())
        when (resolvedStatement) {
            is ResolvedNodes.ResolvedQueryStmt ->
                return resolvedStatement.outputColumnList.joinToString(",") { column -> "${column.name}:${column.column.type}" }
            is ResolvedNodes.ResolvedCreateTableStmtBase ->
                return resolvedStatement.columnDefinitionList.joinToString(",") { columnDef -> "${columnDef.name}:${columnDef.type}" }
            is ResolvedNodes.ResolvedCreateIndexStmt -> {
            }
        }
        throw Exception("resolvedStatement is ${resolvedStatement.nodeKindString()}")
    }

    @JvmStatic
    fun main(args: Array<String>) {
        try {
            val input = generateSequence(::readLine).joinToString("\n")
            val str = when (args[0]) {
                "format" -> format(input)
                "parse" -> f(input)
                else -> throw Exception("unknown:" + args[0])
            }
            println(str)
        } catch(e: Exception) {
            println(e)
        }
    }
}
