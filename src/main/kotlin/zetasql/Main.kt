package zetasql

import com.google.zetasql.*
import com.google.zetasql.resolvedast.ResolvedNodes

object Main {
    fun extractTable(sql: String): String =
            Analyzer.extractTableNamesFromStatement(sql).joinToString("\n") { it.joinToString(".") }

    fun format(sql: String): String = SqlFormatter().formatSql(sql)

    fun analyze(sql: String): String {
        val languageOptions = LanguageOptions()
        languageOptions.setSupportsAllStatementKinds()
        languageOptions.enableMaximumLanguageFeatures()
        val analyzerOptions = AnalyzerOptions()
        analyzerOptions.languageOptions = languageOptions
        val catalog = SimpleCatalog("global")
        val analyzer = Analyzer(analyzerOptions, catalog)
        val parseResumeLocation = ParseResumeLocation(sql)
        val resolvedStatements = ArrayList<ResolvedNodes.ResolvedStatement>()
        // TODO: justify loop condition
        while (parseResumeLocation.bytePosition != sql.length) {
            resolvedStatements.add(analyzer.analyzeNextStatement(parseResumeLocation))
        }
        return resolvedStatements.joinToString("\n") { toString(it)}
    }

    private fun toString(resolvedStatement: ResolvedNodes.ResolvedStatement): String {
        println(resolvedStatement.nodeKindString())
        when (resolvedStatement) {
            is ResolvedNodes.ResolvedQueryStmt ->
                return resolvedStatement.outputColumnList.joinToString(",") { "${it.name}:${it.column.type}" }
            is ResolvedNodes.ResolvedCreateTableStmtBase ->
                return resolvedStatement.columnDefinitionList.joinToString(",") { "${it.name}:${it.type}" }
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
                "analyze" -> analyze(input)
                "extract-table" -> extractTable(input)
                else -> throw Exception("unknown command:" + args[0])
            }
            println(str)
        } catch(e: Exception) {
            println(e)
        }
    }
}
