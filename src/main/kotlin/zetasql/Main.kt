package zetasql

import com.google.cloud.bigquery.*
import com.google.zetasql.*
import com.google.zetasql.resolvedast.ResolvedNodes

object Main {
    fun extractTableSchemaAsSimpleCatalog(sql: String, project: String? = null, dataset: String? = null): SimpleCatalog {
        val simpleTables = extractTableSchemaAsSimpleTable(sql, project, dataset)
        val catalog = SimpleCatalog("global")
        simpleTables.forEach { simpleTable ->
            if (catalog.tableNameList.none { it == simpleTable.name })
                catalog.addSimpleTable(simpleTable)
            var cat: SimpleCatalog = catalog
            val tableNamePath = simpleTable.fullName.split(".")
            val tableName = tableNamePath.last()
            for (path in tableNamePath.dropLast(1)) {
                cat = cat.catalogList.find { it -> it.fullName == path } ?: cat.addNewSimpleCatalog(path)
            }
            cat.addSimpleTable(tableName, simpleTable)
        }
        return catalog
    }

    fun extractTableSchemaAsSimpleTable(sql: String, project: String? = null, dataset: String? = null): List<SimpleTable> {
        val tableReferences = extractTableImpl(sql, project, dataset)

        return tableReferences.map{
            val optionsBuilder = BigQueryOptions.newBuilder();
            val bigquery = optionsBuilder.setProjectId(it[0]).build().service
            val table = bigquery.getTable(it[1], it[2])
            val simpleTable = SimpleTable("${table.tableId.project}.${table.tableId.dataset}.${table.tableId.table}")
            table.getDefinition<TableDefinition>().schema?.fields?.forEach{ field ->
                simpleTable.addSimpleColumn(field.name, toZetaSQLType(field))
            }
            return@map simpleTable
        }
    }

    private fun toZetaSQLType(field: Field): Type {
        val type = if (field.subFields == null) {
            TypeFactory.createSimpleType(toZetaSQLTypeKind(field.type.standardType))
        } else {
            TypeFactory.createStructType(field.subFields.map { StructType.StructField(it.name, toZetaSQLType(it)) })
        }
        return when (field.mode!!) {
            Field.Mode.REPEATED -> TypeFactory.createArrayType(type)
            Field.Mode.NULLABLE, Field.Mode.REQUIRED -> type
        }
    }

    private fun toZetaSQLTypeKind(standardType: StandardSQLTypeName): ZetaSQLType.TypeKind {
        return when (standardType) {
            StandardSQLTypeName.INT64 -> ZetaSQLType.TypeKind.TYPE_INT64
            StandardSQLTypeName.BOOL -> ZetaSQLType.TypeKind.TYPE_BOOL
            StandardSQLTypeName.FLOAT64 -> ZetaSQLType.TypeKind.TYPE_DOUBLE
            StandardSQLTypeName.NUMERIC -> ZetaSQLType.TypeKind.TYPE_NUMERIC
            StandardSQLTypeName.STRING -> ZetaSQLType.TypeKind.TYPE_STRING
            StandardSQLTypeName.BYTES -> ZetaSQLType.TypeKind.TYPE_BYTES
            StandardSQLTypeName.STRUCT -> ZetaSQLType.TypeKind.TYPE_STRUCT
            StandardSQLTypeName.ARRAY -> ZetaSQLType.TypeKind.TYPE_ARRAY
            StandardSQLTypeName.TIMESTAMP -> ZetaSQLType.TypeKind.TYPE_TIMESTAMP
            StandardSQLTypeName.DATE -> ZetaSQLType.TypeKind.TYPE_DATE
            StandardSQLTypeName.TIME -> ZetaSQLType.TypeKind.TYPE_TIME
            StandardSQLTypeName.DATETIME -> ZetaSQLType.TypeKind.TYPE_DATETIME
            StandardSQLTypeName.GEOGRAPHY -> ZetaSQLType.TypeKind.TYPE_GEOGRAPHY
        }
    }

    fun extractTable(sql: String, project: String? = null, dataset: String? = null): String =
            extractTableImpl(sql, project, dataset).joinToString("\n") { it.joinToString(".") }

    private fun extractTableImpl(sql: String, project: String?, dataset: String?): List<List<String>> {
        val list: List<List<String>> = Analyzer.extractTableNamesFromStatement(sql).map{ tableNameList -> tableNameList.flatMap{ it.split(".")}}.map {
            when (it.size) {
                1 -> arrayListOf(project, dataset, it[0]).filterNotNull()
                2 -> arrayListOf(project, it[0], it[1]).filterNotNull()
                else -> it
            }
        }
        return list
    }

    fun format(sql: String): String = SqlFormatter().formatSql(sql)

    fun analyze(sql: String): String {
        val catalog = SimpleCatalog("global")

        val analyzerOptions = defaultAnalyzerOptions()
        val zetaSQLBuiltinFunctionOptions = ZetaSQLBuiltinFunctionOptions()
        catalog.addZetaSQLFunctions(zetaSQLBuiltinFunctionOptions)
        val analyzer = Analyzer(analyzerOptions, catalog)
        val resolvedStatements = analyzeSqlStatements(analyzer, sql)
        return resolvedStatements.joinToString("\n") { toString(it)}
    }

    private fun defaultAnalyzerOptions(): AnalyzerOptions {
        val analyzerOptions = AnalyzerOptions()
        analyzerOptions.pruneUnusedColumns = true
        analyzerOptions.languageOptions = with(LanguageOptions()) {
            enableLanguageFeature(ZetaSQLOptions.LanguageFeature.FEATURE_V_1_3_ALLOW_DASHES_IN_TABLE_NAME)
            setSupportsAllStatementKinds()
            enableMaximumLanguageFeatures()
        }
        return analyzerOptions
    }

    private fun analyzeSqlStatements(analyzer: Analyzer, sql: String): List<ResolvedNodes.ResolvedStatement> {
        val parseResumeLocation = ParseResumeLocation(sql)
        val resolvedStatements = ArrayList<ResolvedNodes.ResolvedStatement>()
        // TODO: justify loop condition
        while (parseResumeLocation.bytePosition != sql.length) {
            resolvedStatements.add(analyzer.analyzeNextStatement(parseResumeLocation))
        }
        return resolvedStatements
    }

    fun analyzePrint(sql: String): String {
        val catalog = SimpleCatalog("global")
        val resolvedStatements = analyzePrintImpl(sql, catalog)
        return SqlFormatter().formatSql(resolvedStatements.joinToString("\n") { "${Analyzer.buildStatement(it,catalog)};"})
    }

    fun analyzePrintWithBQSchema(sql: String): String {
        val catalog = extractTableSchemaAsSimpleCatalog(sql)
        val resolvedStatements = analyzePrintImpl(sql, catalog)
        return SqlFormatter().formatSql(resolvedStatements.joinToString("\n") { "${Analyzer.buildStatement(it,catalog)};"})
    }

    private fun analyzePrintImpl(sql: String, catalog: SimpleCatalog): List<ResolvedNodes.ResolvedStatement> {
        val analyzerOptions = defaultAnalyzerOptions()
        val zetaSQLBuiltinFunctionOptions = ZetaSQLBuiltinFunctionOptions()
        catalog.addZetaSQLFunctions(zetaSQLBuiltinFunctionOptions)
        val analyzer = Analyzer(analyzerOptions, catalog)
        return analyzeSqlStatements(analyzer, sql)
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
                "analyze-print" -> analyzePrint(input)
                "analyze-print-with-bqschema" -> analyzePrintWithBQSchema(input)
                "extract-table" -> extractTable(input)
                else -> throw Exception("unknown command:" + args[0])
            }
            println(str)
        } catch(e: Exception) {
            println(e)
            e.printStackTrace()
        }
    }
}
