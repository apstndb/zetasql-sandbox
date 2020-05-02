package dev.apstn.zetasql

import com.google.cloud.bigquery.*
import com.google.cloud.spanner.*
import com.google.zetasql.*
import com.google.zetasql.Type
import com.google.zetasql.resolvedast.ResolvedNodes

object Main {
    private fun extractBigQueryTableSchemaAsSimpleCatalog(sql: String, project: String? = null, dataset: String? = null): SimpleCatalog {
        val simpleTables = extractBigQueryTableSchemaAsSimpleTable(sql, project, dataset)
        return createSimpleCatalog(simpleTables)
    }

    private fun extractSpannerTableSchemaAsSimpleCatalog(sql: String, project: String? = null, instance: String? = null, database: String? = null): SimpleCatalog {
        val simpleTables = extractSpannerTableSchemaAsSimpleTable(sql, project, instance, database)
        return createSimpleCatalog(simpleTables)
    }

    private fun createSimpleCatalog(simpleTables: List<SimpleTable>): SimpleCatalog {
        val catalog = SimpleCatalog("global")
        simpleTables.forEach { simpleTable ->
            addSimpleTableIfAbsent(catalog, simpleTable.name, simpleTable)

            val tableNamePath = simpleTable.fullName.split(".")
            val cat = makeNestedCatalogToParent(catalog, tableNamePath)
            addSimpleTableIfAbsent(cat, tableNamePath.last(), simpleTable)
        }
        return catalog
    }

    private fun addSimpleTableIfAbsent(catalog: SimpleCatalog, name: String, simpleTable: SimpleTable) {
        if (!catalog.tableNameList.contains(name.toLowerCase()))
            catalog.addSimpleTable(name, simpleTable)
    }

    private fun makeNestedCatalogToParent(catalog: SimpleCatalog, tableNamePath: List<String>): SimpleCatalog {
        var cat: SimpleCatalog = catalog
        for (path in tableNamePath.dropLast(1)) {
            cat = cat.catalogList.find { it.fullName == path } ?: cat.addNewSimpleCatalog(path)
        }
        return cat
    }

    private fun extractBigQueryTableSchemaAsSimpleTable(sql: String, project: String? = null, dataset: String? = null): List<SimpleTable> {
        val tableReferences = extractTableImpl(sql, project, dataset)

        return tableReferences.map{tableReference ->
            val projectId = tableReference[0]
            val datasetId = tableReference[1]
            val tableId = tableReference.drop(2).joinToString(".")
            val optionsBuilder = BigQueryOptions.newBuilder()
            val bigquery = optionsBuilder.setProjectId(projectId).build().service
            val table = bigquery.getTable(datasetId, tableId)
            val simpleTable = SimpleTable("${table.tableId.project}.${table.tableId.dataset}.${table.tableId.table}")

            val standardTableDefinition = table.getDefinition<StandardTableDefinition>()
            createBigQueryColumns(simpleTable, standardTableDefinition).forEach { simpleTable.addSimpleColumn(it) }
            createBigQueryPseudoColumns(simpleTable, standardTableDefinition).forEach { simpleTable.addSimpleColumn(it) }

            return@map simpleTable
        }
    }

    private fun extractSpannerTableSchemaAsSimpleTable(sql: String, project: String?, instance: String?, database: String?): List<SimpleTable> {
        val tableReferences = extractTableImpl(sql)

        val options = SpannerOptions.newBuilder()
                .setNumChannels(1)
                .setSessionPoolOption(SessionPoolOptions.newBuilder().setMinSessions(1).setMaxSessions(1).build())
                .build()
        return options.service.use {spanner ->
            val dbClient = spanner.getDatabaseClient(DatabaseId.of(project ?: options.projectId, instance, database))
            tableReferences.map {tableReference ->
                val table = tableReference.joinToString(".")
                dbClient.singleUse().executeQuery(Statement.newBuilder(
                        """SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = '' AND TABLE_NAME = @table ORDER BY ORDINAL_POSITION""")
                        .bind("table").to(table).build()).use { resultSet ->
                    val columns = mutableListOf<SimpleColumn>()
                    while (resultSet.next()) {
                        val row = resultSet
                        val columnName = row.getString("COLUMN_NAME")
                        val spannerType = row.getString("SPANNER_TYPE")
                        columns.add(SimpleColumn(table, columnName, spannerTypeToZetaSQLType(spannerType)))
                    }

                    SimpleTable(table, columns)
                }
            }
        }
    }

    private fun spannerTypeToZetaSQLType(spannerType: String): Type? {
        val findResult = "^ARRAY<([^>]*)>$".toRegex().find(spannerType)
        if (findResult != null) return TypeFactory.createArrayType(spannerTypeToZetaSQLType(findResult.groupValues[1]))
        return TypeFactory.createSimpleType(when {
            spannerType == "INT64" -> ZetaSQLType.TypeKind.TYPE_INT64
            spannerType == "TIMESTAMP" -> ZetaSQLType.TypeKind.TYPE_TIMESTAMP
            spannerType == "DATE" -> ZetaSQLType.TypeKind.TYPE_DATE
            spannerType == "FLOAT64" -> ZetaSQLType.TypeKind.TYPE_DOUBLE
            spannerType == "BOOL" -> ZetaSQLType.TypeKind.TYPE_BOOL
            spannerType.startsWith("STRING") -> ZetaSQLType.TypeKind.TYPE_STRING
            spannerType.startsWith("BYTES") -> ZetaSQLType.TypeKind.TYPE_BYTES
            else -> ZetaSQLType.TypeKind.TYPE_UNKNOWN
        })
    }

    private fun createBigQueryColumns(simpleTable: SimpleTable, standardTableDefinition: StandardTableDefinition): List<SimpleColumn> {
        val fields = standardTableDefinition.schema?.fields ?: return listOf()
        return fields.filterNotNull().map { SimpleColumn(simpleTable.name, it.name, it.toZetaSQLType()) }
    }

    private const val PARTITION_TIME_COLUMN_NAME = "_PARTITIONTIME"
    private const val PARTITION_DATE_COLUMN_NAME = "_PARTITIONDATE"

    private fun createBigQueryPseudoColumns(simpleTable: SimpleTable, standardTableDefinition: StandardTableDefinition): List<SimpleColumn> {
        val timePartitioning = standardTableDefinition.timePartitioning
        if (timePartitioning?.type == null || timePartitioning.field != null) return listOf()
        return mapOf(
                PARTITION_DATE_COLUMN_NAME to ZetaSQLType.TypeKind.TYPE_DATE,
                PARTITION_TIME_COLUMN_NAME to ZetaSQLType.TypeKind.TYPE_TIMESTAMP
        ).map { SimpleColumn(simpleTable.name, it.key, TypeFactory.createSimpleType(it.value), true, false) }
    }

    private fun Field.toZetaSQLType(): Type {
        val type = if (this.subFields == null) {
            TypeFactory.createSimpleType((this.type.standardType.toZetaSQLTypeKind()))
        } else {
            TypeFactory.createStructType(this.subFields.map { StructType.StructField(it.name, it.toZetaSQLType()) })
        }
        return when (this.mode) {
            Field.Mode.REPEATED -> TypeFactory.createArrayType(type)
            Field.Mode.NULLABLE, Field.Mode.REQUIRED -> type
            null -> type
        }
    }

    private fun StandardSQLTypeName.toZetaSQLTypeKind(): ZetaSQLType.TypeKind {
        return when (this) {
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

    private fun extractTableImpl(sql: String, project: String? = null, dataset: String? = null): List<List<String>> {
        return Analyzer.extractTableNamesFromStatement(sql).map { tableNameList -> tableNameList.flatMap{ it.split(".")}}.map {
            when (it.size) {
                1 -> arrayListOf(project, dataset, it[0]).filterNotNull()
                2 -> arrayListOf(project, it[0], it[1]).filterNotNull()
                else -> it
            }
        }
    }

    private fun format(sql: String): String = SqlFormatter().formatSql(sql)

    fun analyze(sql: String): String {
        val catalog = SimpleCatalog("global")

        val analyzerOptions = defaultAnalyzerOptions()
        val zetaSQLBuiltinFunctionOptions = ZetaSQLBuiltinFunctionOptions()
        catalog.addZetaSQLFunctions(zetaSQLBuiltinFunctionOptions)
        val analyzer = Analyzer(analyzerOptions, catalog)
        val resolvedStatements = analyzeSqlStatements(analyzer, sql)
        return resolvedStatements.joinToString("\n") { toString(it) }
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

    private fun analyzePrint(sql: String): String {
        val catalog = SimpleCatalog("global")
        val resolvedStatements = analyzePrintImpl(sql, catalog)
        return SqlFormatter().formatSql(resolvedStatements.joinToString("\n") { "${Analyzer.buildStatement(it,catalog)};"})
    }

    private fun analyzePrintWithBQSchema(sql: String): String {
        val catalog = extractBigQueryTableSchemaAsSimpleCatalog(sql)
        val resolvedStatements = analyzePrintImpl(sql, catalog)
        return SqlFormatter().formatSql(resolvedStatements.joinToString("\n") { "${Analyzer.buildStatement(it,catalog)};"})
    }

    private fun analyzePrintWithSpannerSchema(sql: String, project: String, instance: String, database: String): Any? {
        val catalog = extractSpannerTableSchemaAsSimpleCatalog(sql, project, instance, database)
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
                "analyze-print-with-spannerschema" -> analyzePrintWithSpannerSchema(input, args[1], args[2], args[3])
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
