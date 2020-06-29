package dev.apstn.zetasql

import com.google.cloud.bigquery.*
import com.google.cloud.spanner.*
import com.google.protobuf.util.JsonFormat
import com.google.cloud.spanner.Type as SpannerType
import com.google.zetasql.*
import com.google.zetasql.Type
import com.google.zetasql.resolvedast.ResolvedNodes
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement
import java.util.*

object Main {
    private fun extractBigQueryTableSchemaAsSimpleCatalog(sql: String, project: String? = null, dataset: String? = null) =
            createSimpleCatalogForTables(extractBigQueryTableSchemaAsSimpleTable(sql, project, dataset))

    private fun extractSpannerTableSchemaAsSimpleCatalog(sql: String, project: String? = null, instance: String? = null, database: String? = null) =
            createSimpleCatalogForTables(extractSpannerTableSchemaAsSimpleTable(sql, project, instance, database))

    private fun createSimpleCatalogForTables(simpleTables: Map<String, SimpleTable>) =
            createSimpleCatalogContainsBuiltinFunctions().also {
                simpleTables.forEach { (tableFullName, simpleTable) ->
                    val tableNamePath = tableFullName.split(".")
                    addSimpleTableIfAbsent(it, tableFullName, simpleTable)

                    val cat = makeNestedCatalogToParent(it, tableNamePath)
                    addSimpleTableIfAbsent(cat, tableNamePath.last(), simpleTable)
                }
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

    private fun extractBigQueryTableSchemaAsSimpleTable(sql: String, project: String? = null, dataset: String? = null) =
            extractTableImpl(sql, project, dataset).associateTo(HashMap<String, SimpleTable>()){ tableReference ->
                val projectId = tableReference[0]
                val datasetId = tableReference[1]
                val tableId = tableReference.drop(2).joinToString(".")
                val optionsBuilder = BigQueryOptions.newBuilder()
                val bigquery = optionsBuilder.setProjectId(projectId).build().service
                val table = bigquery.getTable(datasetId, tableId)
                val tableFullName = "${table.tableId.project}.${table.tableId.dataset}.${table.tableId.table}"
                val simpleTable = SimpleTable(tableFullName)

                val standardTableDefinition = table.getDefinition<StandardTableDefinition>()
                createBigQueryColumns(tableFullName, standardTableDefinition).forEach { simpleTable.addSimpleColumn(it) }
                createBigQueryPseudoColumns(tableFullName, standardTableDefinition).forEach { simpleTable.addSimpleColumn(it) }

                tableFullName to simpleTable
            }

    private val spannerTablePathType =
            SpannerType.struct(
                    listOf(
                            SpannerType.StructField.of("TABLE_SCHEMA", SpannerType.string()),
                            SpannerType.StructField.of("TABLE_NAME", SpannerType.string())))

    private fun extractSpannerTableSchemaAsSimpleTable(sql: String, project: String?, instance: String?, database: String?) =
            defaultSpannerOptions().let { options ->
                val statement = Statement.newBuilder("""
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SPANNER_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = ''
      AND STRUCT<TABLE_SCHEMA STRING, TABLE_NAME STRING>(LOWER(TABLE_SCHEMA), LOWER(TABLE_NAME)) IN (SELECT AS STRUCT LOWER(TABLE_SCHEMA) AS TABLE_SCHEMA, LOWER(TABLE_NAME) AS TABLE_NAME FROM UNNEST(@table_path))
    ORDER BY ORDINAL_POSITION"""
                )
                        .bind("table_path").toStructArray(spannerTablePathType, extractTablePathAsStructs(sql))
                        .build()
                options.service.use { spanner ->
                    // TODO: back-quoted identifier should be treated as case-sensitive but it is not supportedby Analyzer.extractTableNamesFromStatement.
                    spanner.getDatabaseClient(DatabaseId.of(project ?: options.projectId, instance, database))
                            .singleUse()
                            .executeQuery(statement)
                            .use {
                                sequence {
                                    while (it.next())
                                        yield(it.currentRowAsStruct)
                                }.toList()
                            }.groupingBy {
                                val tableSchema = it.getString("TABLE_SCHEMA")
                                val tableName = it.getString("TABLE_NAME")
                                when {
                                    tableSchema.isEmpty() -> tableName
                                    else -> "${tableSchema}.${tableName}"
                                }
                            }.aggregateTo(TreeMap<String, SimpleTable>()) { tableFullPath, simpleTable, rs, _ ->
                                (simpleTable ?: SimpleTable(tableFullPath)).apply {
                                    addSimpleColumn(rs.getString("COLUMN_NAME"), spannerTypeToZetaSQLType(rs.getString("SPANNER_TYPE")))
                                }
                            }
                }
            }

    private fun extractTablePathAsStructs(sql: String) =
            extractTableImpl(sql).map { tableReference ->
                Struct.newBuilder()
                        .set("TABLE_SCHEMA").to(if (tableReference.size > 1) tableReference.first() else "")
                        .set("TABLE_NAME").to(tableReference.last())
                        .build()
            }

    private fun defaultSpannerOptions() =
            SpannerOptions.newBuilder()
                    .setNumChannels(1)
                    .setSessionPoolOption(SessionPoolOptions.newBuilder().setMinSessions(1).setMaxSessions(1).build())
                    .build()

    private fun spannerTypeToZetaSQLType(spannerType: String): Type =
            when (val findResult = """^ARRAY<([^>]*)>$""".toRegex().find(spannerType)) {
                is MatchResult -> TypeFactory.createArrayType(spannerTypeToZetaSQLType(findResult.groupValues[1]))
                else ->
                    TypeFactory.createSimpleType(when (spannerType) {
                        "FLOAT64" -> ZetaSQLType.TypeKind.TYPE_DOUBLE
                        else -> {
                            val typeName = """^([A-Z0-9]+)""".toRegex().find(spannerType)!!.groupValues[1]
                            ZetaSQLType.TypeKind.valueOf("TYPE_$typeName")
                        }})}

    private fun createBigQueryColumns(tableName: String, standardTableDefinition: StandardTableDefinition) =
            (standardTableDefinition.schema?.fields ?: listOf<Field>()).map { SimpleColumn(tableName, it.name, it.toZetaSQLType()) }

    private val BigQueryPartitionedTablePseudoColumns = mapOf(
            "_PARTITIONDATE" to ZetaSQLType.TypeKind.TYPE_DATE,
            "_PARTITIONTIME" to ZetaSQLType.TypeKind.TYPE_TIMESTAMP
    )

    private fun createBigQueryPseudoColumns(tableName: String, standardTableDefinition: StandardTableDefinition): List<SimpleColumn> {
        val timePartitioning = standardTableDefinition.timePartitioning
        if (timePartitioning?.type == null || timePartitioning.field != null) return listOf()
        return BigQueryPartitionedTablePseudoColumns.map { SimpleColumn(tableName, it.key, TypeFactory.createSimpleType(it.value), true, false) }
    }

    private fun Field.toZetaSQLType(): Type =
            when (this.subFields) {
                null -> TypeFactory.createSimpleType((this.type.standardType.toZetaSQLTypeKind()))
                else -> TypeFactory.createStructType(this.subFields.map { StructType.StructField(it.name, it.toZetaSQLType()) })
            }.let { type ->
                when (this.mode) {
                    Field.Mode.REPEATED -> TypeFactory.createArrayType(type)
                    Field.Mode.NULLABLE, Field.Mode.REQUIRED -> type
                    null -> type
                }
            }

    private fun StandardSQLTypeName.toZetaSQLTypeKind() =
            when (this) {
                StandardSQLTypeName.FLOAT64 -> ZetaSQLType.TypeKind.TYPE_DOUBLE
                else -> ZetaSQLType.TypeKind.valueOf("TYPE_${name}")
            }

    fun extractTable(sql: String, project: String? = null, dataset: String? = null) =
            extractTableImpl(sql, project, dataset).joinToString("\n") { it.joinToString(".") }

    private fun extractTableImpl(sql: String, project: String? = null, dataset: String? = null) =
            Analyzer.extractTableNamesFromStatement(sql).map { tableNameList -> tableNameList.flatMap{ it.split(".")}}.map {
                when (it.size) {
                    1 -> arrayListOf(project, dataset, it[0]).filterNotNull()
                    2 -> arrayListOf(project, it[0], it[1]).filterNotNull()
                    else -> it
                }
            }

    private fun format(sql: String) = SqlFormatter().formatSql(sql)

    fun analyze(sql: String) =
            analyzeSqlStatements(Analyzer(defaultAnalyzerOptions(), createSimpleCatalogContainsBuiltinFunctions()), sql).joinToString("\n") { toString(it) }

    private fun createSimpleCatalogContainsBuiltinFunctions() =
            SimpleCatalog("global").apply {
                addZetaSQLFunctions(ZetaSQLBuiltinFunctionOptions())
            }

    private fun defaultAnalyzerOptions() =
            AnalyzerOptions().apply {
                pruneUnusedColumns = true
                // TODO: Support positional query parameters
                allowUndeclaredParameters = true
                languageOptions = LanguageOptions().apply {
                    enableLanguageFeature(ZetaSQLOptions.LanguageFeature.FEATURE_V_1_3_ALLOW_DASHES_IN_TABLE_NAME)
                    setSupportsAllStatementKinds()
                    enableMaximumLanguageFeatures()
                }
            }

    private fun analyzeSqlStatements(analyzer: Analyzer, sql: String) =
            sequence {
                val parseResumeLocation = ParseResumeLocation(sql)
                val byteLength = sql.toByteArray().size
                // TODO: justify loop condition
                while (parseResumeLocation.bytePosition != byteLength) {
                    yield(analyzer.analyzeNextStatement(parseResumeLocation))
                }
            }.toList()

    private fun analyzePrint(sql: String) =
            analyzePrintForCatalog(sql, createSimpleCatalogContainsBuiltinFunctions())

    private fun analyzePrintJSON(sql: String) =
            analyzePrintJSONForCatalog(sql, createSimpleCatalogContainsBuiltinFunctions())

    private fun analyzePrintJSONWithBQSchema(sql: String) =
            analyzePrintJSONForCatalog(sql, extractBigQueryTableSchemaAsSimpleCatalog(sql))

    private fun analyzePrintJSONWithSpannerSchema(sql: String, project: String, instance: String, database: String) =
            analyzePrintJSONForCatalog(sql, extractSpannerTableSchemaAsSimpleCatalog(sql, project, instance, database))

    private fun analyzePrintWithBQSchema(sql: String) =
            analyzePrintForCatalog(sql, extractBigQueryTableSchemaAsSimpleCatalog(sql))

    private fun analyzePrintWithSpannerSchema(sql: String, project: String, instance: String, database: String) =
            analyzePrintForCatalog(sql, extractSpannerTableSchemaAsSimpleCatalog(sql, project, instance, database))

    private fun analyzePrintForCatalog(sql: String, catalog: SimpleCatalog) =
            SqlFormatter().formatSql(analyzeForCatalog(sql, catalog).joinToString("\n") { "${Analyzer.buildStatement(it, catalog)};" })

    private fun analyzeForCatalog(sql: String, catalog: SimpleCatalog) =
            analyzeSqlStatements(Analyzer(defaultAnalyzerOptions(), catalog), sql)

    private fun analyzePrintJSONForCatalog(sql: String, catalog: SimpleCatalog) =
            analyzeForCatalog(sql, catalog).map {val builder = AnyResolvedStatementProto.newBuilder(); it.serialize(FileDescriptorSetsBuilder(), builder); JsonFormat.printer().print(builder.build())}.joinToString("\n")
    // SqlFormatter().formatSql(analyzeForCatalog(sql, catalog).joinToString("\n") { "${Analyzer.buildStatement(it, catalog)};" })

    private fun toString(resolvedStatement: ResolvedStatement) = when (resolvedStatement) {
        is ResolvedNodes.ResolvedQueryStmt ->
            resolvedStatement.outputColumnList.joinToString(",") { "${it.name}:${it.column.type}" }
        is ResolvedNodes.ResolvedCreateTableStmtBase ->
            resolvedStatement.columnDefinitionList.joinToString(",") { "${it.name}:${it.type}" }
        else ->
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
                "analyze-print-json" -> analyzePrintJSON(input)
                "analyze-print-json-with-bqschema" -> analyzePrintJSONWithBQSchema(input)
                "analyze-print-json-with-spannerschema" -> analyzePrintJSONWithSpannerSchema(input, args[1], args[2], args[3])
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
