import com.google.zetasql.Analyzer
import com.google.zetasql.AnalyzerOptions
import com.google.zetasql.SimpleCatalog
import com.google.zetasql.resolvedast.ResolvedNode
import com.google.zetasql.resolvedast.ResolvedNodes
import com.google.zetasql.resolvedast.ResolvedNodes.Visitor

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val analyzerOptions = AnalyzerOptions()
        val catalog = SimpleCatalog("global")
        val resolvedStatement = Analyzer(analyzerOptions, catalog).analyzeStatement(args[0])
        println(resolvedStatement.nodeKindString())
        if (resolvedStatement !is ResolvedNodes.ResolvedQueryStmt) {
            println("resolvedStatement is ${resolvedStatement.nodeKindString()}")
            return
        }
        println(resolvedStatement.outputColumnList.map { s -> "${s.name}:${s.column.type}" }.forEach(System.out::println))
        /*
        resolvedStatement.accept(object: Visitor() {
            override fun visit(node: ResolvedNodes.ResolvedQueryStmt) {
                node.outputColumnList
            }
        })
         */
    }
}
