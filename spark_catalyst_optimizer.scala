// Spark Catalyst Optimizer: a query optimization engine in Apache Spark
// It combines rule-based and cost-based optimization techniques to generate an efficient execution plan

// Catalyst Optimizer consists of several components:
// 1. Parser: converts SQL query into an abstract syntax tree (AST)
// 2. Analyzer: resolves references and checks for errors in the AST
// 3. Optimizer: applies optimization rules to the AST to generate an optimized plan
// 4. Planner: generates a physical execution plan from the optimized plan

// Example of a more complex Spark query with filtering, grouping, and aggregation
val df = spark.sql("SELECT *, sum(score) as total_score " +
  "FROM mytable " +
  "WHERE category = 'sports' " +  // Filter by category
  "GROUP BY team " +  // Group by team
  "HAVING total_score > 100")  // Filter by total score

// Catalyst Optimizer will generate an optimized execution plan for this query
// The plan will include operations such as filtering, sorting, aggregation, and subqueries
