
cd /Users/Tim/Documents/dev/kafka-spark/spark-2.0.0-bin-hadoop2.7
./bin/spark-shell



https://spark.apache.org/docs/latest/quick-start.html

val textFile = sc.textFile("/home/timvl/Documenten/Overview.html")



scala> val textFile = sc.textFile("README.md")

textFile: org.apache.spark.rdd.RDD[String] = README.md MapPartitionsRDD[1] at textFile at <console>:25
RDDs have actions, which return values, and transformations, which return pointers to new RDDs. Let’s start with a few actions:

scala> textFile.count() // Number of items in this RDD
res0: Long = 126

scala> textFile.first() // First item in this RDD
res1: String = # Apache Spark

Now let’s use a transformation. We will use the filter transformation to return a new RDD with a subset of the items in the file.

scala> val linesWithSpark = textFile.filter(line => line.contains("Spark"))
linesWithSpark: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at filter at <console>:27

We can chain together transformations and actions:

scala> textFile.filter(line => line.contains("Spark")).count() // How many lines contain "Spark"?
res3: Long = 15