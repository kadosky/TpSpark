import org.apache.spark.sql._
import org.apache.spark._


object SparkApp {
    def SessionSpark() : Unit={
        System.setProperty("winutils","C:\\Hadoop")
        val ss = SparkSession.builder() //construction de la sessiom
          .master("local[*]")
          .appName("Ma premiere app spark")
          //.enableHiveSupport() // sur hive est installer
          .getOrCreate()

        val rdd = ss.sparkContext.parallelize(Seq("Ma premiere app spark, je suis diop assane"))
        //rdd.foreach(println)

        val df1 = ss.read
          .format("csv")
          .option("inferSchema",true) // forcer le type des variables selon un algo
          .option("header",true)
          .option("delimiter",";")
          .csv("D:\\Cours\\M1 IDSI\\SEMESTRE 2\\Big Data\\orders_csv.csv")

        //df1.show(10)
        // df1.printSchema() pour voir le schema des colonnes

        df1.createOrReplaceTempView("orders") // permet de creer une table temporaire car hive est absent
        // metastore cree pour ta session 
        ss.sql("select count(*) from orders where city = 'NEWTON'").show(10)
    }

    def main(args: Array[String]): Unit = {
        SessionSpark()
    }
}
