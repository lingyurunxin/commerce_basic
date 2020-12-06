import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Aggregate")
        val context: SparkContext = new SparkContext(sparkConf)


        val value: RDD[(Int, Int)] = context.makeRDD(List(1, 1)).map((_, 1))
        val value1: RDD[(Int, Int)] = context.makeRDD(List(2, 3)).map((_, 1))
        println(value.join(value1).collect().mkString("|"))


    }

}
