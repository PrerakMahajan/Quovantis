package org.example

//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, rank, when}


object App  {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("QuoTest")
      .getOrCreate()

    /* Read 2 dataFrames*/

    val marksDF = spark.read.option("header", "true")
    .csv("/Users/prerak/Desktop/Fiserv/quovantis/marks.csv")

    val studentDF = spark.read.option("header", "true")
      .csv("/Users/prerak/Desktop/Fiserv/quovantis/student.csv")

    import spark.implicits._

    val joinDF = marksDF.join(studentDF,marksDF("student_id") ===  studentDF("student_id"),"inner")

    val result =  joinDF
      .withColumn("Result",($"english"+$"hindi"+$"maths"+$"science")/4)
      .withColumn("Grade",
        when($"Result" >= 90 , lit('A'))
        .when($"Result" >= 80 && $"Result" < 90 , lit('B'))
          .when($"Result" >= 70 && $"Result" < 80 , lit('C'))
            .when($"Result" >= 40 && $"Result" < 70 , lit('D'))
             .otherwise( lit('F')))
      .withColumn("Status",
        when($"Grade" === "F",lit("Fail"))
          .otherwise(lit("Pass")))

    //Answer 1
    val marksheet =  result.select($"name",$"Result",$"Grade")
    marksheet.write.option("header", "true").csv("/Users/prerak/Desktop/Fiserv/quovantis/answer1")

    //Answer 2
    val sctn_pass_Fail_DF = result.groupBy($"Section").pivot($"Status").count()
    sctn_pass_Fail_DF.coalesce(1)
      .write.option("header", "true").csv("/Users/prerak/Desktop/Fiserv/quovantis/answer2")

    //Answer 3
    val topper = result.withColumn("Ranks",
      rank().over( Window.partitionBy($"Section").orderBy($"Result".desc)))
      .filter($"Ranks" === 1)
      .select($"Section",$"Name")

    topper.coalesce(1)
      .write.option("header", "true").csv("/Users/prerak/Desktop/Fiserv/quovantis/answer3")

    //Answer 4
    val avg = result.groupBy($"Class").avg("Result")
    avg.coalesce(1)
      .write.option("header", "true").csv("/Users/prerak/Desktop/Fiserv/quovantis/answer4")

    //Answer 5
    val failSecC =  result.filter($"Section"=== "C" && $"Status" === "Fail").select($"Name")
    failSecC.write.option("header", "true").csv("/Users/prerak/Desktop/Fiserv/quovantis/answer5")


  }



}
