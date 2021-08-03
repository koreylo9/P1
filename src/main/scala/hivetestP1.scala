import org.apache.spark.sql.SparkSession

object hivetestP1 {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sql("DROP table IF EXISTS BevA")
    spark.sql("create table IF NOT EXISTS BevA(id Int,name String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE BevA")
    //spark.sql("SELECT * FROM BevA").show()
    spark.sql("SELECT Count(*) AS TOTALCOUNT FROM BevA").show()
    spark.sql("SELECT Count(*) AS NumBranch2BevAFile FROM BevA WHERE BevA.name='Branch2'").show()
  }
}

