import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._


object Assignment20 {
  case class TravelMode_cls(TravelMode:String,price:Int)
  case class UserDetails_cls(UserId:String,Name:String,Age:Int)

  def main(args: Array[String]): Unit = {

    //Creating spark Session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    println("Spark Session Object created")

      //creating schema for the table that need to be loaded from file
    val Manual_schema = new StructType(Array( new StructField("UserId",StringType,false),
      new StructField("Source",StringType,false),
      new StructField("Destination", StringType, true),
      new StructField("TravelMode", StringType, false),
      new StructField("Distance", LongType, false),
      new StructField("Year", LongType, false) ))

    //Now load the data with above schema
    val Holiday_data = spark.read.format("csv")
      .option("header", "false")
      .schema(Manual_schema)
      .load("D:\\Lavanya\\S20_Dataset_Holidays.txt").toDF()

    Holiday_data.registerTempTable("HolidayTrip") //Registering as temporary table HolidayTrip.
    println("Dataframe Registered as table !")
    Holiday_data.show()

    // Loading the second data set Transport.csv
    val data2 = spark.sparkContext.textFile("D:\\Lavanya\\S20_Dataset_Transport.txt")

    // Loading the 3rd data set S20_Dataset_User_details.csv
    val data3 = spark.sparkContext.textFile("D:\\Lavanya\\S20_Dataset_User_details.txt")// Loading the 3rd data set Transport.csv

    //For implicit conversions like converting RDDs and sequences to DataFrames
    import spark.implicits._

    //converting RDDs and sequences to DataFrames
    val Traveltype = data2.map(x=> x.split(",")).map(x => TravelMode_cls(x(0),x(1).toInt)).toDF
    Traveltype.show()
    Traveltype.registerTempTable("TravelMode") //Registering as temporary table TravelMode.
    println("Dataframe Registered as table !")

    //converting RDDs and sequences to DataFrames
    val UserDetails = data3.map(x=> x.split(",")).map(x => UserDetails_cls(x(0),x(1),x(2).toInt)).toDF
    Traveltype.show()
    Traveltype.registerTempTable("UserDetails") //Registering as temporary table UserDetails.
    println("Dataframe Registered as table !")

    //To get the distribution of the total number of air-travelers per year
    val TotAirTravellers = spark.sql("select year, count(TravelMode) from HolidayTrip where TravelMode = 'airplane' group by year ")
//    TotAirTravellers.show()

    //the total air distance covered by each user per year
    val TotAirDistCovered = spark.sql("select UserId, year, sum(Distance) from HolidayTrip where TravelMode = 'airplane' group by year, UserId order by UserId, year ")
  //  TotAirDistCovered.show()

    //Which user has travelled the largest distance till date
     val LargestDistanceUser = spark.sql("select sum(Distance) as MaxDistance, UserId from HolidayTrip group by UserId order by MaxDistance desc").take(1)
    println(s"Gives Largest distance covered by the user and user id ${LargestDistanceUser.foreach(println)} ")

    //Most preferred destination for all users.
    val PrefferedDest = spark.sql("select count(*) as distribution, Destination from HolidayTrip group by Destination order by distribution desc").take(1)
    println(s"Gives the number of times and Destination visted by the user  ${PrefferedDest.foreach(println)} ")

    // What is the total amount spent by every user on air-travel per year
    val newColumn = when(col("TravelMode").equalTo("airplane") , "170").when(col("TravelMode").equalTo("ship"), "200").when(col("TravelMode").equalTo("car") , "140").when(col("TravelMode").equalTo("train") , "120")
    val newData = Holiday_data.withColumn("Expense",newColumn ) //Adding new column containing the expenses
    newData.createOrReplaceTempView("TotAmountPerUser")
    val TotExpenseperuser = spark.sql("select UserId, year, sum(expense) from TotAmountPerUser where TravelMode == 'airplane' group by Year, UserId order by Year, UserId")
    TotExpenseperuser.show()

    //Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most every year
    val newUserColumn = Holiday_data.join(UserDetails,Holiday_data("UserId") <=> UserDetails("UserId") )
    newUserColumn.show()
    newUserColumn.createOrReplaceTempView("YearwiseDetails")
    val Agegroup = spark.sql("select Year, age ,count(Age) as Agecount from YearwiseDetails group by Year,age order by Agecount desc").take(1)
    println(s"Gives the year, agegroup, Maximium number of times travelled  ${Agegroup.foreach(println)} ")

    //Which route is generating the most revenue per year
     val MoreRevenue = spark.sql("select year, source, Destination, sum(expense) as MaxExpense from TotAmountPerUser group by year, source, Destination order by MaxExpense desc")
    MoreRevenue.show(10)
  }
}
