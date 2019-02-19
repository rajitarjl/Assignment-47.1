// Databricks notebook source
// Assignment 47.1
// Task 1 - Problem Statement 1
/*
Given a dataset of college students as a text file (name, subject, grade, marks) :
Dataset : https://drive.google.com/open?id=1SMuaFRag_pitDld_Vq9BsYeH0NaUBtW0
1. Read the text file, and create a tupled rdd.
2. Find the count of total number of rows present.
3. What is the distinct number of subjects present in the entire school
4. What is the count of the number of students in the school, whose name is Mathew and marks is 55
*/

// COMMAND ----------

//1. Read the text file, and create a tuple rdd.
// read the CSV file
val rdd  = sc.textFile("/FileStore/tables/student_dataset.txt")
// split & clean data
val headerAndRows = rdd.map(line => line.split(",")).map(array => (array(0), array(1), array(2), array(3), array(4))).collect.foreach(println)
//println(headerAndRows)

// COMMAND ----------

// 2. Find the count of total number of rows present.
// count of total number of rows present
println(s"The total number of rows present is "+rdd.count())

// COMMAND ----------

// 3. What is the distinct number of subjects present in the entire school

val distinctSubject = rdd.map(line => line.split(",")).map(array => (array(1))).distinct().count() - 1
println("The distinct number of subjects present in the entire school is : "+distinctSubject)

// COMMAND ----------

// 4. What is the count of the number of students in the school, whose name is Mathew and marks is 55

// count of the number of students in the school, whose name is Mathew and marks are 55
val countstudents = rdd.filter(s => s.contains("Mathew") && s.contains("55")).count()
println("Count of the number of students in the school whose name is Mathew and marks is 55 : "+countstudents)

// COMMAND ----------

// Problem Statement 2
/*
1. What is the count of students per grade in the school?
2. Find the average of each student (Note - Mathew is grade-1, is different from Mathew in )some other grade!)
3. What is the average score of students in each subject across all grades?
4. What is the average score of students in each subject per grade?
5. For all students in grade-2, how many have average score greater than 50?
*/

// COMMAND ----------

import org.apache.spark.sql.functions._

//Read CSV data file
val df = spark.read.option("header", "true").csv("/FileStore/tables/student_dataset.txt")

//What is the count of students per grade in the school?
println("*----- Count of Students per Grade in School -----*")
df.groupBy("grade").agg(count("grade") as "Grade Count").show()

//Find the average of each student grade wise
println("*----- Average of Each Student Gradewise -----*")
df.groupBy("name","grade").agg(avg("marks") as "Average Marks").show()

//Find the average of each student (irrespective of grade)
println("*----- Average of Each Student -----*")
df.groupBy("name").agg(avg("marks") as "Average Marks").show()

//What is the average score of students in each subject across all grades?
println("*----- Average Score of Students in each Subject across all Grades -----*")
df.groupBy("subject").agg(avg("marks")).show()

//What is the average score of students in each subject per grade?
println("*----- Average Score of Students in each Subject per Grade -----*")
df.groupBy("subject","grade").agg(avg("marks")).show()

//For all students in grade-2, how many have average score greater than 50?
println("*----- For all students in grade-2, how many have average score greater than 50? -----*")
val gr2df = df.where(df("grade") === "grade-2").groupBy("name").agg(avg("marks")>50 as "AverageMarks")
println("All students in grade-2, who have average score greater than 50 : " + gr2df.filter(gr2df("AverageMarks") === true).count())

// COMMAND ----------

// Task 1 - Problem Statement 3
/*
Are there any students in the college that satisfy the below criteria :
1. Average score per student_name across all grades is same as average score per student_name per grade
Hint - Use Intersection Property.
*/

// COMMAND ----------

println(s"The average score per student_name across all grades:-")
val df1 = df.groupBy("name").agg(avg("marks") as "AvgScore").show()

// COMMAND ----------

println(s"The average score per student_name per grade:-")
val df2 = df.groupBy("name","grade").agg(avg("marks") as "AverageScore").show()

// COMMAND ----------

// Average score per student_name across all grades
val df1 = df.groupBy("name").agg(avg("marks") as "AverageScore")
df1.show()
// average score per student_name per grade
val df2 = df.groupBy("name","grade").agg(avg("marks") as "AverageScore")
df2.show()
// select the 2 columns for intersect to happen as otherwise it shall throw error as df1 has 2 columns and df2 has 3 columns
val columnnames = Seq ("name", "AverageScore")
val df3 = df2.select(columnnames.map(c => col(c)):_*)
val result = df1.intersect(df3)
df3.show()
result.show()
println("There are no student in the college which satisfy the criteria that Average score per student_name across all grades is same as average score per student_name per grade")

// COMMAND ----------

// Task 2 - Problem Statement 1
// Data Set : https://drive.google.com/drive/folders/0B_P3pWagdIrrVThBaUdVSUtzbms
// What is the distribution of the total number of air-travelers per year

// COMMAND ----------

import org.apache.spark.sql.functions._

//Read CSV data files
val dfHol = spark.read.option("header", "true").csv("/FileStore/tables/Dataset_Holidays.txt")
val dfTrans = spark.read.option("header", "true").csv("/FileStore/tables/Dataset_Transport.txt")
val dfUser = spark.read.option("header", "true").csv("/FileStore/tables/Dataset_User_details.txt")

//Join of Holidays, Transport, User_Details
val dfholusr = dfHol.join(dfUser, dfHol.col("user_id") === dfUser.col("user_id"))
val dfholusrtrans = dfholusr.join(dfTrans, dfholusr.col("travel_mode") === dfTrans.col("travel_mode"))
println("*----- HolidayUser -----*")
dfholusr.show()
println("*----- HolidyUserTransport -----*")
dfholusrtrans.show()

// COMMAND ----------

//What is the distribution of the total number of air-travelers per year?
dfholusrtrans.groupBy("year_of_travel","name").count().orderBy("year_of_travel").show()

// COMMAND ----------

//What is the total air distance covered by each user per year?
dfholusrtrans.groupBy("year_of_travel","name").agg(sum("distance") as "distance sum").orderBy("year_of_travel").show()

// COMMAND ----------

////What is the total air distance covered by each user
dfholusrtrans.groupBy("name").agg(sum("distance") as "distance sum").orderBy("name").show()

// COMMAND ----------

//Which user has travelled the largest distance till date?
dfholusrtrans.groupBy("name").agg(sum("distance")).sort(desc("sum(distance)")).show()

// COMMAND ----------

//What is the most preferred destination for all users?
dfholusrtrans.groupBy("dest").count().sort(desc("count")).show()

// COMMAND ----------

println("The most preferred destination is India follow by China and then Russia")

// COMMAND ----------

/*Task 3
1) Which route is generating the most revenue per year
2) What is the total amount spent by every user on air-travel per year
3) Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most
every year.*/

// COMMAND ----------

//Anomynous function which takes 2 Integer and returns the product
val mulColumn : (Int,Int)=>Int=(num1:Int,num2:Int)=>{num1*num2}
//Declare the UDF
val mulColumnUDF = udf(mulColumn)

// COMMAND ----------

// Which route is generating the most revenue per year

// COMMAND ----------

//Modified Dataframe with Amount, add the new column “amount” by calling the udf
val dfholusrtransamt = dfholusrtrans.withColumn("amount",mulColumnUDF(dfholusrtrans.col("distance"),dfholusrtrans.col("cost_per_unit")))
dfholusrtransamt.show()

// COMMAND ----------

//Which route is generating the most revenue per year
dfholusrtransamt.groupBy("src","dest").sum("amount").sort(desc("sum(amount)")).show()
dfholusrtransamt.groupBy("dest","src").sum("amount").sort(desc("sum(amount)")).show()

// COMMAND ----------

// What is the total amount spent by every user on air-travel per year

// COMMAND ----------

dfholusrtransamt.groupBy("year_of_travel","name").sum("amount").sort(desc("year_of_travel"),desc("sum(amount)")).show()

// COMMAND ----------

// Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most every year.

// COMMAND ----------

//Anomynous function which determines age group
val chkAge : (Int)=>Int=(num1:Int)=>{if (num1 < 20) 1 else if (num1 < 35) 2 else 3}
//Declare the UDF
val chkAgeUDF = udf(chkAge)

// COMMAND ----------

//Modified Dataframe with Age Group, add the new column “agegrp” by calling the udf
val dfholusrtransage = dfholusrtrans.withColumn("agegrp",chkAgeUDF(dfholusrtrans.col("age")))
dfholusrtransage.show()

// COMMAND ----------

//Considering age groups of < 20 , 20-35, 35 > , which age group is travelling the most every year
dfholusrtransage.groupBy("year_of_travel","agegrp").agg(sum("distance")).sort(desc("year_of_travel"),desc("sum(distance)")).show()
