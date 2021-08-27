import org.apache.spark.sql.functions.{explode}

#Fetching nested Json from the file
val data = spark.read.option("multiline", "true").json("<input_file_path>")
#Dropping unwanted columns like meta and info for perticular problemstatement
val innings=data.drop("meta").drop("info")
#Expoding the innings.overs to get perticular column
val s5=innings.withColumn("batsman",explode($"innings.overs"))
val s6=s5.withColumn("delivery",explode($"batsman.deliveries"))
#Exploding for batter in the nested json
val s7=s6.withColumn("batter",explode($"delivery.batter"))
#Exploding for bowler in the nested json
val s7=s6.withColumn("batter",explode($"delivery.bowler"))
#Getting the batter and bowler column for the dataset
val s7=s6.withColumn("batter",explode($"delivery.batter")).withColumn("bowler",explode($"delivery.bowler"))
#Exploding and fetching runs scored by the batter for each bowl with respect to the bowler who bowled him
val s10=s7.withColumn("runs",explode($"delivery.runs.total"))
#Dropping out unnessary columns from the dataset
val res_table= s10.drop("innings").drop("batsman").drop("delivery")
#resultant table mainly shows the batter bowler and runs scored for each ball 
res_table.show()
#Summing up the runs for each batsman for a perticular bowler
val sum_runs=res_table.groupBy("batter","bowler").agg(sum("runs")).show()
#once we Sum up the total runs than for each batsman the bowler from which he has scored maximum number of 
#runs that is max_runs
val max_runs=sum_runs.groupBy("batter","bowler").agg(max("runs").as("max_runs")).show()

























