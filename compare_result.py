from pyspark import SparkContext
import re
import utilities

sc=SparkContext()
testing = True


#step 1: filter needed data from file----------------------------------------------------------
lines=sc.textFile('flowData1.txt',1)                # Read text as RDD
representing_station = "3"                          # change value here for different station
# filter out unneeded stations
representing_station_rdd = lines.filter(lambda x: x[0] == representing_station)
# convert to <time,level> kv pair
obs_time_lvl_pair_rdd = representing_station_rdd.map(
                        lambda x: [re.split('\s+', x)[1],re.split('\s+', x)[3]])

if testing == True:
    print (obs_time_lvl_pair_rdd)                              # print result for testing
    obs_time_lvl_pair_rdd.saveAsTextFile("step1_result")

# step 1 ends ---------------------------------------------------------------------------------



# step 2: convert prediction to comparable form------------------------------------------------
lines=sc.textFile('rainFor2.txt',1)                # Read text as RDD
# select time you want to compare
# in db: rm05	rm10 	rm30	r001	r002	r003	r006	r012	r024	r048
# converted to mins: 5,10,30,60,120,180,360,720,1440,2880
time_selected = 5
forecast_index = 2
time_interval_allowed = [5,10,30,60,120,180,360,720,1440,2880]
#               index = [2, 3, 4, 5,  6,  7,  8,  9,  10,  11]
# ensure a correct time is entered
if time_selected not in time_interval_allowed:
    print ("error, time not allowed") 
    exit(1)

# helper function to calculate new datetime
# add time in formate yyyymmddhhmmss (ex:20210122084400)
forecast_time_lvl_pair_rdd = lines.map(
                        lambda x: [utilities.dateTimeAddition(re.split('\s+', x)[1],time_selected),
                        re.split('\s+', x)[forecast_index]+"p"])

if testing == True:
    forecast_time_lvl_pair_rdd.saveAsTextFile("step2_result")
# step 2 ends ---------------------------------------------------------------------------------



# step 3: compute the difference between two ------------------------------------------------
difference_rdd = obs_time_lvl_pair_rdd.union(forecast_time_lvl_pair_rdd).reduceByKey(
                                            lambda x,y: x +"/" + y + "/" + 
                                            utilities.errorCalculation(x,y))
                                        
# filter result to eliminate unrelated time
filtered_difference_rdd = difference_rdd.filter(lambda x: x[1][-1:]=="%" or x[1][-1:]== "-")
filtered_difference_rdd.saveAsTextFile("step3_result")
# step 3 ends ---------------------------------------------------------------------------------
