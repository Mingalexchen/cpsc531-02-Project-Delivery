from pyspark import SparkContext
import re
import utilities

sc=SparkContext()
testing = False


#step 1: filter needed data from file----------------------------------------------------------
lines=sc.textFile('flowData0.txt',1)                # Read text as RDD
representing_station = "3"                          # change value here for different station
# filter out unneeded stations
representing_station_rdd = lines.filter(lambda x: x[0] == representing_station)
# convert to <time,flow> kv pair
obs_time_lvl_pair_rdd = representing_station_rdd.map(
                        lambda x: [re.split('\s+', x)[1],re.split('\s+', x)[3]])

if testing == True:
    print (obs_time_lvl_pair_rdd)                              # print result for testing
    obs_time_lvl_pair_rdd.saveAsTextFile("step1_result")

# step 1 ends ---------------------------------------------------------------------------------



# step 2: convert prediction to comparable form------------------------------------------------
lines=sc.textFile('rainFor0.txt',1)                # Read text as RDD

time_interval_allowed = [5,10,30,60,120,180,360,720,1440,2880]
index_allowed         = [2, 3, 4, 5,  6,  7,  8,  9,  10,  11]
for i in range(10):
    time_selected = time_interval_allowed[i]
    forecast_index = index_allowed[i]
    # create kv pair 
    forecast_time_lvl_pair_rdd = lines.map(
                        lambda x: [utilities.dateTimeAddition(re.split('\s+', x)[1],time_selected),
                        re.split('\s+', x)[forecast_index]+"p"])
    # calculate difference
    difference_rdd = obs_time_lvl_pair_rdd.union(forecast_time_lvl_pair_rdd).reduceByKey(lambda x,y: x +"/" + y + "/" + utilities.errorCalculation(x,y))
    # filter meaningless results
    filtered_difference_rdd = difference_rdd.filter(lambda x: x[1][-1:]=="%" or x[1][-1:]== "-")
    save_path_name = "difference_" + str(time_selected)
    filtered_difference_rdd.saveAsTextFile(save_path_name)
    
