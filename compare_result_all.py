from pyspark import SparkContext
import re
import utilities

sc=SparkContext()
testing = False


# part 1: filter data inside obs file----------------------------------------------------------
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

# part 2: use a loop to compute the result for all 10 different time period 
lines=sc.textFile('rainFor0.txt',1)                # Read text as RDD

time_interval_allowed = [5,10,30,60,120,180,360,720,1440,2880] # time difference from observation time in mins
index_allowed         = [2, 3, 4, 5,  6,  7,  8,  9,  10,  11] # where they are if we divide each row with white space
for i in range(10):
    time_selected = time_interval_allowed[i]
    forecast_index = index_allowed[i]
    # create kv pair 
    forecast_time_lvl_pair_rdd = lines.map(
                        lambda x: [utilities.dateTimeAddition(re.split('\s+', x)[1],time_selected),
                        re.split('\s+', x)[forecast_index]+"p"])
    # calculate difference
    difference_rdd = obs_time_lvl_pair_rdd.union(forecast_time_lvl_pair_rdd).reduceByKey(lambda x,y: x +"/" + y + "/" + utilities.errorCalculation(x,y))
    # filter out meaningless results and sort it by key. 
    filtered_difference_rdd = difference_rdd.filter(lambda x: x[1][-1:]=="%" or x[1][-1:]== "-").sortByKey(True)
    # change save directory name for each loop
    save_path_name = "difference_" + str(time_selected)
    # save to file
    filtered_difference_rdd.saveAsTextFile(save_path_name)
    