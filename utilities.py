import datetime

# add time to a string date time, and return the new
# date time as string
def dateTimeAddition(time1,add_time):
    date1 = time1
    # convert str to datetime
    year_str = date1[:4]
    month_str = date1[4:6]
    day_str = date1[6:8]
    hour_str = date1[8:10]
    min_str = date1[10:12]
    sec_str = date1[12:]
    year_int = int(year_str)
    month_int = int(month_str)
    day_int = int(day_str)
    hour_int = int(hour_str)
    min_int = int(min_str)
    sec_int = int(sec_str)
    date_time = datetime.datetime(year_int,month_int,
                                day_int,hour_int,
                                min_int,sec_int)
    
    time_selected = add_time  
    min_change = datetime.timedelta(minutes=time_selected) 
    date_time = date_time + min_change  # add time to converted time
    date_time_str = dateTimeToString(date_time)  # after getting the new time, convert it back and return
    return date_time_str

# convert date time formate to string
def dateTimeToString(time1):
    date_time_int = time1.year*10000000000 + time1.month * 100000000 + time1.day * 1000000 + time1.hour*10000 + time1.minute*100 + time1.second
    return str(date_time_int)

# calculate the precentage difference between observed level and predicted level
def errorCalculation(obs_lvl, p_lvl):
    # check which val should be the base
    if obs_lvl[-1:] == "p":
        print(p_lvl+"\n")
        print(obs_lvl+"\n")
        temp = obs_lvl
        obs_lvl = p_lvl
        p_lvl = temp
    # convert them to float
    obs_lvl_fl = float(obs_lvl)
    p_lvl_fl = float(p_lvl[:-1])
    if obs_lvl_fl == 0 or obs_lvl == None or p_lvl == None:  # if base is 0, return ---
        return "---"
    difference = (obs_lvl_fl - p_lvl_fl)/obs_lvl_fl  # compute difference 
    diff_int = int(difference * 100000)  # adjust format to return only 3 decimal places
    difference = diff_int/1000
    return str(difference)+"%"