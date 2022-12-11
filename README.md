# cpsc531-02-Project-Delivery

## Dependency: 
python 3.X  
pyspark 3.3.1  
spark 3.1.2  

## Files  
python source code:  
  ### compare_result.py  
  --- python program that contains detailed explaination of logic of the program. 
  --- Outputs result for each step by default. 
  --- The last step outputs the difference between observation and predicted at one selected time.   
  ### compare_result_all.py  
  ---  python program that outputs the difference between observation and predicted of all predicted time
  ### utilities.py  
  --- contains helper functions  
  
## How to run it
at current folder  
./python3 compare_result.py  
or  
./python3 compare_result_all.py  
depends on which one you want to run.  

Note:  
--- make sure provided sample data are in the same folder as python files,
or program will fail to find them.  
--- Only tested on ubuntu 20.04. No guarantee that it will run on other OS.  
--- Program will fail if you do not remove existing output files.  

## Sample Outputs
Sample outputs are provided for compare_result.py and compare_result_all.py  
 
--- 
### stepX_result are output files for compare_result.py   
X represents the step indicated in compare_result.py.
  
This part contains very little data, and only for the purpose of explaining how the program works.  
Only one row is valid and output to the step3 sample output.
  
--- 
### difference_* are output files for compare_result_all.py  
The value after _ represents the time difference bewteen observation and prediction  
  
EX: difference_30 contains difference between    
    prediction of level that is calculated with data from 30mins before the time indicated by the key   
    and   
    observation at the time equal to key  
  
This part contains more data, and each result contains 9424 to 9999 row.
  
Note: if you tries to test program with data that has no matching time, the result will be empty because no meaningful comparison can be done.


