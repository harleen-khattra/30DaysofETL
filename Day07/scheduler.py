import schedule
import time
from etl_pipeline import etl_pipeline

def task1():
    return etl_pipeline()

def task2():
    print('Hello World!')

#Schedule the task to run every 5 seconds
schedule.every(5).seconds.do(task1).tag('task1')
schedule.every(5).seconds.do(task2).tag('task2')

#You can also schedule to run the task after every hour or a day or once a week
start_time = time.time()
while True:
    schedule.run_pending() #Check and run any pending tasks
    time.sleep(1) #Wait 1 second

    #Clear task 1 after 15 seconds but keep task 2 running
    if time.time() - start_time > 15:
        print('Clearing Task 1')
        schedule.clear('task1') #Stops task 1 

    if time.time()- start_time >30:
        print('Stopping all tasks..')
        schedule.clear() #Stop all tasks
        break



print('Scheduled tasks stopped')
