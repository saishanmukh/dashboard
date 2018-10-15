import pymysql
from datetime import datetime
import flask
from flask import jsonify,request,redirect, url_for
from flask_cors import CORS


time = datetime.now()
time = time.strftime("%Y-%m-%d")
 

def fetching():
    try:
        db = pymysql.connect(host ="localhost",user="root",password ="133233",db ="dashboard")
        c = db.cursor()
        
        c.execute("""SELECT skExecutionTrackerId,TaskName,SubTaskMasterId,StepName,
ActualStartTime,TIME(StartTime) as Start_time,DATE(StartTime) as Start_date,TIME(EndTime) as End_time,
DelayGracePeriodINSec,TIMEDIFF(TIME(StartTime),ActualStartTime) as time_diff_starting,
EmailTriggerTimeInSec,
TaskExecutionTracker.Status,ErrorAction,DelayAction,
StartTime,EndTime
                  FROM TaskExecutionTracker 
                  JOIN SubTaskMaster ON SubTaskMaster.bTaskMasterId = TaskExecutionTracker.SubTaskMasterId
                  JOIN TaskMaster ON TaskMaster.skMasterId = SubTaskMaster.TaskMasterId 
                  """)
        
        tasktracker = c.fetchall()
        return tasktracker
    
    except:
        print("error")

    finally:
        c.close()
        db.close()



def h_to_sec(t):
    h, m, s = [int(i) for i in t.split(':')]
    return 3600*h + 60*m + s

def fppp(today_s):
    today_jobs = fetching()
    d={}
    print(today_s)
    for x in today_jobs:
        if(str(x[6]) == today_s and str(x[7]) != "None"):
            start_time = str(x[5])
            end_time = str(x[7]).split(".")
            FMT = "%H:%M:%S"
            current_running_time = str(datetime.strptime(end_time[0], FMT) - datetime.strptime(start_time, FMT))
            a = {'MainTask' :x[1],'SubtaskId' : x[2], 'SubTaskname' : x[3], 'StartTime' :str(x[5]),'Status' : str(x[-5]),"time_taken" : h_to_sec(current_running_time)}
            d[x[0]] = a
    
    return d

    
app = flask.Flask(__name__)
CORS(app)
app.config["DEBUG"] = True

@app.route('/jobs', methods=['GET','POST'])
def job_fetching():
    data_fetched = fetching()
    d = {}
    
    for x in data_fetched:
        start_time = str(x[5])
        end_time = str(x[7]).split(".")
        FMT = "%H:%M:%S"
        if str(x[5]) != "None" and str(x[7]) != "None":       
            current_running_time = str(datetime.strptime(end_time[0], FMT) - datetime.strptime(start_time, FMT))
            #print(current_running_time)
            a = {'MainTask':x[1],'SubtaskId' : x[2], 'SubTaskname' : x[3],'StartTime' :str(x[5]),'EndTime' : str(x[7]), 'delay_grace' : str(x[8]),"running_time" : current_running_time}
            d[x[0]]=a
            
    return jsonify(d)

@app.route('/completed_jobs', methods=['GET','POST'])
def completed_jobs():
    data_fetching_for_completed_jobs = fetching()
    d={}
    for x in data_fetching_for_completed_jobs:
#        print(x)
        start_time = str(x[5])
        end_time = str(x[7]).split(".")
        
        if(x[-5] == "1") and str(x[7]) != "None":
            FMT = "%H:%M:%S"
            current_running_time = str(datetime.strptime(end_time[0], FMT) - datetime.strptime(start_time, FMT))
            a ={'MainTask' :x[1],'SubtaskId' : x[2], 'SubTaskname' : x[3], 'StartTime' :str(x[5]),'Status' : str(x[-5]),"time_taken" : h_to_sec(current_running_time)}
            d[x[0]] = a
    return jsonify(d)

@app.route('/today', methods=['GET','POST'])
def today_s():
    global today_date
    d ={}
    if(request.method == 'POST'):
        today = request.form
        print(today["From"])
        today_date = today["From"]
        d =fppp(today_date)
        print(d)    
    return jsonify(d)

@app.route('/gooo', methods=['GET','POST'])
def fkdkp():
    today_jobs = fetching()
    global today_date
    d={}
    print(today_date)
    
    for x in today_jobs:
        if(str(x[6]) == today_date and str(x[7]) != "None"):
            start_time = str(x[5])
            end_time = str(x[7]).split(".")
            FMT = "%H:%M:%S"
            current_running_time = str(datetime.strptime(end_time[0], FMT) - datetime.strptime(start_time, FMT))
            a = {'MainTask' :x[1],'SubtaskId' : x[2], 'SubTaskname' : x[3], 'StartTime' :str(x[5]),'Status' : str(x[-5]),"time_taken" : h_to_sec(current_running_time)}
            d[x[0]] = a

    return jsonify(d)

@app.route('/filter1', methods=['GET','POST'])
def filter1():
    filter1 = fetching()
    d={}
    #print(filter1)
    for x in filter1:
        if(x[-5] == "1") and str(x[7]) != "None":
            FMT = "%H:%M:%S"
            start = x[-1].split(".")[0]
            end = x[-2].split(".")[0]
            start_date = start.split(" ")[0]
            end_date = end.split(" ")[0]
            if(start_date == end_date):
                running_time = str(datetime.strptime(str(x[-1].split(" ")[1]).split(".")[0], FMT) - datetime.strptime(str(x[-2].split(" ")[1]).split(".")[0], FMT))
            #print(running_time)
            p=h_to_sec(running_time)
            s =h_to_sec("00:00:30")
            #print(str(p)+"   "+str(s))
            
            if (p < s):
                print(running_time)
                start_time = str(x[5])
                end_time = str(x[7]).split(".")
                current_running_time = str(datetime.strptime(end_time[0], FMT) - datetime.strptime(start_time, FMT))
                a = {'MainTask' :x[1],'SubtaskId' : x[2], 'SubTaskname' : x[3], 'StartTime' :str(x[5]),'EndTime' : str(x[7]),"running_time" : current_running_time}
                d[x[0]] = a
                print("............")
    return jsonify(d)

if __name__ == "__main__":
    app.run(port = 8000)
