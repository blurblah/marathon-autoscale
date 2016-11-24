__author__ = 'tkraus'

import sys
import requests
import json
import math
import time
import config


class Marathon(object):
    def __init__(self, endpoint):
        self.uri = endpoint
        self.apps = self.get_all_apps()

    def get_all_apps(self):
        response = requests.get(self.uri + '/v2/apps', verify=False).json()
        if response['apps'] ==[]:
            print ("No Apps found on Marathon")
            sys.exit(1)
        else:
            apps=[]
            for i in response['apps']:
                appid = i['id'].strip('/')
                apps.append(appid)
            print ("Found the following App LIST on Marathon =", apps)
            return apps

    def get_app_details(self, marathon_app):
        response = requests.get(self.uri + '/v2/apps/'+ marathon_app, verify=False).json()
        if (response['app']['tasks'] ==[]):
            print ('No task data on Marathon for App !', marathon_app)
        else:
            app_instances = response['app']['instances']
            self.appinstances = app_instances
            print(marathon_app, "has", self.appinstances, "deployed instances")
            app_task_dict={}
            for i in response['app']['tasks']:
                taskid = i['id']
                hostid = i['host']
                slaveId=i['slaveId']
                print ('DEBUG - taskId=', taskid +' running on '+hostid + 'which is Mesos Slave Id '+slaveId)
                app_task_dict[str(taskid)] = str(hostid)
            return app_task_dict

    def scale_app(self, marathon_app, autoscale_multiplier, max_instances):
        target_instances_float=self.appinstances * autoscale_multiplier
        target_instances=math.ceil(target_instances_float)
        if target_instances > max_instances:
            print("Reached the set maximum instances of", max_instances)
            target_instances = max_instances
        else:
            target_instances = target_instances
        data ={'instances': target_instances}
        json_data = json.dumps(data)
        headers = {'Content-type': 'application/json'}
        response = requests.put(self.uri + '/v2/apps/'+ marathon_app, json_data, headers=headers, verify=False)
        print ('Scale_app return status code =', response.status_code)


def get_task_agentstatistics(task, host):
    # Get the performance Metrics for all the tasks for the Marathon App specified
    # by connecting to the Mesos Agent and then making a REST call against Mesos statistics
    # Return to Statistics for the specific task for the marathon_app
    response = requests.get('http://' + host + ':5051/monitor/statistics.json', verify=False).json()
    for i in response:
        executor_id = i['executor_id']
        if (executor_id == task):
            task_stats = i['statistics']
            print ('****Specific stats for task',executor_id,'=',task_stats)
            return task_stats


def timer():
    print("Successfully completed a cycle, sleeping for 10 seconds ...")
    time.sleep(10)
    return

if __name__ == "__main__":
    print ("This application tested with Python3 only")

    config_file = 'config.properties'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]

    cfg = config.Configuration()
    try:
        cfg.load(config_file)
    except Exception as e:
        print("Exception %s" % e)
        sys.exit(1)

    running=1
    while running == 1:
        # Initialize the Marathon object
        aws_marathon = Marathon(cfg.marathon_endpoint)
        print ("Marathon URI = ...", aws_marathon.uri)
        # Call get_all_apps method for new object created from aws_marathon class and return all apps
        marathon_apps = aws_marathon.get_all_apps()
        print ("The following apps exist in Marathon...", marathon_apps)
        # Quick sanity check to test for apps existence in MArathon.
        marathon_app = cfg.target_app
        if cfg.target_app in marathon_apps:
            print ("  Found your Marathon App=", marathon_app)
        else:
            print ("  Could not find your App =", marathon_app)
            timer()
            continue

        # Return a dictionary comprised of the target app taskId and hostId.
        app_task_dict = aws_marathon.get_app_details(marathon_app)
        print ("    Marathon  App 'tasks' for", marathon_app, "are=", app_task_dict)

        app_cpu_values = []
        app_mem_values = []
        for task,agent in app_task_dict.items():
            print('Task = '+ task)
            print ('Agent = ' + agent)
            # Compute CPU usage
            task_stats = get_task_agentstatistics(task, agent)
            print('#######CPU SYSTEM TIME1 : ' + str(task_stats['cpus_system_time_secs']))
            cpus_system_time_secs0 = float(task_stats['cpus_system_time_secs'])
            cpus_user_time_secs0 = float(task_stats['cpus_user_time_secs'])
            timestamp0 = float(task_stats['timestamp'])

            time.sleep(1)

            task_stats = get_task_agentstatistics(task, agent)
            print('#######CPU SYSTEM TIME2 : ' + str(task_stats['cpus_system_time_secs']))
            cpus_system_time_secs1 = float(task_stats['cpus_system_time_secs'])
            cpus_user_time_secs1 = float(task_stats['cpus_user_time_secs'])
            timestamp1 = float(task_stats['timestamp'])

            cpus_time_total0 = cpus_system_time_secs0 + cpus_user_time_secs0
            cpus_time_total1 = cpus_system_time_secs1 + cpus_user_time_secs1
            cpus_time_delta = cpus_time_total1 - cpus_time_total0
            timestamp_delta = timestamp1 - timestamp0

            # CPU percentage usage
            usage = float(cpus_time_delta / timestamp_delta) * 100

            # RAM usage
            mem_rss_bytes = int(task_stats['mem_rss_bytes'])
            print ("task", task, "mem_rss_bytes=", mem_rss_bytes)
            mem_limit_bytes = int(task_stats['mem_limit_bytes'])
            print ("task", task, "mem_limit_bytes=", mem_limit_bytes)
            mem_utilization = 100 * (float(mem_rss_bytes) / float(mem_limit_bytes))
            print ("task", task, "mem Utilization=", mem_utilization)
            print()

            app_cpu_values.append(usage)
            app_mem_values.append(mem_utilization)
        # Normalized data for all tasks into a single value by averaging
        app_avg_cpu = (sum(app_cpu_values) / len(app_cpu_values))
        print ('Current Average  CPU Time for app', marathon_app, '=', app_avg_cpu)
        app_avg_mem=(sum(app_mem_values) / len(app_mem_values))
        print ('Current Average Mem Utilization for app', marathon_app,'=', app_avg_mem)
        #Evaluate whether an autoscale trigger is called for
        print('\n')
        if (app_avg_cpu > cfg.cpu_threshold) or (app_avg_mem > cfg.mem_threshold):
            print ("Autoscale triggered based Mem 'or' CPU exceeding threshold")
            aws_marathon.scale_app(marathon_app, cfg.multiplier, cfg.max_size)
        else:
            print ("Neither Mem 'or' CPU values exceeding threshold")

        timer()
