__author__ = 'tkraus'

import sys
import requests
import json
import math
import time
import config
import logging


class Marathon(object):
    def __init__(self, endpoint, auth_id=None, auth_password=None):
        self.uri = endpoint
        self.id = auth_id
        self.password = auth_password
        self.apps = self.get_all_apps()

    def get_all_apps(self):
        response = requests.get(self.uri + '/v2/apps', auth=(self.id, self.password), verify=False).json()
        if response['apps'] ==[]:
            logging.info("No Apps found on Marathon")
            sys.exit(1)
        else:
            apps=[]
            for i in response['apps']:
                appid = i['id'].strip('/')
                apps.append(appid)
            logging.info("Found the following App LIST on Marathon = %s" % apps)
            return apps

    def get_app_details(self, marathon_app):
        response = requests.get(self.uri + '/v2/apps/'+ marathon_app, auth=(self.id, self.password), verify=False).json()
        if (response['app']['tasks'] ==[]):
            logging.info('No task data on Marathon for App ! %s' % marathon_app)
        else:
            app_instances = response['app']['instances']
            self.appinstances = app_instances
            logging.info("%s has %d deployed instances" % (marathon_app, self.appinstances))
            app_task_dict={}
            for i in response['app']['tasks']:
                taskid = i['id']
                hostid = i['host']
                slaveId = i['slaveId']
                logging.info('DEBUG - taskId=%s running on %s which is Mesos Slave Id %s' % (taskid, hostid, slaveId))
                app_task_dict[str(taskid)] = str(hostid)
            return app_task_dict

    def scale_out(self, marathon_app, autoscale_multiplier, max_instances):
        target_instances_float=self.appinstances * autoscale_multiplier
        target_instances=math.ceil(target_instances_float)
        if target_instances > max_instances:
            logging.info("Reached the set maximum instances of %d" % max_instances)
            target_instances = max_instances
        else:
            target_instances = target_instances
        data = {'instances': target_instances}
        json_data = json.dumps(data)
        headers = {'Content-type': 'application/json'}
        response = requests.put(self.uri + '/v2/apps/'+ marathon_app,
                                json_data, headers=headers, auth=(self.id, self.password), verify=False)
        logging.info('Scale out return status code = %d' % response.status_code)

    def scale_in(self, marathon_app):
        target_instances = self.appinstances - 1
        if target_instances < 1:
            logging.info("Reached the set 1 instance.")
        else:
            data = {'instances': target_instances}
            json_data = json.dumps(data)
            headers = {'Content-type': 'application/json'}
            response = requests.put(self.uri + '/v2/apps/'+ marathon_app,
                                    json_data, headers=headers, auth=(self.id, self.password), verify=False)
            logging.info('Scale in return status code = %d' % response.status_code)



def get_task_agentstatistics(task, host):
    # Get the performance Metrics for all the tasks for the Marathon App specified
    # by connecting to the Mesos Agent and then making a REST call against Mesos statistics
    # Return to Statistics for the specific task for the marathon_app
    response = requests.get('http://' + host + ':5051/monitor/statistics.json', verify=False).json()
    for i in response:
        executor_id = i['executor_id']
        if (executor_id == task):
            task_stats = i['statistics']
            logging.info('****Specific stats for task %s = %s' % (executor_id, task_stats))
            return task_stats


def timer():
    logging.info("Successfully completed a cycle, sleeping for 10 seconds ...")
    logging.info('\n\n\n')
    time.sleep(10)
    return

if __name__ == "__main__":
    logging.basicConfig(#filename='marathon-autoscale.log',
                        format='[%(asctime)s] %(levelname)s  %(message)s',
                        level=logging.DEBUG)

    logging.info("This application tested with Python3 only")

    config_file = 'config.properties'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]

    cfg = config.Configuration()
    try:
        cfg.load(config_file)
    except Exception as e:
        logging.info("Exception %s" % e)
        sys.exit(1)

    running = 1
    continuous_low_usage_count = 0
    while running == 1:
        # Initialize the Marathon object
        aws_marathon = Marathon(cfg.marathon_endpoint, cfg.auth_id, cfg.auth_password)
        logging.info("Marathon URI = %s" % aws_marathon.uri)
        # Call get_all_apps method for new object created from aws_marathon class and return all apps
        marathon_apps = aws_marathon.get_all_apps()
        logging.info("The following apps exist in Marathon. %s" % marathon_apps)
        # Quick sanity check to test for apps existence in MArathon.
        marathon_app = cfg.target_app
        if cfg.target_app in marathon_apps:
            logging.info("  Found your Marathon App =  %s" % marathon_app)
        else:
            logging.info("  Could not find your App = %s" % marathon_app)
            timer()
            continue

        # Return a dictionary comprised of the target app taskId and hostId.
        app_task_dict = aws_marathon.get_app_details(marathon_app)
        logging.info("    Marathon  App 'tasks' for %s are = %s" % (marathon_app, app_task_dict))

        app_cpu_values = []
        app_mem_values = []
        for task,agent in app_task_dict.items():
            logging.info('Task = '+ task)
            logging.info('Agent = ' + agent)
            # Compute CPU usage
            task_stats = get_task_agentstatistics(task, agent)
            logging.info('#######CPU SYSTEM TIME1 : ' + str(task_stats['cpus_system_time_secs']))
            cpus_system_time_secs0 = float(task_stats['cpus_system_time_secs'])
            cpus_user_time_secs0 = float(task_stats['cpus_user_time_secs'])
            timestamp0 = float(task_stats['timestamp'])

            time.sleep(1)

            task_stats = get_task_agentstatistics(task, agent)
            logging.info('#######CPU SYSTEM TIME2 : ' + str(task_stats['cpus_system_time_secs']))
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
            logging.info("task %s mem_rss_bytes = %d" % (task, mem_rss_bytes))
            mem_limit_bytes = int(task_stats['mem_limit_bytes'])
            logging.info("task %s mem_limit_bytes = %d" % (task, mem_limit_bytes))
            mem_utilization = 100 * (float(mem_rss_bytes) / float(mem_limit_bytes))
            logging.info("task %s mem Utilization = %f" % (task, mem_utilization))
            logging.info('\n')

            app_cpu_values.append(usage)
            app_mem_values.append(mem_utilization)
        # Normalized data for all tasks into a single value by averaging
        app_avg_cpu = (sum(app_cpu_values) / len(app_cpu_values))
        logging.info('Current Average  CPU Time for app %s = %f' % (marathon_app, app_avg_cpu))
        app_avg_mem = (sum(app_mem_values) / len(app_mem_values))
        logging.info('Current Average Mem Utilization for app %s = %d' % (marathon_app, app_avg_mem))
        #Evaluate whether an autoscale trigger is called for
        logging.info('\n')
        if (app_avg_cpu > cfg.out_cpu_threshold) or (app_avg_mem > cfg.out_mem_threshold):
            logging.info("Auto scale-out triggered based Mem 'or' CPU exceeding threshold")
            continuous_low_usage_count = 0
            aws_marathon.scale_out(marathon_app, cfg.multiplier, cfg.max_size)
        else:
            logging.info("Neither Mem 'or' CPU values exceeding threshold")
            if (app_avg_cpu < cfg.in_cpu_threshold) and (app_avg_mem < cfg.in_mem_threshold):
                continuous_low_usage_count += 1
                logging.info("Met scale-in conditions. Mem 'and' CPU are under threshold.")
                logging.info("Scale-in condition count : %d" % continuous_low_usage_count)
            else:
                continuous_low_usage_count = 0

            if continuous_low_usage_count == 3:
                continuous_low_usage_count = 0
                logging.info("Auto scale-in triggered.")
                aws_marathon.scale_in(marathon_app)

        timer()
