
from flask import Flask
from flask import request
import config
import requests
import sys
import time
from marathon import Marathon
from apscheduler.schedulers.background import BackgroundScheduler
import logging

app = Flask(__name__)
continuous_low_usage_count = 0
marathon = None


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


@app.route('/')
def root():
    return 'Hello World!'


@app.route('/events', methods=['GET'])
def get_event_callbacks():
    return str(marathon.get_event_callbacks())


@app.route('/events', methods=['POST'])
def register_callback():
    callback = request.args.get('callback')
    logging.info('Callback to register : %s' % callback)
    if callback is None:
        return 'Invalid parameters'
    else:
        return str(marathon.register_event_callback(callback))


@app.route('/events', methods=['DELETE'])
def unregister_callback():
    callback = request.args.get('callback')
    logging.info('Callback to unregister : %s' % callback)
    if callback is None:
        return 'Invalid parameters'
    else:
        return str(marathon.unregister_event_callback(callback))


@app.route('/callback', methods=['POST'])
def callback():
    marathon_app = 'tester1/simplewebapp-test-webapp'
    event_data = request.get_json()
    if (event_data['eventType'] == 'status_update_event') and (event_data['appId'] == '/' + marathon_app):
        logging.debug(event_data)
        if (event_data['taskStatus'] == 'TASK_KILLED') or (event_data['taskStatus'] == 'TASK_FINISHED'):
            logging.info('[scalein] completed. current instances : %d' % marathon.get_app_instances(marathon_app))
        elif event_data['taskStatus'] == 'TASK_STAGING':
            logging.info('[scaleout] progressing')
        elif event_data['taskStatus'] == 'TASK_RUNNING':
            logging.info('[scaleout] completed. current instances : %d' % marathon.get_app_instances(marathon_app))

    return 'ok'


def monitor(marathon_instance, cfg):
    global continuous_low_usage_count
    aws_marathon = marathon_instance
    # Call get_all_apps method for new object created from aws_marathon class and return all apps
    marathon_apps = aws_marathon.get_all_apps()
    logging.info("The following apps exist in Marathon. %s" % marathon_apps)
    # Quick sanity check to test for apps existence in MArathon.
    marathon_app = cfg.target_app
    if cfg.target_app in marathon_apps:
        logging.info("  Found your Marathon App =  %s" % marathon_app)
    else:
        logging.info("  Could not find your App = %s" % marathon_app)
        logging.info('\n\n\n')
        return

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

    logging.info('\n\n\n')


if __name__ == '__main__':
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

    # Initialize the Marathon object
    marathon = Marathon(cfg.marathon_endpoint, cfg.auth_id, cfg.auth_password)
    logging.info("Marathon URI = %s" % marathon.uri)

    scheduler = BackgroundScheduler()
    scheduler.add_job(monitor, 'interval', seconds=15, args=[marathon, cfg])
    scheduler.start()

    app.run(host='0.0.0.0', port=5000)
