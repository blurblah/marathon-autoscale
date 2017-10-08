
import sys
import requests
import json
import math
import logging


class Marathon(object):
    def __init__(self, endpoint, auth_id=None, auth_password=None):
        self.uri = endpoint
        self.id = auth_id
        self.password = auth_password
        self.apps = self.get_all_apps()

    def get_event_callbacks(self):
        callbacks = dict()
        response = requests.get(self.uri + '/v2/eventSubscriptions', auth=(self.id, self.password))
        if response.status_code == 200:
            callbacks = response.json()

        return callbacks

    def register_event_callback(self, callback):
        registered_callbacks = dict()
        data = {'callbackUrl': callback}
        response = requests.post(self.uri + '/v2/eventSubscriptions', auth=(self.id, self.password), params=data)
        if response.status_code == 200:
            logging.info("Event callback registered.")
            registered_callbacks = self.get_event_callbacks()
            logging.info(str(registered_callbacks))

        return registered_callbacks

    def unregister_event_callback(self, callback):
        registered_callbacks = dict()
        data = {'callbackUrl': callback}
        response = requests.delete(self.uri + '/v2/eventSubscriptions', auth=(self.id, self.password), params=data)
        if response.status_code == 200:
            logging.info("Event callback unregistered.")
            registered_callbacks = self.get_event_callbacks()
            logging.info(str(registered_callbacks))

        return registered_callbacks

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

    def get_app_instances(self, marathon_app):
        response = requests.get(self.uri + '/v2/apps/'+ marathon_app, auth=(self.id, self.password), verify=False).json()
        if 'app' not in response or response['app']['tasks'] == []:
            logging.info('No task data on Marathon for App ! %s' % marathon_app)
            return 0
        else:
            return response['app']['instances']

    def scale_out(self, marathon_app, autoscale_multiplier, max_instances):
        if self.appinstances == max_instances:
            logging.info("Already reached the set maximum instances of %d" % self.appinstances)
            return

        target_instances_float=self.appinstances * autoscale_multiplier
        target_instances=math.ceil(target_instances_float)
        if target_instances > max_instances:
            logging.info("Reached the set maximum instances of %d" % max_instances)
            target_instances = max_instances

        logging.info("[scaleout] start. current instances : %d" % self.appinstances)
        data = {'instances': target_instances}
        json_data = json.dumps(data)
        headers = {'Content-type': 'application/json'}
        response = requests.put(self.uri + '/v2/apps/'+ marathon_app,
                                json_data, headers=headers, auth=(self.id, self.password), verify=False)
        logging.debug(response.text)
        logging.info('Scale out return status code = %d' % response.status_code)

    def scale_in(self, marathon_app):
        target_instances = self.appinstances - 1
        if target_instances < 1:
            logging.info("Reached the set 1 instance.")
        else:
            logging.info("[scalein] start. current instances : %d" % self.appinstances)
            data = {'instances': target_instances}
            json_data = json.dumps(data)
            headers = {'Content-type': 'application/json'}
            response = requests.put(self.uri + '/v2/apps/'+ marathon_app,
                                    json_data, headers=headers, auth=(self.id, self.password), verify=False)
            logging.debug(response.text)
            logging.info('Scale in return status code = %d' % response.status_code)
