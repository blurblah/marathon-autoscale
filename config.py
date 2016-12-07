
import os
import configparser


class Configuration:

    marathon_endpoint = ''
    auth_id = ''
    auth_password = ''
    out_cpu_threshold = 0
    out_mem_threshold = 0
    in_cpu_threshold = 0
    in_mem_threshold = 0
    multiplier = 0.0
    target_app = ''
    max_size = 0

    def load(self, config_file):
        if not os.path.exists(config_file):
            raise Exception('%s No such file or directory' % config_file)

        config = configparser.ConfigParser()
        config.read(config_file)

        self.marathon_endpoint = config.get('Marathon', 'endpoint')
        self.auth_id = config.get('Marathon', 'auth_id')
        self.auth_password = config.get('Marathon', 'auth_password')

        self.out_cpu_threshold = config.getint('Autoscale', 'out_cpu_threshold')
        self.out_mem_threshold = config.getint('Autoscale', 'out_mem_threshold')
        self.in_cpu_threshold = config.getint('Autoscale', 'in_cpu_threshold')
        self.in_mem_threshold = config.getint('Autoscale', 'in_mem_threshold')

        self.multiplier = config.getfloat('Autoscale', 'multiplier')
        self.target_app = config.get('Autoscale', 'target_app')
        self.max_size = config.getint('Autoscale', 'max_size')
