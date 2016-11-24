
import os
import configparser


class Configuration:

    marathon_endpoint = ''
    cpu_threshold = 0
    mem_threshold = 0
    multiplier = 0.0
    target_app = ''
    max_size = 0

    def load(self, config_file):
        if not os.path.exists(config_file):
            raise Exception('%s No such file or directory' % config_file)

        config = configparser.ConfigParser()
        config.read(config_file)

        self.marathon_endpoint = config.get('Marathon', 'endpoint')
        self.cpu_threshold = config.getint('Autoscale', 'cpu_threshold')
        self.mem_threshold = config.getint('Autoscale', 'mem_threshold')
        self.multiplier = config.getfloat('Autoscale', 'multiplier')
        self.target_app = config.get('Autoscale', 'target_app')
        self.max_size = config.getint('Autoscale', 'max_size')
