##
##

import platform


class sys_info(object):

    def __init__(self):
        self.os_type = platform.system()

    def get_proc_fs(self, parameter):
        path_prefix = '/proc/sys/'
        path_suffix = parameter.replace('.','/')
        search_path = path_prefix + path_suffix
        with open(search_path, 'r') as proc_file:
            line = proc_file.read()
            data = line.split()
        proc_file.close()
        return data

    def get_net_buffer(self):
        if self.os_type == 'Linux':
            value = self.get_proc_fs('net.ipv4.tcp_wmem')[-1]
            print(value)
