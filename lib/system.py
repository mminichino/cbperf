##
##

import platform
import os


class sys_info(object):

    def __init__(self):
        self.os_type = platform.system()

    def get_proc_fs(self, parameter):
        data = None

        path_prefix = '/proc/sys/'
        path_suffix = parameter.replace('.','/')
        search_path = path_prefix + path_suffix

        try:
            with open(search_path, 'r') as proc_file:
                line = proc_file.read()
                data = line.split()
            proc_file.close()
        except Exception:
            pass

        return data

    def get_mac_sysctl(self, parameter):
        value = None

        for line in os.popen('sysctl -a'):
            line = line.strip()
            if line.startswith(parameter):
                value = line.split(':')[-1]
                value = value.lstrip()

        return value

    def get_net_buffer(self):
        value = None

        if self.os_type == 'Linux':
            value = self.get_proc_fs('net.ipv4.tcp_wmem')[-1]
        elif self.os_type == 'Darwin':
            value = self.get_mac_sysctl('kern.ipc.maxsockbuf')

        if value:
            return int(value)
        else:
            return None
