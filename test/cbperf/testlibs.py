##
##

import types
import re
import io
import datetime

class CheckCompare(object):

    def __init__(self):
        self.num = False
        self.chr = False
        self.bln = False
        self.tim = False
        self.low = 0
        self.high = 0
        self.chars = 0
        self.p = None
        self.bv = False

    def num_range(self, l: int, h: int):
        self.num = True
        self.chr = False
        self.bln = False
        self.tim = False
        self.low = l
        self.high = h

    def pattern(self, s):
        self.num = False
        self.chr = True
        self.bln = False
        self.tim = False
        self.p = re.compile(s)

    def boolean(self):
        self.num = False
        self.chr = False
        self.bln = True
        self.tim = False

    def time(self):
        self.num = False
        self.chr = False
        self.bln = False
        self.tim = True

    def check(self, v):
        if isinstance(v, types.GeneratorType):
            v = next(v)
        if self.num:
            if self.low <= int(v) <= self.high:
                return True
            else:
                return False
        elif self.chr:
            if self.p.match(v):
                return True
            else:
                return False
        elif self.bln:
            if type(v) == bool:
                return True
            else:
                return False
        elif self.tim:
            if isinstance(v, datetime.datetime):
                return True
            else:
                return False

    def read_file(self, filename):
        with open(filename, "r") as file:
            data = file.read()
            file.close()
            return data

    def clear_file(self, filename):
        file = open(filename, "w")
        file.close()

    def check_host_map(self, output):
        buffer = io.StringIO(output)
        line = buffer.readline()
        p = re.compile(r"^.*Cluster Host List.*$")
        if not p.match(line):
            return False
        p = re.compile(r"^ \[[0-9]+\] .*$")
        for line in buffer.readlines():
            if not p.match(line):
                print(f"Unexpected output: {line}")
                return False
        return True

    def check_run_output(self, output):
        success_found = False
        failure_found = 0

        buffer = io.StringIO(output)
        line = buffer.readline()
        while line:
            p = re.compile("Beginning [a]*sync .* test with [0-9]+ instances")
            if p.match(line):
                success_found = False
                result_line = buffer.readline()
                while result_line:
                    p = re.compile(".*[0-9]+ of [0-9]+, [0-9]+%")
                    if p.match(result_line):
                        result = re.search("[0-9]+ of [0-9]+, [0-9]+%", result_line)
                        percentage = result.group(0).split(',')[1].lstrip().rstrip("%")
                        if int(percentage) != 100:
                            failure_found += 1
                        error_line = buffer.readline()
                        while error_line:
                            p = re.compile("^[0-9]+ errors.*$")
                            if p.match(error_line):
                                result = re.search("^[0-9]+ errors.*$", error_line)
                                err_num = result.group(0).split(' ')[0].strip()
                                if int(err_num) != 0:
                                    failure_found += 1
                                success_found = True
                                break
                            error_line = buffer.readline()
                    if success_found:
                        break
                    result_line = buffer.readline()
            line = buffer.readline()
        if failure_found == 0 and success_found:
            return True
        else:
            print(f"{failure_found} failures ", end='')
            return False
