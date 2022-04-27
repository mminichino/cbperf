#!/usr/bin/env -S python3 -W ignore

from lib import system


def main():
    sy = system.sys_info()
    sy.get_net_buffer()


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
