##
##

import sys
import getpass


class ask(object):

    def __init__(self):
        pass

    def text(self, question):
        """Get text input"""
        print("%s:" % question)
        while True:
            suffix = ' [q=quit]'
            prompt = 'Selection' + suffix + ': '
            answer = input(prompt)
            answer = answer.rstrip("\n")
            if answer == 'q':
                sys.exit(0)
            if len(answer) > 0:
                return answer
            else:
                print("Response can not be empty.")
                continue

    def password(self, question, verify=True):
        while True:
            passanswer = getpass.getpass(prompt=question + ': ')
            passanswer = passanswer.rstrip("\n")
            if verify:
                checkanswer = getpass.getpass(prompt="Re-enter password: ")
                checkanswer = checkanswer.rstrip("\n")
                if passanswer == checkanswer:
                    break
                else:
                    print(" [!] Passwords do not match, please try again ...")
            else:
                break

        return passanswer
