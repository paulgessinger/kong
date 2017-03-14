from __future__ import print_function
import logging
import sys

from termcolor import colored

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        color = "blue"
    
        level = record.levelno

        if level == 20:
            color = "green"
        elif level == 30:
            color = "yellow"
        elif level >= 40:
            color = "red"
    
        if color != "":
            pf = lambda s: colored(s, color)
        else:
            pf = lambda s:s


        str = "[ {} : {} ] {}".format(record.name, pf(record.levelname), pf(record.msg % record.args))

        # if level <= 30:
            # sys.stdout.write(str)
        # else:
            # sys.stderr.write(str)

        return str


logger = logging.getLogger("kong")
handler = logging.StreamHandler()
logger.addHandler(handler)

handler.setFormatter(ColoredFormatter())



