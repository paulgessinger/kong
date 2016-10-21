from __future__ import print_function
import logging
import sys

from termcolor import colored

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        color = "white"
    
        level = record.levelno

        if level == 20:
            color = "green"
        elif level == 30:
            color = "yellow"
        elif level >= 40:
            color = "red"

        str = "[ {} : {} ] {}".format(record.name, colored(record.levelname, color), colored(record.msg % record.args, color))

        # if level <= 30:
            # sys.stdout.write(str)
        # else:
            # sys.stderr.write(str)

        return str


logger = logging.getLogger("kong")
handler = logging.StreamHandler()
logger.addHandler(handler)

handler.setFormatter(ColoredFormatter())



