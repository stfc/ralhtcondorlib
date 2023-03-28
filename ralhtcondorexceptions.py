#!/usr/bin/env python3


class CollectorNotReachable(Exception):
    def __init__(self):
        self.value = "Collector not reachable"
    def __str__(self):
        return repr(self.value)


class ScheddNotReachable(Exception):
    def __init__(self):
        self.value = "Schedd not reachable"
    def __str__(self):
        return repr(self.value)


class IncorrectInputType(Exception):
    def __init__(self, name, type):
        self.value = 'Input option %s is not type %s' %(name, type)
    def __str__(self):
        return repr(self.value)

