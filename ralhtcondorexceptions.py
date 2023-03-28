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


class EmptySubmitFile(Exception):
    def __init__(self):
        self.value = "submit file is emtpy"
    def __str__(self):
        return repr(self.value)

class MalformedSubmitFile(Exception):
    def __init__(self, line):
        self.value = 'line %s in submit file does not have the right format'
    def __str__(self):
        return repr(self.value)


class IncorrectInputType(Exception):
    def __init__(self, name, type):
        self.value = 'Input option %s is not type %s' %(name, type)
    def __str__(self):
        return repr(self.value)


class NegativeSubmissionNumber(Exception):
    def __init__(self, value):
        self.value = "Negative number of jobs to submit: %s" %value
    def __str__(self):
        return repr(self.value)


class ErrorReadingSubmitFile(Exception):
    def __init__(self, value):
        self.value = "Unable to read the submit file %s" %value
    def __str__(self):
        return repr(self.value)


class ErrorWritingSubmitFile(Exception):
    def __init__(self, value):
        self.value = "Unable to write the submit file %s" %value
    def __str__(self):
        return repr(self.value)

