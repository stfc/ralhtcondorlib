#!/usr/bin/env python3

import htcondor


# ============================================================================== 
# ancillaries 
# ============================================================================== 

def _build_constraint_str(constraint_l=None):
    """
    builds the contraint string expression for different queries.
    Default is string 'true'.
    :param list constraint_l: list of constraints to be combined
    :return str: 
    """
    if constraint_l:
        constraint_str = " && ".join(constraint_l)
    else:
        constraint_str = "true"
    return constraint_str

# ============================================================================== 
# HTCondor classes
# ============================================================================== 

class HTCondorSchedd(object):
    """
    class to represent a single HTCondor Schedd
    """
    def __init__(self, schedd=None):
        if schedd:
            self.schedd = schedd
        else:
            self.schedd = htcondor.Schedd()

    def condor_q(self, attribute_l, constraint_l=None):
        """
        Returns a list of ClassAd objects, output of a condor_q query.
        :param list attribute_l: list of classads strings to be included 
                                 in the query
        :param list constraint_l: [optional] list of constraints strings 
                                  for the query
        :return list: list of ClassAd objects
        """
        constraint_str = _build_constraint_str(constraint_l)
        jobs = self.schedd.query(constraint_str, attribute_l)
        return jobs

    def condor_history(self, attribute_l, constraint_l=None):
        """
        Returns a list of ClassAd objects, output of a condor_history query.
        :param list attribute_l: list of classads strings to be included 
                                 in the query
        :param list constraint_l: [optional] list of constraints strings 
                                  for the query
        :return list: list of ClassAd objects
        """
        constraint_str = _build_constraint_str(constraint_l)
        jobs = self.schedd.history(constraint_str, attribute_l) 
        # output of history() is an iterator, convert it to list
        # NOTE the MAX number of jobs is defined in config variable HISTORY_HELPER_MAX_HISTORY
        jobs = list(jobs)
        return jobs


class HTCondorScheddList(list):
    """
    list of HTCondorSchedd objects
    """
    def condor_q(self, attribute_l, constraint_l=None):
        """
        Returns a list of ClassAd objects, output of a condor_q query.
        :param list attribute_l: list of classads strings to be included 
                                 in the query
        :param list constraint_l: [optional] list of constraints strings 
                                  for the query
        :return list: list of ClassAd objects
        """
        jobs = []
        for schedd in self.__iter__():
            jobs += schedd.condor_q(attribute_l, constraint_l)
        return jobs

    def condor_history(self, attribute_l, constraint_l=None):
        """
        Returns a list of ClassAd objects, output of a condor_history query.
        :param list attribute_l: list of classads strings to be included 
                                 in the query
        :param list constraint_l: [optional] list of constraints strings 
                                  for the query
        :return list: list of ClassAd objects
        """
        jobs = []
        for schedd in self.__iter__():
            jobs += schedd.condor_history(attribute_l, constraint_l)
        return jobs
        



class HTCondorCollector(object):
    def __init__(self):
        self.collector = htcondor.Collector()

    def get_schedd_l(self):
        """
        returns all Schedds in the pool
        :return HTCondorScheddList:
        """
        schedd_l = HTCondorScheddList()
        schedd_ad_l = self.collector.locateAll(htcondor.DaemonTypes.Schedd)
        for schedd_ad in schedd_ad_l:
            schedd = HTCondorSchedd(htcondor.Schedd(schedd_ad))
            schedd_l.append(schedd)
        return schedd_l
    
    def query(self, htcondortype, attribute_l, constraint_l=None):
        constraint_str = _build_constraint_str(constraint_l)
        out = self.collector.query(htcondortype, constraint_str, attribute_l)
        return out


class HTCondorPool(object):
    def __init__(self):
        self.collector = HTCondorCollector()
        self.schedd_l = self.collector.get_schedd_l()

    def condor_q(self, attribute_l, constraint_l=None):
        jobs = self.schedd_l.condor_q(attribute_l, constraint_l)
        return jobs

    def condor_history(self, attribute_l, constraint_l=None):
        jobs = self.schedd_l.condor_history(attribute_l, constraint_l)
        return jobs

    def condor_status(self, attribute_l, constraint_l=None):
        machines = self.collector.query(htcondor.AdTypes.Startd, attribute_l, constraint_l)
        return machines



# ============================================================================== 
# tests 
# ============================================================================== 

#pool = HTCondorPool()

#jobs = pool.condor_q(['JobStatus', 'x509UserProxyVOName', 'ScheddHostName'])
#print(jobs)

#attr_l = ["Name", "TotalSlotCpus", "Cpus", "TotalSlotMemory", "Memory", "State", "PREEMPTABLE_ONLY", "StartJobs", "NODE_IS_HEALTHY"]
#constr_l = ["PartitionableSlot =?= True"] 
#machines = pool.condor_status(attr_l, constr_l)
#print(len(machines))
#print(machines[0])
