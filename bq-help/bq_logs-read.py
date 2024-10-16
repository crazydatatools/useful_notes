from dataclasses import dataclass

@dataclass
class BQJobProfile:

    job: object
    user: str
#https://medium.com/towards-data-science/how-i-build-a-real-time-bigquery-pipeline-for-cost-saving-and-capacity-planning-15712c97f058
    def __post_init__(self):
        self.job_id = self.job["jobName"]["jobId"]
        self.location = self.job["jobName"]["location"]
        self.project_id = self.job["jobName"]["projectId"]
        self.query = self.job["jobConfiguration"]["query"]["query"].replace("\n", " ")
        self.cost = self._calculate_cost(self.job)
        self.duration = self._calculate_duration(self.job)
        self.slot = self._calculate_slot(self.job, self.duration)

    def _calculate_slot(self, job, duration):
        """Calculate query BQ slot usage"""
        # totalSlotMs is the Slot-milliseconds for the job.
        # totalSlotMs = num of used slots * duration of the query
        # ref:https://cloud.google.com/blog/topics/developers-practitioners/monitoring-bigquery-reservations-and-slot-utilization-information_schema
        return (int(job["jobStatistics"]["totalSlotMs"]) / 1000 / (duration * 60))

    def _calculate_cost(self, job):
        """Calculate query cost in USD"""
        return round((int(job["jobStatistics"]["totalBilledBytes"]) / 2**40) * TERA_BYTES_COST, 2)

    def _calculate_duration(self, job):
        """Calculate query duration in minutes"""
        return round((parser.parse(job["jobStatistics"]["endTime"]) - parser.parse(job["jobStatistics"]["startTime"])).total_seconds() / 60, 2)
 
  job_raw = data["protoPayload"]["serviceData"]["jobCompletedEvent"]["job"]
  user = data["protoPayload"]["authenticationInfo"]["principalEmail"]
  job = BQJobProfile(job=job_raw, user=user)



