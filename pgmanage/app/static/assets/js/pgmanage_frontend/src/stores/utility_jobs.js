import { defineStore } from "pinia";

const useUtilityJobsStore = defineStore("utilityJob", {
  state: () => ({
    selectedJob: {},
  }),
  actions: {
    clearSelected() {
      this.selectedJob = {};
    },
    setJob(job) {
      this.selectedJob = job;
    },
    setDuration(duration) {
      this.selectedJob.duration = duration;
    },
  },
});

const utilityJobStore = useUtilityJobsStore();

export { utilityJobStore };
