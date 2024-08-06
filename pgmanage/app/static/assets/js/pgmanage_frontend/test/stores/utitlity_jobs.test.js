import { setActivePinia, createPinia } from "pinia";
import { describe, it, expect, beforeEach } from "vitest";
import { useUtilityJobsStore } from "@/stores/utility_jobs";

describe("utilityJobs store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("initializes with an empty selectedJob object", () => {
    const store = useUtilityJobsStore();
    expect(store.selectedJob).toEqual({});
  });

  it("clears the selectedJob object", () => {
    const store = useUtilityJobsStore();
    store.selectedJob = { id: 1, name: "Test Job" };

    store.clearSelected();
    expect(store.selectedJob).toEqual({});
  });

  it("sets the selectedJob object", () => {
    const store = useUtilityJobsStore();
    const job = { id: 1, name: "Test Job" };

    store.setJob(job);
    expect(store.selectedJob).toEqual(job);
  });

  it("sets the duration of the selectedJob object", () => {
    const store = useUtilityJobsStore();
    const job = { id: 1, name: "Test Job", duration: 0 };

    store.setJob(job);
    store.setDuration(120);
    expect(store.selectedJob.duration).toBe(120);
  });
});
