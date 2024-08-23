import { flushPromises, mount } from "@vue/test-utils";
import UtilityJobs from "@/components/UtilityJobs.vue";
import axios from "axios";
import { describe, it, expect, beforeEach, vi } from "vitest";
import moment from "moment";

vi.mock("axios");

describe("UtilityJobs.vue", () => {
  let wrapper;

  beforeEach(() => {
    axios.get.mockResolvedValue({
      data: {
        data: [
          {
            id: 1,
            utility_pid: 12345,
            details: {
              type: "Backup",
              server: "Server1",
              object: "Database1",
            },
            start_time: "2024-08-01T12:00:00Z",
            process_state: 1,
            duration: "5 mins",
          },
        ],
      },
    });

    wrapper = mount(UtilityJobs, {
      global: {
        mocks: {
          $toast: {
            success: vi.fn(),
            error: vi.fn(),
          },
        },
      },
      shallow: true,
    });
  });

  it("renders job list with correct data", async () => {
    await wrapper.vm.getJobList();

    const jobItems = wrapper.findAll(".list-group-item");

    expect(jobItems.length).toBe(2);

    const jobItem = jobItems[1];

    expect(jobItem.find("div:nth-of-type(1)").text()).toBe("12345");
    expect(jobItem.find("div:nth-of-type(2)").text()).toBe("Backup");
    expect(jobItem.find("div:nth-of-type(3)").text()).toBe("Server1");
    expect(jobItem.find("div:nth-of-type(4)").text()).toBe("Database1");
    expect(jobItem.find("div:nth-of-type(5)").text()).toBe(
      moment("2024-08-01T12:00:00Z").format()
    );
    expect(jobItem.find("div:nth-of-type(7)").text()).toBe("5 mins");
  });

  it("displays correct status icon based on job status", async () => {
    await wrapper.vm.getJobList();

    const jobItem = wrapper.findAll(".list-group-item")[1];
    const statusIcon = jobItem.find("i");

    expect(statusIcon.classes()).toContain("fa-hourglass");
    expect(statusIcon.classes()).toContain("text-info");
    expect(statusIcon.attributes("title")).toBe("Running");
  });

  it("calls stopJob and updates job status", async () => {
    axios.post.mockResolvedValue({});

    await wrapper.vm.getJobList();

    wrapper.vm.stopJob(1);

    expect(wrapper.vm.jobList[0].process_state).toBe(10); // PROCESS_TERMINATING

    await flushPromises();

    expect(axios.post).toHaveBeenCalledWith("/bgprocess/stop/1/");
    expect(wrapper.vm.jobList[0].process_state).toBe(3); // PROCESS_TERMINATED
  });

  it("calls deleteJob and removes job from list", async () => {
    axios.post.mockResolvedValue({});

    await wrapper.vm.getJobList();

    vi.clearAllMocks();
    axios.get.mockResolvedValue({
      data: {
        data: [],
      },
    });

    wrapper.vm.deleteJob(1);

    await flushPromises();

    expect(axios.post).toHaveBeenCalledWith("/bgprocess/delete/1/");
    expect(wrapper.vm.jobList.length).toBe(0);
  });
  it("fetches job list from API and populates jobList", async () => {
    await wrapper.vm.getJobList();

    expect(wrapper.vm.jobList.length).toBe(1);
    expect(wrapper.vm.jobList[0].utility_pid).toBe(12345);
  });

  it("evaluates job process state correctly", () => {
    const job = {
      process_state: 1, // PROCESS_STARTED
      end_time: "2024-08-01T12:05:00Z",
      exit_code: 0,
    };

    const evaluatedState = wrapper.vm.evaluateProcessState(job);

    expect(evaluatedState).toBe(2); // PROCESS_FINISHED
  });

  it("sends correct notification when job finishes", () => {
    const onClickProcess = vi.fn();
    wrapper.vm.sendNotifyJobFinished("Job description", 2, onClickProcess);

    expect(wrapper.vm.$toast.success).toHaveBeenCalled();
  });

  it("calls startWorker on mount", () => {
    wrapper.unmount();
    const startWorkerSpy = vi.spyOn(UtilityJobs.methods, "startWorker");
    wrapper = mount(UtilityJobs, {
      shallow: true,
    });

    expect(startWorkerSpy).toBeCalled();
  });
});
