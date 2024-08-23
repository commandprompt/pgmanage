import { flushPromises, mount } from "@vue/test-utils";
import UtilityJobDetail from "@/components/UtilityJobsJobDetail.vue";
import { utilityJobStore } from "@/stores/stores_initializer";
import axios from "axios";
import { Modal } from "bootstrap";
import { vi, describe, beforeEach, it, expect, afterEach } from "vitest";
import { nextTick } from "vue";

vi.mock("axios");
vi.mock("bootstrap", () => ({
  Modal: {
    getOrCreateInstance: vi.fn(() => ({
      show: vi.fn(),
    })),
  },
}));

describe("UtilityJobsJobDetail.vue", () => {
  let wrapper, response;

  response = {
    data: {
      data: {
        out: {
          pos: 1,
          lines: [["out", "Test log"]],
          done: true,
        },
        err: {
          pos: 1,
          lines: [["err", "Error log"]],
          done: true,
        },
        duration: "10s",
        exit_code: 0,
      },
    },
  };

  beforeEach(() => {
    wrapper = mount(UtilityJobDetail, {
      shallow: true,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("renders job details in the modal", async () => {
    // Set up the mock data
    utilityJobStore.selectedJob = {
      id: 1,
      type_desc: "Test Job",
      description: "This is a test job",
      details: {
        cmd: "test command",
      },
      start_time: "2024-08-15T12:00:00Z",
      duration: "10s",
    };

    await nextTick();

    expect(wrapper.find(".modal-title").text()).toBe("Test Job");
    expect(wrapper.find(".modal-body").text()).toContain("This is a test job");
    expect(wrapper.find(".modal-body").text()).toContain("test command");
    expect(wrapper.find(".modal-body").text()).toContain("10s");
  });

  it("calls getJobDetails when the modal is shown", async () => {
    axios.get.mockResolvedValue(response);
    const spy = vi.spyOn(wrapper.vm, "getJobDetails");
    wrapper.vm.$refs.jobDetailModal.dispatchEvent(new Event("show.bs.modal"));

    expect(spy).toHaveBeenCalledWith(utilityJobStore.selectedJob.id, 0, 0);
  });

  it("resets logs and other data when the modal is hidden", async () => {
    wrapper.vm.logs = ["Test log"];
    wrapper.vm.autoScroll = false;
    wrapper.vm.detailJobWorkerId = "workerId";
    wrapper.vm.out = 10;
    wrapper.vm.err = 5;

    wrapper.vm.$refs.jobDetailModal.dispatchEvent(new Event("hide.bs.modal"));

    expect(wrapper.vm.logs).toEqual([]);
    expect(wrapper.vm.autoScroll).toBe(true);
    expect(wrapper.vm.detailJobWorkerId).toBe("");
    expect(wrapper.vm.out).toBe(0);
    expect(wrapper.vm.err).toBe(0);
  });

  it("adds logs and scrolls to the bottom on receiving job details", async () => {
    axios.get.mockResolvedValueOnce(response);

    wrapper.vm.getJobDetails(1, 0, 0);

    await flushPromises();

    expect(wrapper.vm.logs).toEqual(["Error log", "Test log"]);
  });

  it("shows the modal when setJob action is triggered", async () => {
    utilityJobStore.setJob({
      id: 1,
      type_desc: "Test Job",
    });
    expect(Modal.getOrCreateInstance).toHaveBeenCalledWith(
      wrapper.vm.$refs.jobDetailModal
    );
  });
});
