<template>
  <div class="modal fade" id="jobDetailModal" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h3 class="modal-title">{{ selectedJob?.type_desc }}</h3>
          <button type="button" class="close" data-dismiss="modal" aria-label="Close">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div class="modal-body">
          <p class="mb">{{ selectedJob?.description }}</p>
          <p class="font-weight-bold mb-2">Command:</p>
          <p class="p-2 border border-radius text-break">
            {{ selectedJob?.details?.cmd }}
          </p>
          <div class="d-flex justify-content-between mt-3 mb-2">
            <span>
              <span class="font-weight-bold">Start time:</span>
              {{ selectedJob?.start_time }}
            </span>
            <span>
              <span class="font-weight-bold">Duration:</span>
              {{ selectedJob?.duration }}
            </span>
          </div>
          <div class="d-flex justify-content-between mt-3 mb-2">
            <span class="font-weight-bold">Output:</span>
            <div class="custom-control custom-switch">
              <input class="custom-control-input" type="checkbox" id="jobDetailAutoscroll" v-model="autoScroll" />
              <label class="custom-control-label" for="jobDetailAutoscroll">
                Autoscroll
              </label>
            </div>
          </div>
          <div id="job_detail_output" :style="{ height: '200px', overflowY: 'auto' }" class="border border-radius p-1">
            <p v-for="log in logs">{{ log }}</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { utilityJobStore } from "../stores/stores_initializer";
import axios from "axios";
import $ from "jquery";

export default {
  name: "JobDetail",
  data() {
    return {
      detailJobWorkerId: "",
      logs: [],
      autoScroll: true,
      out: 0,
      err: 0,
    };
  },
  mounted() {
    $("#jobDetailModal").on("hide.bs.modal", () => {
      this.setDefault();
      utilityJobStore.clearSelected();
    });
    $("#jobDetailModal").on("show.bs.modal", () => {
      this.getJobDetails(this.selectedJob.id, this.out, this.err);
    });
    $("#jobDetailModal").on("shown.bs.modal", () => {
      this.scrollToBottom();
    });

    utilityJobStore.$onAction((action) => {
      if (action.name === "setJob") {
        action.after(() => {
          $("#jobDetailModal").modal("show");
        });
      }
    });
  },
  computed: {
    selectedJob() {
      return utilityJobStore.selectedJob;
    },
  },

  methods: {
    getJobDetails(job_id, out, err) {
      axios
        .get(`/bgprocess/${job_id}/${out}/${err}/`)
        .then((resp) => {
          this.out = resp.data.data.out.pos;
          this.err = resp.data.data.err.pos;
          utilityJobStore.setDuration(resp.data.data.duration);
          this.logs.push(
            ...resp.data.data.err.lines.map((l) => l[1]),
            ...resp.data.data.out.lines.map((l) => l[1])
          );

          this.scrollToBottom();
          if (!this.detailJobWorkerId) {
            this.detailJobWorkerId = setInterval(() => {
              if (!!Object.keys(this.selectedJob).length) {
                this.getJobDetails(job_id, this.out, this.err);
              }
            }, 1000);
          }

          if (
            resp.data.data.out.done &&
            resp.data.data.err.done &&
            resp.data.data.exit_code != null
          ) {
            clearInterval(this.detailJobWorkerId);
            this.detailJobWorkerId = "";
          }
        })
        .catch((error) => {
          console.log(error);
        });
    },
    setDefault() {
      this.logs.splice(0);
      this.autoScroll = true;
      this.detailJobWorkerId = "";
      this.out = 0;
      this.err = 0;
    },
    scrollToBottom() {
      this.$nextTick(() => {
        if (this.autoScroll) {
          const lastChild = $("#job_detail_output").children().last()[0];
          if (lastChild) {
            setTimeout(() => {
              lastChild.scrollIntoView({ block: "end" });
            }, 500);
          }
        }
      });
    },
  },
};
</script>
