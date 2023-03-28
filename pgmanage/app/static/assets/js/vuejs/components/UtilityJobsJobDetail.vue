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
          <p class="p-2 border border-radius text-break">{{ selectedJob?.details?.cmd }}</p>
          <div class="d-flex justify-content-between mt-3 mb-2">
            <span>
              <span class="font-weight-bold">Start time:</span> {{ selectedJob?.start_time }}
            </span>
            <span>
              <span class="font-weight-bold">Duration:</span> {{ selectedJob?.duration }}
            </span>
          </div>
          <div class="d-flex justify-content-between mt-3 mb-2">
            <span class="font-weight-bold">Output:</span>
            <div class="custom-control custom-switch">
              <input class="custom-control-input" type="checkbox" id="jobDetailAutoscroll" v-model="autoScroll">
              <label class="custom-control-label" for="jobDetailAutoscroll">
                Autoscroll
              </label>
            </div>
          </div>
          <div ref="job_detail_output" :style="{ height: '200px', overflowY: 'auto' }" class="border border-radius p-1">
            <p v-for="log in logs"> {{ log }}</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'JobDetail',
  data() {
    return {
      detailJobWorkerId: '',
      autoScrollWorkerId: '',
      logs: [],
      autoScroll: true
    }
  },
  mounted() {
    $('#jobDetailModal').on('hide.bs.modal', () => {
      this.setDefault()
      jobDetailState.clearSelectedAndHide()
      clearInterval(this.detailJobWorkerId)
      clearInterval(this.autoScrollWorkerId)
    })
    $('#jobDetailModal').on('show.bs.modal', () => {
      this.getJobDetails(this.selectedJob.id);
      this.startAutoScrollWorker()
    })
    setInterval(() => {
      if (this.visible && !this.logs.length) {
        $('#jobDetailModal').modal('show');
      }
    }, 1000)
  },
  computed: {
    visible() {
      return jobDetailState.visible
    },
    selectedJob() {
      return jobDetailState.selectedJob
    }
  },

  methods: {
    getJobDetails(job_id, out = 0, err = 0) {
      axios.get(`/bgprocess/${job_id}/${out}/${err}/`)
        .then((resp) => {
          console.log(resp)
          let out_pos = resp.data.data.out.pos
          let err_pos = resp.data.data.err.pos
          jobDetailState.setDuration(resp.data.data.duration)
          this.logs.push(
            ...resp.data.data.err.lines.map((l) => l[1]),
            ...resp.data.data.out.lines.map((l) => l[1]))
          if (!this.detailJobWorkerId) {
            this.detailJobWorkerId = setInterval(() => {
              if (!!Object.keys(this.selectedJob).length) {
                this.getJobDetails(job_id, out_pos, err_pos)
              }
            }, 1000)
          }
          if (resp.data.data.out.done && resp.data.data.err.done && resp.data.data.exit_code != null && this.detailJobWorkerId) {
            clearInterval(this.detailJobWorkerId)
          }
        })
        .catch((error) => {
          console.log(error)
        })
    },
    startAutoScrollWorker() {
      this.autoScrollWorkerId = setInterval(() => {
        if (this.autoScroll) {
          let scrollHeight = this.$refs.job_detail_output.scrollHeight
          this.$refs.job_detail_output.scrollTo({ 'top': scrollHeight, 'behavior': 'smooth' })
        }
      }, 1000)
    },
    setDefault() {
      this.$refs.job_detail_output.scrollTop = 0;
      this.logs.splice(0)
      this.autoScroll = true
      this.detailJobWorkerId = ''
    }
  }
}
</script>