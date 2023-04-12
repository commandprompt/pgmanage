const jobDetailModal = createApp({
  components: {
    "job-detail": Vue.defineAsyncComponent(() =>
      loadModule(
        "../static/assets/js/vuejs/components/UtilityJobsJobDetail.vue",
        options
      )
    ),
  },
});

jobDetailModal.mount("#utility-job-detail-wrap");
