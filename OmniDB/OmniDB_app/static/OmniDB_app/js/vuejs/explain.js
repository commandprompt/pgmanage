function vueExplain(div_id, query, plan){
  const {createApp} = Vue;

  const app = createApp({
      data() {
        return {
          plan: plan,
          query: query,
        }
      },
    })
  app.component("pev2", pev2.Plan);
  app.mount(`#${div_id}`)};