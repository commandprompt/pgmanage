import { defineStore } from "pinia";
import axios from "axios";

const useDbMetadataStore = defineStore("dbMetadata", {
  state: () => ({
    initialized: 'false',
    dbMeta: {}
  }),
  actions: {
    getDbMeta(conn_id, db_name) {
      if(this.dbMeta[conn_id])
        if(this.dbMeta[conn_id][db_name])
          return this.dbMeta[conn_id][db_name]
    },
    async fetchDbMeta(conn_id, workspace_id, db_name) {
      if(this.dbMeta[conn_id])
        if(this.dbMeta[conn_id][db_name])
          return
      const meta_response = await axios.post('/get_database_meta/', {
        database_index: conn_id,
        workspace_id: workspace_id,
        database_name: db_name
      })

      if(!this.dbMeta[conn_id])
        this.dbMeta[conn_id] = {}
      this.dbMeta[conn_id][db_name] = meta_response.data.schemas
      return meta_response
    }
  },
});

export { useDbMetadataStore };