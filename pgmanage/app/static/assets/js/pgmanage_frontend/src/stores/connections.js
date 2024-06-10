import { defineStore } from "pinia";
import axios from "axios";
import moment from "moment";
import { tabsStore } from "./stores_initializer";

const useConnectionsStore = defineStore({
  id: "connections",
  state: () => ({
    connections: [],
    groups: [],
    changeActiveDatabaseCallList: [],
    changeActiveDatabaseCallRunning: false,
  }),
  getters: {
    remote_terminals: (state) =>
      state.connections.filter((conn) => conn.technology === "terminal"),
  },
  actions: {
    getConnection(conn_id) {
      return this.connections.find((conn) => conn.id === conn_id);
    },
    updateConnection(conn_id, data) {
      let con = this.getConnection(conn_id);
      if (!con) return;
      Object.assign(con, { ...data });
    },
    selectConnection(connection) {
      connection.last_access_date = moment.now();
      if (connection.technology === "terminal") {
        let details = `${connection.tunnel.user}@${connection.tunnel.server}:${connection.tunnel.port}`;
        tabsStore.createTerminalTab(connection.id, connection.alias, details);
      } else {
        tabsStore.createConnectionTab(connection.id);
      }
    },
    queueChangeActiveDatabaseThreadSafe(data) {
      this.changeActiveDatabaseCallList.push(data);
      if (!this.changeActiveDatabaseCallRunning) {
        this.changeActiveDatabaseThreadSafe(
          this.changeActiveDatabaseCallList.pop()
        );
      }
    },
    changeActiveDatabaseThreadSafe(data) {
      this.changeActiveDatabaseCallRunning = true;
      axios
        .post("/change_active_database/", data)
        .then((resp) => {
          this.changeActiveDatabaseCallRunning = false;
          if (this.changeActiveDatabaseCallList.length > 0)
            this.changeActiveDatabaseThreadSafe(
              this.changeActiveDatabaseCallList.pop()
            );
        })
        .catch((error) => {
          console.log(error);
        });
    },
    async testConnection(connection) {
      try {
        const response = await axios.post("/test_connection/", connection);
        return response;
      } catch (error) {
        throw error;
      }
    },
  },
});

export { useConnectionsStore };
