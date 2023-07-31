const pinia = Pinia.createPinia();
Pinia.setActivePinia(pinia);

const useConnectionsStore = Pinia.defineStore({
  id: "connections",
  state: () => ({
    connections: [],
    groups: [],
  }),
  getters: {
    remote_terminals: (state) =>
      state.connections.filter((conn) => conn.technology === "terminal"),
  },
});

const connectionsStore = useConnectionsStore();

const emitter = mitt()