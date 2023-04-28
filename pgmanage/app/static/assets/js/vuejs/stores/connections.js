const pinia = Pinia.createPinia();
Pinia.setActivePinia(pinia);

const useConnectionsStore = Pinia.defineStore({
  id: "connections",
  state: () => ({
    connections: [],
    remote_terminals: [],
    groups: [],
  }),
});

const connectionsStore = useConnectionsStore();
