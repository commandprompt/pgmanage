import { setActivePinia, createPinia } from "pinia";
import { describe, it, expect, beforeEach, vi } from "vitest";
import axios from "axios";
import { useConnectionsStore } from "@/stores/connections";
import { tabsStore } from "@/stores/stores_initializer";
import { flushPromises } from "@vue/test-utils";

vi.mock("@/stores/stores_initializer", () => {
  const tabsStore = vi.fn();
  return { tabsStore };
});

vi.mock("axios");

describe("connections store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.clearAllMocks();
  });

  it("initializes with default state", () => {
    const store = useConnectionsStore();
    expect(store.connections).toEqual([]);
    expect(store.groups).toEqual([]);
    expect(store.changeActiveDatabaseCallList).toEqual([]);
    expect(store.changeActiveDatabaseCallRunning).toBe(false);
  });

  it("filters remote terminals using remote_terminals getter", () => {
    const store = useConnectionsStore();
    store.connections = [
      { id: 1, technology: "terminal" },
      { id: 2, technology: "other" },
    ];

    expect(store.remote_terminals).toEqual([{ id: 1, technology: "terminal" }]);
  });

  it("gets a connection by id", () => {
    const store = useConnectionsStore();
    store.connections = [{ id: 1, name: "Test Connection" }];

    const connection = store.getConnection(1);
    expect(connection).toEqual({ id: 1, name: "Test Connection" });
  });

  it("updates a connection with new data", () => {
    const store = useConnectionsStore();
    store.connections = [{ id: 1, name: "Old Connection" }];

    store.updateConnection(1, { name: "Updated Connection" });
    expect(store.connections[0]).toEqual({ id: 1, name: "Updated Connection" });
  });

  it("selects a connection and creates a terminal tab if technology is terminal", () => {
    const store = useConnectionsStore();
    const createTerminalTab = vi.fn();
    tabsStore.createTerminalTab = createTerminalTab;
    const connection = {
      id: 1,
      alias: "Test",
      technology: "terminal",
      tunnel: { user: "user", server: "server", port: 22 },
    };

    store.selectConnection(connection);
    expect(createTerminalTab).toHaveBeenCalledWith(
      connection.id,
      connection.alias,
      "user@server:22"
    );
  });

  it("selects a connection and creates a connection tab if technology is not terminal", () => {
    const store = useConnectionsStore();
    const createConnectionTab = vi.fn();
    tabsStore.createConnectionTab = createConnectionTab;
    const connection = { id: 1, technology: "other" };

    store.selectConnection(connection);
    expect(createConnectionTab).toHaveBeenCalledWith(connection.id);
  });

  it("queues and processes database change requests", async () => {
    const store = useConnectionsStore();
    axios.post.mockResolvedValue({ data: "success" });

    store.queueChangeActiveDatabaseThreadSafe({ db: "test" });
    expect(store.changeActiveDatabaseCallList).toHaveLength(0);
    expect(store.changeActiveDatabaseCallRunning).toBe(true);

    await flushPromises();

    expect(store.changeActiveDatabaseCallRunning).toBe(false);
    expect(store.changeActiveDatabaseCallList).toHaveLength(0);
  });

  it("handles database change request errors", async () => {
    const store = useConnectionsStore();
    axios.post.mockRejectedValue(new Error("Network Error"));

    store.queueChangeActiveDatabaseThreadSafe({ db: "test" });

    await flushPromises();

    expect(store.changeActiveDatabaseCallRunning).toBe(false);
  });

  it("tests a connection and returns response", async () => {
    const store = useConnectionsStore();
    const mockResponse = { data: "success" };
    axios.post.mockResolvedValue(mockResponse);

    const response = await store.testConnection({ id: 1 });
    expect(response).toEqual(mockResponse);
  });

  it("handles errors during connection test", async () => {
    const store = useConnectionsStore();
    axios.post.mockRejectedValue(new Error("Network Error"));

    await expect(store.testConnection({ id: 1 })).rejects.toThrow(
      "Network Error"
    );
  });
});
