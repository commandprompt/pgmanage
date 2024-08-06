import { setActivePinia, createPinia } from "pinia";
import { describe, it, expect, beforeEach } from "vitest";
import { useCommandsHistoryStore } from "@/stores/commands_history";

describe("commandsHistoryModal store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("initializes with default state", () => {
    const store = useCommandsHistoryStore();
    expect(store.visible).toBe(false);
    expect(store.tabId).toBe(null);
    expect(store.databaseIndex).toBe(null);
    expect(store.tabType).toBe(null);
  });

  it("shows modal and sets the correct state", () => {
    const store = useCommandsHistoryStore();
    const tabId = 1;
    const databaseIndex = 2;
    const tabType = "Query";

    store.showModal(tabId, databaseIndex, tabType);

    expect(store.visible).toBe(true);
    expect(store.tabId).toBe(tabId);
    expect(store.databaseIndex).toBe(databaseIndex);
    expect(store.tabType).toBe(tabType);
  });

  it("resets the state to default values", () => {
    const store = useCommandsHistoryStore();

    store.showModal(1, 2, "Query");
    store.reset();

    expect(store.visible).toBe(false);
    expect(store.tabId).toBe(null);
    expect(store.databaseIndex).toBe(null);
    expect(store.tabType).toBe(null);
  });
});
