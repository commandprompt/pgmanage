import { setActivePinia, createPinia } from "pinia";
import { describe, it, expect, beforeEach } from "vitest";
import { useCellDataModalStore } from "@/stores/celldata_modal";

describe("cellDataModal store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("initializes with default state", () => {
    const store = useCellDataModalStore();
    expect(store.visible).toBe(false);
    expect(store.cellContent).toBe(null);
  });

  it("shows modal and sets the correct state", () => {
    const store = useCellDataModalStore();
    const cellContent = "Sample cell content";

    store.showModal(cellContent);

    expect(store.visible).toBe(true);
    expect(store.cellContent).toBe(cellContent);
  });

  it("hides modal and resets the state", () => {
    const store = useCellDataModalStore();

    store.showModal("Sample cell content");
    store.hideModal();

    expect(store.visible).toBe(false);
    expect(store.cellContent).toBe(null);
  });
});
