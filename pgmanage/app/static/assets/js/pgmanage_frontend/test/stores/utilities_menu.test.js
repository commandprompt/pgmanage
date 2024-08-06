import { setActivePinia, createPinia } from "pinia";
import { describe, it, expect, beforeEach, vi } from "vitest";
import { useUtilitiesMenuStore } from "@/stores/utilities_menu";
import { settingsStore } from "@/stores/stores_initializer";

vi.mock("@/stores/stores_initializer", () => ({
  settingsStore: {
    desktopMode: false,
  },
}));

describe("utilitiesMenu store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("initializes with an empty items array", () => {
    const store = useUtilitiesMenuStore();
    expect(store.items).toEqual([]);
  });

  it("adds an item when user matches and mode matches", () => {
    window.v_super_user = true;
    settingsStore.desktopMode = true;

    const store = useUtilitiesMenuStore();
    const mockClickFunction = vi.fn();
    store.addItem({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      desktop: true,
      superuserRequired: true,
    });

    expect(store.items).toHaveLength(1);
    expect(store.items[0]).toEqual({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      show: true,
    });
  });

  it("adds an item when user matches and mode does not match", () => {
    window.v_super_user = true;
    settingsStore.desktopMode = false;

    const store = useUtilitiesMenuStore();
    const mockClickFunction = vi.fn();
    store.addItem({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      desktop: true,
      superuserRequired: true,
    });

    expect(store.items).toHaveLength(1);
    expect(store.items[0]).toEqual({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      show: false,
    });
  });

  it("adds an item when user does not match and mode matches", () => {
    window.v_super_user = false;
    settingsStore.desktopMode = true;

    const store = useUtilitiesMenuStore();
    const mockClickFunction = vi.fn();
    store.addItem({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      desktop: true,
      superuserRequired: true,
    });

    expect(store.items).toHaveLength(1);
    expect(store.items[0]).toEqual({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      show: false,
    });
  });

  it("adds an item when user does not match and mode does not match", () => {
    window.v_super_user = false;
    settingsStore.desktopMode = false;

    const store = useUtilitiesMenuStore();
    const mockClickFunction = vi.fn();
    store.addItem({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      desktop: true,
      superuserRequired: true,
    });

    expect(store.items).toHaveLength(1);
    expect(store.items[0]).toEqual({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      show: false,
    });
  });

  it("adds an item when superuser is not required and mode matches", () => {
    window.v_super_user = false;
    settingsStore.desktopMode = true;

    const store = useUtilitiesMenuStore();
    const mockClickFunction = vi.fn();
    store.addItem({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      desktop: true,
      superuserRequired: false,
    });

    expect(store.items).toHaveLength(1);
    expect(store.items[0]).toEqual({
      name: "Test Item",
      icon: "test-icon",
      clickFunction: mockClickFunction,
      show: true,
    });
  });
});
