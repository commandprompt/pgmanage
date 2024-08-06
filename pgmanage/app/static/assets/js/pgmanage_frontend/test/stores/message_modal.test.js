import { setActivePinia, createPinia } from "pinia";
import { describe, it, expect, beforeEach, vi } from "vitest";
import { useMessageModalStore } from "@/stores/message_modal";

describe("messageModal store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("initializes with default state", () => {
    const store = useMessageModalStore();
    expect(store.visible).toBe(false);
    expect(store.message).toBe("");
    expect(store.successFunc).toBeInstanceOf(Function);
    expect(store.cancelFunc).toBeInstanceOf(Function);
    expect(store.closable).toBe(true);
    expect(store.checkboxes).toEqual([]);
  });

  it("shows modal and sets the correct state", () => {
    const store = useMessageModalStore();
    const message = "Test message";
    const successFunc = vi.fn();
    const cancelFunc = vi.fn();
    const checkboxes = ["option1", "option2"];

    store.showModal(message, successFunc, cancelFunc, false, checkboxes);

    expect(store.message).toBe(message);
    expect(store.successFunc).toBe(successFunc);
    expect(store.cancelFunc).toBe(cancelFunc);
    expect(store.closable).toBe(false);
    expect(store.checkboxes).toEqual(checkboxes);
    expect(store.visible).toBe(true);
  });

  it("hides modal and resets the state", () => {
    const store = useMessageModalStore();

    store.showModal("Test message", vi.fn(), vi.fn(), false, ["option1"]);
    store.hideModal();

    expect(store.visible).toBe(false);
    expect(store.message).toBe("");
    expect(store.successFunc).toBeInstanceOf(Function);
    expect(store.cancelFunc).toBeInstanceOf(Function);
    expect(store.closable).toBe(true);
    expect(store.checkboxes).toEqual([]);
  });

  it("executes success function and hides modal", () => {
    const store = useMessageModalStore();
    const successFunc = vi.fn();

    store.showModal("Test message", successFunc, vi.fn());
    store.executeSuccess();

    expect(successFunc).toHaveBeenCalled();
    expect(store.visible).toBe(false);
  });

  it("executes cancel function and hides modal", () => {
    const store = useMessageModalStore();
    const cancelFunc = vi.fn();

    store.showModal("Test message", vi.fn(), cancelFunc);
    store.executeCancel();

    expect(cancelFunc).toHaveBeenCalled();
    expect(store.visible).toBe(false);
  });
});
