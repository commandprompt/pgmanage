import { setActivePinia, createPinia } from "pinia";
import { describe, it, expect, beforeEach, vi } from "vitest";
import { useFileManagerStore } from "@/stores/file_manager";

describe("fileManager store", () => {
  let fileManagerStore;

  beforeEach(() => {
    setActivePinia(createPinia());
    fileManagerStore = useFileManagerStore();
    vi.clearAllMocks();
  });

  it("initializes with default state", () => {
    expect(fileManagerStore.onChange).toBeInstanceOf(Function);
    expect(fileManagerStore.dialogType).toBeNull();
    expect(fileManagerStore.file).toBeNull();
    expect(fileManagerStore.visible).toBe(false);
    expect(fileManagerStore.desktopMode).toBeNull();
  });

  it("shows the modal with provided parameters", () => {
    const onChange = vi.fn();
    const dialogType = "info";
    const desktopMode = true;

    fileManagerStore.showModal(desktopMode, onChange, dialogType);

    expect(fileManagerStore.visible).toBe(true);
    expect(fileManagerStore.desktopMode).toBe(desktopMode);
    expect(fileManagerStore.onChange).toBe(onChange);
    expect(fileManagerStore.dialogType).toBe(dialogType);
  });

  it("changes the file", () => {
    const newFile = { name: "test file", path: "/new/path/to/file" };
    fileManagerStore.changeFile(newFile);
    expect(fileManagerStore.file).toStrictEqual(newFile);
  });

  it("hides the modal and resets the state", () => {
    fileManagerStore.showModal(true, vi.fn(), "info");
    fileManagerStore.changeFile("/path/to/file");
    fileManagerStore.hideModal();

    expect(fileManagerStore.visible).toBe(false);
    expect(fileManagerStore.file).toBeNull();
    expect(fileManagerStore.dialogType).toBeNull();
    expect(fileManagerStore.desktopMode).toBeNull();
    expect(fileManagerStore.onChange).toBeInstanceOf(Function);
    expect(fileManagerStore.onChange).toEqual(expect.any(Function)); // Check that onChange is reset to a function
  });

  it("resets the store state", () => {
    fileManagerStore.showModal(true, vi.fn(), "info");
    fileManagerStore.changeFile("/path/to/file");
    fileManagerStore.reset();

    expect(fileManagerStore.onChange).toBeInstanceOf(Function);
    expect(fileManagerStore.dialogType).toBeNull();
    expect(fileManagerStore.file).toBeNull();
    expect(fileManagerStore.desktopMode).toBeNull();
  });
});
