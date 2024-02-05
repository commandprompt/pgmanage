import { mount } from "@vue/test-utils";
import mixin from "../../src/mixins/file_input_mixin";
import { describe, test, expect, vi, beforeEach } from "vitest";
import * as notificatonModule from "../../src/notification_control";
import { maxFileSizeInKB, maxFileSizeInMB } from "../../src/constants";

describe("File Input Mixin", () => {
  let wrapper;

  beforeEach(() => {
    wrapper = mount({
      mixins: [mixin],
      template: `<div></div>`,
    });
  });
  test("handles valid file type", () => {
    const fileReaderSpy = vi
      .spyOn(FileReader.prototype, "readAsText")
      .mockImplementation(() => null);

    const fileMock = new File(["content"], "example.sql", {
      type: "application/sql",
    });

    const eventMock = { target: { files: [fileMock] } };

    wrapper.vm.handleFileInputChange(eventMock);

    expect(fileReaderSpy).toHaveBeenCalledWith(eventMock.target.files[0]);
  });

  test("handles invalid file type", () => {
    const showToastSpy = vi.spyOn(notificatonModule, "showToast");
    const fileMock = new File(["content"], "example.jpg", {
      type: "image/jpeg",
    });
    const eventMock = { target: { files: [fileMock] } };
    wrapper.vm.handleFileInputChange(eventMock);
    expect(showToastSpy).toHaveBeenCalledWith(
      "error",
      "File with type 'image/jpeg' is not supported."
    );
  });

  test("handles file size within limit", () => {
    const fileReaderSpy = vi
      .spyOn(FileReader.prototype, "readAsText")
      .mockImplementation(() => null);
    const fileMock = new File(["content"], "example.txt", {
      type: "text/plain",
    });
    Object.defineProperty(fileMock, "size", { value: 1024 }); // 1KB
    const eventMock = { target: { files: [fileMock] } };
    wrapper.vm.handleFileInputChange(eventMock);

    expect(fileReaderSpy).toHaveBeenCalledWith(eventMock.target.files[0]);
    expect(fileReaderSpy).toBeCalledTimes(1);
  });

  test("handles file size exceeds limit", () => {
    const showToastSpy = vi.spyOn(notificatonModule, "showToast");
    const fileMock = new File(["content"], "example.txt", {
      type: "text/plain",
    });
    Object.defineProperty(fileMock, "size", { value: maxFileSizeInKB + 1024 });
    const eventMock = { target: { files: [fileMock] } };
    wrapper.vm.handleFileInputChange(eventMock);
    expect(showToastSpy).toHaveBeenCalledWith(
      "error",
      `Please select a file that is ${maxFileSizeInMB}MB or less.`
    );
  });

  test("handles exceptions", () => {
    const showToastSpy = vi.spyOn(notificatonModule, "showToast");
    const preventDefault = vi.fn();
    const stopPropagation = vi.fn();
    const error = new Error("An error occurred");
    window.FileReader = vi.fn(() => ({
      readAsText: vi.fn(() => {
        throw error;
      }),
    }));

    const fileMock = new File(["content"], "example.txt", {
      type: "text/plain",
    });
    const eventMock = {
      target: { files: [fileMock] },
      preventDefault,
      stopPropagation,
    };
    wrapper.vm.handleFileInputChange(eventMock);

    expect(showToastSpy).toHaveBeenCalledWith("error", error);
    expect(preventDefault).toHaveBeenCalled();
    expect(stopPropagation).toHaveBeenCalled();
  });
});
