import { describe, expect, test, vi } from "vitest";
import {
  getAllSnippets,
  buildSnippetContextMenuObjects,
  executeSnippet,
  saveSnippetTextConfirm,
} from "../../src/tree_context_functions/tree_snippets";
import axios from "axios";
import { flushPromises } from "@vue/test-utils";
import { snippetsStore } from "../../src/stores/stores_initializer";
import * as notificatonModule from "../../src/notification_control";
import { emitter } from "../../src/emitter";

vi.mock("axios");

describe("getAllSnippets", () => {
  test("makes get request to get all snippets", async () => {
    const mockedData = {
      files: [{ name: "file1" }, { name: "file2" }],
      folders: [{ name: "folder1" }, { name: "folder2" }],
    };
    const snippetsStoreMock = vi.spyOn(snippetsStore, "$patch");

    // Mock the Axios get method to return a resolved promise with the mocked data
    axios.get.mockResolvedValue({ data: mockedData });

    getAllSnippets();

    await flushPromises();

    expect(snippetsStoreMock).toHaveBeenCalledOnce();
    expect(axios.get).toHaveBeenCalledWith("/get_all_snippets/");
    expect(snippetsStore.files).toEqual([{ name: "file1" }, { name: "file2" }]);
  });

  test("showToast is called on error after request", async () => {
    axios.get.mockRejectedValue(new Error("Some error"));

    const showToastSpy = vi.spyOn(notificatonModule, "showToast");

    getAllSnippets();

    await flushPromises();

    expect(showToastSpy).toHaveBeenCalledOnce();
  });
});

describe("buildSnippetContextMenuObjects", () => {
  const snippetText = "Some snippet text";
  const callback = vi.fn();

  test('should add "New Snippet" to elements array in save mode', () => {
    const mode = "save";
    const object = {
      id: null,
      files: [],
      folders: [],
    };

    const result = buildSnippetContextMenuObjects(
      mode,
      object,
      snippetText,
      callback
    );

    // Assert that "New Snippet" is added to the elements array
    expect(result).toContainEqual(
      expect.objectContaining({
        label: "New Snippet",
        icon: "fas cm-all fa-save",
        onClick: expect.any(Function),
      })
    );
  });

  test("should add files and folders to elements array", () => {
    const mode = "load";
    const object = {
      id: null,
      files: [
        { id: 1, name: "snippet1" },
        { id: 2, name: "snippet2" },
      ],
      folders: [
        {
          id: 3,
          name: "folder1",
          files: [{ id: 3, name: "snippet3" }],
          folders: [],
        },
      ],
    };

    const result = buildSnippetContextMenuObjects(
      mode,
      object,
      snippetText,
      callback
    );

    // Assert that files are added to the elements array
    expect(result).toContainEqual(
      expect.objectContaining({
        label: "snippet1",
        icon: "fas cm-all fa-align-left",
        onClick: expect.any(Function),
      })
    );
    expect(result).toContainEqual(
      expect.objectContaining({
        label: "folder1",
        icon: "fas cm-all fa-folder",
        children: expect.arrayContaining([
          {
            icon: "fas cm-all fa-align-left",
            label: "snippet3",
            onClick: expect.any(Function),
          },
        ]),
      })
    );
  });
});

describe("executeSnippet", () => {
  test("should emit event to snippet editor", async () => {
    const responseData = {
      data: "Snippet Text",
    };

    const v_connTabControl = {
      selectedTab: { tag: { tabControl: { selectedTab: { id: "tab_id" } } } },
    };

    const emitterEmitSpy = vi.spyOn(emitter, "emit");

    // Mock the global variable for testing
    vi.stubGlobal("v_connTabControl", v_connTabControl);

    axios.post.mockResolvedValue({ data: responseData });

    executeSnippet(1);

    await flushPromises();

    expect(emitterEmitSpy).toHaveBeenCalledOnce();
    expect(emitterEmitSpy).toBeCalledWith(
      `${v_connTabControl.selectedTab.tag.tabControl.selectedTab.id}_insert_to_editor`,
      responseData.data
    );
  });

  test("should show toast message on error response", async () => {
    const snippetId = 1;
    const showToastSpy = vi.spyOn(notificatonModule, "showToast");

    axios.post.mockRejectedValue(new Error("Bad request"));

    executeSnippet(snippetId);

    await flushPromises();

    expect(axios.post).toHaveBeenCalledWith("/get_snippet_text/", {
      snippet_id: snippetId,
    });
    expect(showToastSpy).toHaveBeenCalledOnce();
    expect(showToastSpy).toHaveBeenCalledWith(
      "error",
      new Error("Bad request")
    );
  });
});

describe("saveSnippetTextConfirm", () => {
  const saveObject = {
    id: null,
    name: "test snippet",
    parent: null,
  };
  const snippetText = "snippet text";
  const callback = vi.fn();
  const v_connTabControl = { snippet_tag: { tab_id: "test_id" } };

  test("save snippet with correct arguments", async () => {
    vi.stubGlobal("v_connTabControl", v_connTabControl);

    const showToastSpy = vi.spyOn(notificatonModule, "showToast");
    const emitterEmitSpy = vi.spyOn(emitter, "emit");
    const responseData = { data: { parent: 1 } };

    axios.post.mockResolvedValue(responseData);
    axios.get.mockResolvedValue({ data: {} });

    saveSnippetTextConfirm(saveObject, snippetText, callback);

    await flushPromises();

    expect(callback).toHaveBeenCalledOnce();

    expect(emitterEmitSpy).toHaveBeenCalledOnce();
    expect(emitterEmitSpy).toBeCalledWith(
      `${v_connTabControl.snippet_tag.tab_id}_refresh_snippet_tree`,
      responseData.data.parent
    );

    expect(showToastSpy).toHaveBeenCalledOnce();
    expect(showToastSpy).toHaveBeenCalledWith("success", "Snippet saved.");
  });

  test("should show toast message on error response", async () => {
    const showToastSpy = vi.spyOn(notificatonModule, "showToast");

    axios.post.mockRejectedValue(new Error("Bad request"));

    saveSnippetTextConfirm(saveObject, snippetText, callback);

    await flushPromises();

    expect(showToastSpy).toHaveBeenCalledOnce();
    expect(showToastSpy).toHaveBeenCalledWith(
      "error",
      new Error("Bad request")
    );
  });
});
