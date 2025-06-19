import { beforeAll, describe, expect, test, vi } from "vitest";
import {
  buildSnippetContextMenuObjects,
  executeSnippet,
} from "../../src/tree_context_functions/tree_snippets";
import axios from "axios";
import { flushPromises } from "@vue/test-utils";
import { emitter } from "../../src/emitter";
import { useTabsStore } from "../../src/stores/tabs";
import { handleError } from "@/logging/utils";

vi.hoisted(() => {
  vi.stubGlobal("v_csrf_cookie_name", "test_cookie");
  vi.stubGlobal("app_base_path", "test_folder");
});
vi.mock("@/logging/utils", () => ({
  handleError: vi.fn(),
}));

vi.mock("axios");

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
        icon: "fas fa-save",
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
        icon: "fas fa-align-left",
        onClick: expect.any(Function),
      })
    );
    expect(result).toContainEqual(
      expect.objectContaining({
        label: "folder1",
        icon: "fas fa-folder",
        children: expect.arrayContaining([
          {
            icon: "fas fa-align-left",
            label: "snippet3",
            onClick: expect.any(Function),
          },
        ]),
      })
    );
  });
});

describe("executeSnippet", () => {
  let tabsStore;

  beforeAll(() => {
    tabsStore = useTabsStore();
    let parentTab = tabsStore.addTab({ name: "test primary tab" });
    tabsStore.selectTab(parentTab);

    let secondaryTab = tabsStore.addTab({
      parentId: parentTab.id,
      name: "test secondary tab",
    });
    tabsStore.selectTab(secondaryTab);
  });

  test("should emit event to snippet editor", async () => {
    const responseData = {
      data: "Snippet Text",
    };

    const emitterEmitSpy = vi.spyOn(emitter, "emit");

    axios.post.mockResolvedValue({ data: responseData });

    executeSnippet(1);

    await flushPromises();

    expect(emitterEmitSpy).toHaveBeenCalledOnce();
    expect(emitterEmitSpy).toBeCalledWith(
      `${tabsStore.selectedPrimaryTab.metaData.selectedTab.id}_insert_to_editor`,
      responseData.data
    );
  });

  test("should show toast message on error response", async () => {
    const snippetId = 1;

    axios.post.mockRejectedValue(new Error("Bad request"));

    executeSnippet(snippetId);

    await flushPromises();

    expect(axios.post).toHaveBeenCalledWith("/get_snippet_text/", {
      snippet_id: snippetId,
    });
    expect(handleError).toHaveBeenCalledOnce();
    expect(handleError).toHaveBeenCalledWith(new Error("Bad request"));
  });
});
