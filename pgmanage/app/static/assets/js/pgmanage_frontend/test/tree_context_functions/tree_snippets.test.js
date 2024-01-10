import { beforeEach, describe, expect, test, vi } from "vitest";
import {
  getAllSnippets,
  buildSnippetContextMenuObjects,
} from "../../src/tree_context_functions/tree_snippets";
import axios from "axios";
import { setActivePinia, createPinia } from "pinia";

vi.mock("axios");

describe("getAllSnippets", () => {
  beforeEach(() => {
    // Set up Pinia
    setActivePinia(createPinia());
  });

  test("makes get request to get all snippets", () => {
    const mockedData = {
      data: {
        files: [{ name: "file1" }, { name: "file2" }],
        folders: [{ name: "folder1" }, { name: "folder2" }],
      },
    };

    // Mock the Axios get method to return a resolved promise with the mocked data
    axios.get.mockResolvedValue({ data: mockedData });

    getAllSnippets();

    expect(axios.get).toHaveBeenCalledWith("/get_all_snippets/");
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
