import { beforeEach, describe, expect, test, vi } from "vitest";
import { getAllSnippets } from "../../src/tree_context_functions/tree_snippets";
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
