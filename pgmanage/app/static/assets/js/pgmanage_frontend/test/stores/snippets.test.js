import { setActivePinia, createPinia } from "pinia";
import { useSnippetsStore } from "../../src/stores/snippets";
import { beforeEach, test, expect, describe } from "vitest";

describe("Snippets Store", () => {
  beforeEach(() => {
    // creates a fresh pinia and makes it active
    // so it's automatically picked up by any useStore() call
    // without having to pass it to it: `useStore(pinia)`
    setActivePinia(createPinia());
  });

  test("should have correct initial data", () => {
    const snippets = useSnippetsStore();

    expect(snippets.id).toBeNull();
    expect(snippets.files.length).toBe(0);
    expect(snippets.folders.length).toBe(0);
  });
});
