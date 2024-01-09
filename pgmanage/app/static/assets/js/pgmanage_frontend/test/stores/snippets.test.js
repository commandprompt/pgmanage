import { setActivePinia, createPinia } from "pinia";
import { useSnippetsStore } from "../../src/stores/snippets";
import { beforeEach, test, expect, describe, vi } from "vitest";

describe("Snippets Store", () => {
  let tabList, v_connTabControl;

  beforeEach(() => {
    tabList = [
      { tag: { snippetObject: { id: 1 } } },
      { tag: { snippetObject: { id: 2 } } },
    ];
    v_connTabControl = { snippet_tag: { tabControl: { tabList } } };

    // Mock the global variable for testing
    vi.stubGlobal("v_connTabControl", v_connTabControl);

    // creates a fresh pinia and makes it active
    // so it's automatically picked up by any useStore() call
    // without having to pass it to it: `useStore(pinia)`
    setActivePinia(createPinia());
  });

  test("should return existing tab when id matches", () => {
    const snippets = useSnippetsStore();

    expect(snippets.id).toBeNull();
    expect(snippets.files.length).toBe(0);
    expect(snippets.folders.length).toBe(0);

    let tab = snippets.getExistingTab(1);

    expect(tab).toBe(tabList[0]);
  });

  test("should return undefined for non-existing tab", () => {
    const snippets = useSnippetsStore();

    let tab = snippets.getExistingTab(3);
    expect(tab).toBeUndefined();
  });
});
