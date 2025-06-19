import { setActivePinia, createPinia } from "pinia";
import { describe, it, expect, beforeEach, vi } from "vitest";
import axios from "axios";
import { useDbMetadataStore } from "@/stores/db_metadata";

vi.mock("axios");

describe("dbMetadata store", () => {
  let dbMetadataStore;

  beforeEach(() => {
    setActivePinia(createPinia());
    dbMetadataStore = useDbMetadataStore();
    vi.clearAllMocks();
  });

  it("initializes with default state", () => {
    expect(dbMetadataStore.initialized).toBe("false");
    expect(dbMetadataStore.dbMeta).toEqual({});
  });

  it("retrieves database metadata from state", () => {
    dbMetadataStore.dbMeta = {
      1: {
        test_db: { schemas: ["schema1", "schema2"] },
      },
    };

    const meta = dbMetadataStore.getDbMeta(1, "test_db");
    expect(meta).toEqual({ schemas: ["schema1", "schema2"] });
  });

  it("returns undefined for non-existent database metadata", () => {
    dbMetadataStore.dbMeta = {};

    const meta = dbMetadataStore.getDbMeta(1, "test_db");
    expect(meta).toBeUndefined();
  });

  it("fetches and stores database metadata", async () => {
    const mockResponse = {
      data: {
        schemas: [
          { name: "schema1", tables: [] },
          { name: "schema2", tables: [] },
        ],
      },
    };
    axios.post.mockResolvedValue(mockResponse);

    await dbMetadataStore.fetchDbMeta(1, "workspace1", "test_db");

    expect(axios.post).toHaveBeenCalledWith("/get_database_meta/", {
      database_index: 1,
      workspace_id: "workspace1",
      database_name: "test_db",
    });

    expect(dbMetadataStore.dbMeta).toEqual({
      1: {
        test_db: [
          { name: "schema1", tables: [] },
          { name: "schema2", tables: [] },
        ],
      },
    });
  });

  it("does not fetch metadata if it already exists", async () => {
    dbMetadataStore.dbMeta = {
      1: {
        test_db: { schemas: ["schema1"] },
      },
    };

    await dbMetadataStore.fetchDbMeta(1, "workspace1", "test_db");

    expect(axios.post).not.toHaveBeenCalled();
  });

  it("handles errors when fetching database metadata", async () => {
    axios.post.mockRejectedValue(new Error("Network Error"));

    await expect(
      dbMetadataStore.fetchDbMeta(1, "tab1", "test_db")
    ).rejects.toThrow("Network Error");
  });
});
