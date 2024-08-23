import { describe, it, expect, vi, beforeEach } from "vitest";
import axios from "axios";
import { showToast } from "@/notification_control";
import { emitter } from "@/emitter";
import { tabsStore } from "@/stores/stores_initializer";
import { flushPromises } from "@vue/test-utils";
import {
  TemplateSelectSqlite,
  TemplateInsertSqlite,
  TemplateUpdateSqlite,
} from "@/tree_context_functions/tree_sqlite";
import { tabSQLTemplate } from "@/tree_context_functions/tree_postgresql";

vi.mock("axios");
vi.mock("@/notification_control", () => ({
  showToast: vi.fn(),
}));
vi.mock("@/tree_context_functions/tree_postgresql", () => ({
  tabSQLTemplate: vi.fn(),
}));
vi.mock("@/emitter", () => ({
  emitter: {
    emit: vi.fn(),
  },
}));
vi.mock("@/stores/stores_initializer", () => ({
  tabsStore: {
    selectedPrimaryTab: {
      metaData: {
        selectedDatabaseIndex: 1,
        selectedTab: {
          id: "tab-1",
        },
        selectedDatabase: "database",
      },
      id: "primary-tab-id",
    },
    createQueryTab: vi.fn(),
  },
}));

describe("TemplateSqlite Functions", () => {
  const errorResponse = {
    response: {
      data: {
        data: "Error message",
      },
    },
  };
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
  });

  describe("TemplateSelectSqlite", () => {
    it("should call axios.post and handle success response", async () => {
      const response = {
        data: { template: "SELECT * FROM testTable;" },
      };
      axios.post.mockResolvedValue(response);

      TemplateSelectSqlite("table", "kind");

      await flushPromises();
      vi.runAllTimers();

      expect(axios.post).toHaveBeenCalledWith("/template_select_sqlite/", {
        database_index: 1,
        tab_id: "primary-tab-id",
        table: "table",
        kind: "kind",
      });
      expect(tabsStore.createQueryTab).toHaveBeenCalledWith(
        "table",
        null,
        null,
        response.data.template
      );
      expect(emitter.emit).toHaveBeenCalledWith("tab-1_run_query");
    });

    it("should handle error response", async () => {
      axios.post.mockRejectedValue(errorResponse);

      TemplateSelectSqlite("table", "kind");

      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });

  describe("TemplateInsertSqlite", () => {
    it("should call axios.post and handle success response", async () => {
      const response = {
        data: { template: "INSERT INTO testTable VALUES ();" },
      };

      axios.post.mockResolvedValue(response);

      TemplateInsertSqlite("table");

      await flushPromises();

      expect(axios.post).toHaveBeenCalledWith("/template_insert_sqlite/", {
        database_index: 1,
        tab_id: "primary-tab-id",
        table: "table",
      });
      expect(tabSQLTemplate).toHaveBeenCalledTimes(1);
      expect(tabSQLTemplate).toHaveBeenCalledWith(
        "Insert table",
        response.data.template
      );
    });

    it("should handle error response", async () => {
      axios.post.mockRejectedValue(errorResponse);

      TemplateInsertSqlite("table");
      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });

  describe("TemplateUpdateSqlite", () => {
    it("should call axios.post and handle success response", async () => {
      const response = {
        data: { template: "UPDATE testTable SET column = value;" },
      };
      axios.post.mockResolvedValue(response);

      TemplateUpdateSqlite("table");

      await flushPromises();

      expect(axios.post).toHaveBeenCalledWith("/template_update_sqlite/", {
        database_index: 1,
        tab_id: "primary-tab-id",
        table: "table",
      });
      expect(tabSQLTemplate).toHaveBeenCalledTimes(1);
      expect(tabSQLTemplate).toHaveBeenCalledWith(
        "Update table",
        response.data.template
      );
    });

    it("should handle error response", async () => {
      axios.post.mockRejectedValue(errorResponse);

      TemplateUpdateSqlite("table");
      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });
});
