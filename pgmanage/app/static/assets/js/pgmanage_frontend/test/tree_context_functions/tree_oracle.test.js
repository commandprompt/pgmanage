import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  TemplateSelectOracle,
  TemplateInsertOracle,
  TemplateUpdateOracle,
} from "@/tree_context_functions/tree_oracle";
import axios from "axios";
import { showToast } from "@/notification_control";
import { emitter } from "@/emitter";
import { tabsStore } from "@/stores/stores_initializer";
import { flushPromises } from "@vue/test-utils";
import { tabSQLTemplate } from "@/tree_context_functions/tree_postgresql";

vi.mock("axios");
vi.mock("@/notification_control", () => ({
  showToast: vi.fn(),
}));
vi.mock("@/emitter", () => ({
  emitter: {
    emit: vi.fn(),
  },
}));
vi.mock("@/tree_context_functions/tree_postgresql", () => ({
  tabSQLTemplate: vi.fn(),
}));
vi.mock("@/stores/stores_initializer", () => ({
  tabsStore: {
    selectedPrimaryTab: {
      metaData: {
        selectedDatabaseIndex: 1,
        selectedTab: { id: "tab-1" },
      },
      id: "primary-tab-id",
    },
    createQueryTab: vi.fn(),
  },
}));

describe("Template Functions", () => {
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

  describe("TemplateSelectOracle", async () => {
    it("should create a query tab and emit run query event on success", async () => {
      const schema = "testSchema";
      const table = "testTable";
      const response = { data: { template: "SELECT * FROM testTable;" } };

      axios.post.mockResolvedValueOnce(response);

      TemplateSelectOracle(schema, table);

      await flushPromises();

      expect(axios.post).toHaveBeenCalledWith("/template_select_oracle/", {
        database_index: 1,
        tab_id: "primary-tab-id",
        table,
        schema,
      });
      expect(tabsStore.createQueryTab).toHaveBeenCalledWith(
        "testSchema.testTable",
        null,
        null,
        response.data.template
      );
      vi.runAllTimers();
      expect(emitter.emit).toHaveBeenCalledWith("tab-1_run_query");
    });

    it("should show error toast on failure", async () => {
      const schema = "testSchema";
      const table = "testTable";

      axios.post.mockRejectedValueOnce(errorResponse);

      TemplateSelectOracle(schema, table);
      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });

  describe("TemplateInsertOracle", () => {
    it("should call tabSQLTemplate on success", async () => {
      const schema = "testSchema";
      const table = "testTable";
      const response = {
        data: { template: "INSERT INTO testTable VALUES ();" },
      };

      axios.post.mockResolvedValueOnce(response);

      TemplateInsertOracle(schema, table);
      await flushPromises();

      expect(axios.post).toHaveBeenCalledWith("/template_insert_oracle/", {
        database_index: 1,
        tab_id: "primary-tab-id",
        table,
        schema,
      });
      expect(tabSQLTemplate).toHaveBeenCalledWith(
        "Insert testSchema.testTable",
        response.data.template
      );
    });

    it("should show error toast on failure", async () => {
      const schema = "testSchema";
      const table = "testTable";

      axios.post.mockRejectedValueOnce(errorResponse);

      TemplateInsertOracle(schema, table);

      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });

  describe("TemplateUpdateOracle", () => {
    it("should call tabSQLTemplate on success", async () => {
      const schema = "testSchema";
      const table = "testTable";
      const response = { data: { template: "UPDATE testTable SET ...;" } };

      axios.post.mockResolvedValueOnce(response);

      TemplateUpdateOracle(schema, table);

      await flushPromises();

      expect(axios.post).toHaveBeenCalledWith("/template_update_oracle/", {
        database_index: 1,
        tab_id: "primary-tab-id",
        table,
        schema,
      });
      expect(tabSQLTemplate).toHaveBeenCalledWith(
        "Update testSchema.testTable",
        response.data.template
      );
    });

    it("should show error toast on failure", async () => {
      const schema = "testSchema";
      const table = "testTable";

      axios.post.mockRejectedValueOnce(errorResponse);

      TemplateUpdateOracle(schema, table);

      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });
});
