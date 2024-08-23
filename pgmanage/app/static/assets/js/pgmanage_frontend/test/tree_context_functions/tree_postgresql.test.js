import { showToast } from "@/notification_control";
import { emitter } from "@/emitter";
import { tabsStore } from "@/stores/stores_initializer";
import axios from "axios";
import {
  TemplateSelectPostgresql,
  TemplateUpdatePostgresql,
  TemplateInsertPostgresql,
  TemplateSelectFunctionPostgresql,
} from "@/tree_context_functions/tree_postgresql";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { flushPromises } from "@vue/test-utils";

vi.mock("axios");
vi.mock("@/notification_control", () => ({
  showToast: vi.fn(),
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

describe("TemplatePostgresql Functions", () => {
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

  describe("TemplateSelectPostgresql", () => {
    it("should call axios.post and handle success response", async () => {
      const response = {
        data: {
          template: "SELECT * FROM table",
        },
      };
      axios.post.mockResolvedValue(response);

      TemplateSelectPostgresql("schema", "table", "kind");

      await flushPromises();
      vi.runAllTimers();

      expect(axios.post).toHaveBeenCalledWith("/template_select_postgresql/", {
        database_index: 1,
        tab_id: "primary-tab-id",
        table: "table",
        schema: "schema",
        kind: "kind",
      });
      expect(tabsStore.createQueryTab).toHaveBeenCalledWith(
        "database@schema.table",
        null,
        null,
        "SELECT * FROM table"
      );
      expect(emitter.emit).toHaveBeenCalledWith("tab-1_run_query");
    });

    it("should call axios.post and handle error response", async () => {
      axios.post.mockRejectedValue(errorResponse);

      TemplateSelectPostgresql("schema", "table", "kind");

      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });

  describe("TemplateInsertPostgresql", () => {
    it("should call axios.post and handle success response", async () => {
      const response = {
        data: { template: "INSERT INTO testTable VALUES ();" },
      };
      axios.post.mockResolvedValue(response);

      TemplateInsertPostgresql("schema", "table");

      await flushPromises();

      expect(axios.post).toHaveBeenCalledWith("/template_insert_postgresql/", {
        database_index: 1,
        tab_id: "primary-tab-id",
        table: "table",
        schema: "schema",
      });
      expect(tabsStore.createQueryTab).toHaveBeenCalledWith(
        "Insert schema.table",
        null,
        null,
        "INSERT INTO testTable VALUES ();"
      );
    });

    it("should call axios.post and handle error response", async () => {
      axios.post.mockRejectedValue(errorResponse);

      TemplateInsertPostgresql("schema", "table");
      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });

  describe("TemplateUpdatePostgresql", () => {
    it("should call axios.post and handle success response", async () => {
      const response = {
        data: {
          template: "UPDATE table SET ...",
        },
      };
      axios.post.mockResolvedValue(response);

      TemplateUpdatePostgresql("schema", "table");
      await flushPromises();

      expect(axios.post).toHaveBeenCalledWith("/template_update_postgresql/", {
        database_index: 1,
        tab_id: "primary-tab-id",
        table: "table",
        schema: "schema",
      });
      expect(tabsStore.createQueryTab).toHaveBeenCalledWith(
        "Update schema.table",
        null,
        null,
        "UPDATE table SET ..."
      );
    });

    it("should call axios.post and handle error response", async () => {
      axios.post.mockRejectedValue(errorResponse);

      TemplateUpdatePostgresql("schema", "table");
      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });

  describe("TemplateSelectFunctionPostgresql", () => {
    it("should call axios.post and handle success response", async () => {
      const response = {
        data: {
          template: "SELECT * FROM function()",
        },
      };
      axios.post.mockResolvedValue(response);

      TemplateSelectFunctionPostgresql("schema", "func", "functionid");
      await flushPromises();

      expect(axios.post).toHaveBeenCalledWith(
        "/template_select_function_postgresql/",
        {
          database_index: 1,
          tab_id: "primary-tab-id",
          function: "func",
          functionid: "functionid",
          schema: "schema",
        }
      );
      expect(tabsStore.createQueryTab).toHaveBeenCalledWith(
        "Select schema.func",
        null,
        null,
        "SELECT * FROM function()"
      );
    });

    it("should call axios.post and handle error response", async () => {
      axios.post.mockRejectedValue(errorResponse);

      TemplateSelectFunctionPostgresql("schema", "func", "functionid");
      await flushPromises();

      expect(showToast).toHaveBeenCalledWith("error", "Error message");
    });
  });
});
