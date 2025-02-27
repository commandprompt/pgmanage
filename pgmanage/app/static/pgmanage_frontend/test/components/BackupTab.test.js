import BackupTab from "@/components/BackupTab.vue";
import UtilityJobs from "@/components/UtilityJobs.vue";
import { showAlert } from "@/notification_control";
import { fileManagerStore } from "@/stores/stores_initializer";
import { flushPromises, mount } from "@vue/test-utils";
import axios from "axios";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { handleError } from "@/logging/utils";

vi.hoisted(() => {
  vi.stubGlobal("v_csrf_cookie_name", "test_cookie");
  vi.stubGlobal("app_base_path", "test_folder");
});

vi.mock("@/logging/utils", () => ({
  handleError: vi.fn(),
}));

vi.mock("@/notification_control", () => ({
  showAlert: vi.fn(),
}));

vi.mock("axios");
vi.mock("@/stores/stores_initializer", async (importOriginal) => {
  const orig = await importOriginal();
  return {
    ...orig,
    tabsStore: {
      selectedPrimaryTab: {
        metaData: {
          selectedTab: {
            id: "testTab",
          },
        },
      },
    },
  };
});

describe("BackupTab Component", () => {
  let wrapper;
  const props = {
    workspaceId: "testWorkspace",
    tabId: "testTab",
    databaseIndex: 1,
    backupType: "objects",
    treeNode: {
      data: { type: "table", database: "testDB", schema: "public" },
      title: "testTable",
    },
  };

  beforeEach(() => {
    axios.post.mockResolvedValueOnce({
      data: { data: [] },
    });
    axios.get.mockResolvedValue({
      data: { data: [] },
    });
    wrapper = mount(BackupTab, {
      props: props,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });
  it("renders correctly with required props", () => {
    expect(wrapper.exists()).toBeTruthy();
    expect(wrapper.findComponent(UtilityJobs).exists()).toBeTruthy();
    expect(wrapper.find(".backup-tab-scrollable").exists()).toBeTruthy();
  });

  it('renders the correct options for "server" backup type', async () => {
    await wrapper.setProps({ backupType: "server" });
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsOnlyRoles`).exists()
    ).toBeTruthy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsOnlyTablespaces`).exists()
    ).toBeTruthy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsOnlyGlobals`).exists()
    ).toBeTruthy();
    expect(wrapper.find("#backupFormat").exists()).toBeFalsy();
    expect(wrapper.find("#backupCompressionRatio").exists()).toBeFalsy();
    expect(wrapper.find("#backupNumberOfJobs").exists()).toBeFalsy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsPreData`).exists()
    ).toBeFalsy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsBlobs`).exists()
    ).toBeFalsy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsCreateDbStatement`).exists()
    ).toBeFalsy();
  });

  it('renders the correct options for "objects" backup type', () => {
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsOnlyRoles`).exists()
    ).toBeFalsy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsOnlyTablespaces`).exists()
    ).toBeFalsy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsOnlyGlobals`).exists()
    ).toBeFalsy();
    expect(wrapper.find("#backupFormat").exists()).toBeTruthy();
    expect(wrapper.find("#backupCompressionRatio").exists()).toBeTruthy();
    expect(wrapper.find("#backupNumberOfJobs").exists()).toBeTruthy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsPreData`).exists()
    ).toBeTruthy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsBlobs`).exists()
    ).toBeTruthy();
    expect(
      wrapper.find(`#${props.tabId}_backupOptionsCreateDbStatement`).exists()
    ).toBeTruthy();
  });

  it("renders the role names in a dropdown", async () => {
    await wrapper.setData({ roleNames: ["admin", "user", "guest"] });
    const roleSelect = wrapper.find("#backupRoleName");
    expect(roleSelect.exists()).toBeTruthy();
    const options = roleSelect.findAll("option");
    expect(options).toHaveLength(4);
    expect(options.at(1).text()).toBe("admin");
    expect(options.at(2).text()).toBe("user");
    expect(options.at(3).text()).toBe("guest");
  });

  it("should render Revert Settings, Preview, and Backup buttons as disabled initially", () => {
    const revertSettingsButton = wrapper.find(
      '[data-testid="revert-settings-button"]'
    );
    const previewButton = wrapper.find('[data-testid="preview-button"]');
    const backupButton = wrapper.find('[data-testid="backup-button"]');

    expect(revertSettingsButton.exists()).toBeTruthy();
    expect(previewButton.exists()).toBeTruthy();
    expect(backupButton.exists()).toBeTruthy();

    expect(revertSettingsButton.classes("disabled")).toBeTruthy();
    expect(previewButton.classes("disabled")).toBeTruthy();
    expect(backupButton.classes("disabled")).toBeTruthy();
  });

  it("enables Revert Settings when a value changes in the select element", async () => {
    const selectElement = wrapper.find("#backupEncoding");
    const revertSettingsButton = wrapper.find(
      '[data-testid="revert-settings-button"]'
    );
    const previewButton = wrapper.find('[data-testid="preview-button"]');
    const backupButton = wrapper.find('[data-testid="backup-button"]');

    await selectElement.setValue("BIG5");

    expect(revertSettingsButton.classes("disabled")).toBeFalsy();
    expect(previewButton.classes("disabled")).toBeTruthy();
    expect(backupButton.classes("disabled")).toBeTruthy();
  });

  it("enables Backup and Preview buttons when the backup file name changes", async () => {
    const previewButton = wrapper.find('[data-testid="preview-button"]');
    const backupButton = wrapper.find('[data-testid="backup-button"]');

    await wrapper.setData({ backupOptions: { fileName: "new_backup.sql" } });

    expect(previewButton.classes("disabled")).toBeFalsy();
    expect(backupButton.classes("disabled")).toBeFalsy();
  });

  it("disables Revert Settings, Backup, and Preview buttons on Revert Settings click", async () => {
    const revertSettingsButton = wrapper.find(
      '[data-testid="revert-settings-button"]'
    );
    const previewButton = wrapper.find('[data-testid="preview-button"]');
    const backupButton = wrapper.find('[data-testid="backup-button"]');

    await wrapper.setData({ backupOptions: { fileName: "new_backup.sql" } });

    expect(revertSettingsButton.classes("disabled")).toBeFalsy();
    expect(previewButton.classes("disabled")).toBeFalsy();
    expect(backupButton.classes("disabled")).toBeFalsy();

    await revertSettingsButton.trigger("click");

    expect(revertSettingsButton.classes("disabled")).toBeTruthy();
    expect(previewButton.classes("disabled")).toBeTruthy();
    expect(backupButton.classes("disabled")).toBeTruthy();
  });

  it("should set 'custom' format type as default for 'objects' backup type", () => {
    expect(wrapper.vm.backupOptions.format).toBe("custom");
    expect(wrapper.vm.backupOptions.format).not.toBe("plain");
  });

  it("should set 'plain' format type as default for 'server' backup type", async () => {
    wrapper.unmount();
    axios.post.mockResolvedValueOnce({
      data: { data: [] },
    });
    wrapper = mount(BackupTab, {
      props: {
        ...props,
        backupType: "server",
      },
    });
    await flushPromises();
    expect(wrapper.vm.backupOptions.format).toBe("plain");
    expect(wrapper.vm.backupOptions.format).not.toBe("custom");
  });

  it("should change backup type based on output file extension on 'changeFile' fileManager action", async () => {
    expect(wrapper.vm.backupOptions.format).toBe("custom");

    const fileMockSql = { type: "sql", path: "/custom/path" };
    await fileManagerStore.changeFile(fileMockSql);
    expect(wrapper.vm.backupOptions.format).toBe("plain");

    const fileMockTar = { type: "tar", path: "/custom/path" };
    await fileManagerStore.changeFile(fileMockTar);
    expect(wrapper.vm.backupOptions.format).toBe("tar");

    const fileMockDir = {
      type: "tar",
      path: "/custom/path",
      is_directory: true,
    };
    await fileManagerStore.changeFile(fileMockDir);
    expect(wrapper.vm.backupOptions.format).toBe("directory");

    const fileMockOther = { type: "other", path: "/custom/path" };
    await fileManagerStore.changeFile(fileMockOther);
    expect(wrapper.vm.backupOptions.format).toBe("custom");
  });

  it("should change fileName extension based on selected format type", async () => {
    await wrapper.setData({
      backupOptions: { fileName: "/custom/path/test.sql", format: "directory" },
    });
    expect(wrapper.vm.backupOptions.fileName).toEqual("/custom/path/test");

    await wrapper.setData({ backupOptions: { format: "plain" } });
    expect(wrapper.vm.backupOptions.fileName).toEqual("/custom/path/test.sql");

    await wrapper.setData({ backupOptions: { format: "custom" } });
    expect(wrapper.vm.backupOptions.fileName).toEqual("/custom/path/test.dump");

    await wrapper.setData({ backupOptions: { format: "tar" } });
    expect(wrapper.vm.backupOptions.fileName).toEqual("/custom/path/test.tar");
  });

  it("calls getRoleNames and updates roleNames on success", async () => {
    const mockRoles = {
      data: { data: [{ name: "role1" }, { name: "role2" }] },
    };
    axios.post.mockResolvedValueOnce(mockRoles);

    await wrapper.vm.getRoleNames();
    await flushPromises();
    expect(axios.post).toHaveBeenCalledWith("/get_roles_postgresql/", {
      database_index: props.databaseIndex,
      workspace_id: props.workspaceId,
    });
    expect(wrapper.vm.roleNames).toEqual(["role1", "role2"]);
  });

  it("calls getRoleNames and shows an error toast on failure", async () => {
    const errorResponse = {
      response: { data: { data: "Error fetching roles" } },
    };
    axios.post.mockRejectedValueOnce(errorResponse);

    await wrapper.vm.getRoleNames();
    await flushPromises();

    expect(handleError).toHaveBeenCalledWith(errorResponse);
  });

  it("updates fileName and format correctly in onFile", () => {
    const mockEventSql = {
      target: {
        files: [{ path: "/path/to/file.sql", type: "application/sql" }],
        hasAttribute: vi.fn(() => false),
      },
    };

    const mockEventTar = {
      target: {
        files: [{ path: "/path/to/file.tar", type: "application/x-tar" }],
        hasAttribute: vi.fn(() => false),
      },
    };

    const mockEventOther = {
      target: {
        files: [{ path: "/path/to/file.dump", type: "some-custom-type" }],
        hasAttribute: vi.fn(() => false),
      },
    };

    wrapper.vm.onFile(mockEventSql);

    expect(wrapper.vm.backupOptions.format).toBe("plain");

    wrapper.vm.onFile(mockEventTar);
    expect(wrapper.vm.backupOptions.format).toBe("tar");

    wrapper.vm.onFile(mockEventOther);
    expect(wrapper.vm.backupOptions.format).toBe("custom");
  });

  it("calls saveBackup and starts a job on success", async () => {
    const mockResponse = { data: { job_id: 123, description: "Backup Job" } };
    axios.post.mockResolvedValueOnce(mockResponse);
    const startJobSpy = vi.spyOn(wrapper.vm.$refs.jobs, "startJob");

    await wrapper.vm.saveBackup();
    await flushPromises();

    expect(axios.post).toHaveBeenCalledWith("/backup/", {
      database_index: props.databaseIndex,
      workspaceId: props.workspaceId,
      data: wrapper.vm.backupOptions,
      backup_type: wrapper.vm.type,
    });
    expect(startJobSpy).toHaveBeenCalledWith(123, "Backup Job");
  });

  it("calls saveBackup and shows an error toast on failure", async () => {
    const errorResponse = {
      response: { data: { data: "Error saving backup" } },
    };
    axios.post.mockRejectedValueOnce(errorResponse);

    await wrapper.vm.saveBackup();
    await flushPromises();

    expect(handleError).toHaveBeenCalledWith(errorResponse);
  });

  it("updates fileName in changeFilePath", () => {
    wrapper.vm.changeFilePath("/new/path/to/file.sql");

    expect(wrapper.vm.backupOptions.fileName).toBe("/new/path/to/file.sql");
  });

  it("calls openFileManagerModal and shows the modal", () => {
    const showModalSpy = vi.spyOn(fileManagerStore, "showModal");
    wrapper.vm.openFileManagerModal();

    expect(showModalSpy).toHaveBeenCalledWith(
      undefined,
      wrapper.vm.onFile,
      wrapper.vm.dialogType
    );
  });

  it("resets backupOptions to default", () => {
    wrapper.vm.backupOptions.fileName = "custom.sql";

    wrapper.vm.resetToDefault();

    expect(wrapper.vm.backupOptions).toEqual(wrapper.vm.backupOptionsDefault);
  });

  it("calls previewCommand and shows the command on success", async () => {
    const mockResponse = { data: { command: { cmd: "pg_dump command" } } };
    axios.post.mockResolvedValueOnce(mockResponse);

    await wrapper.vm.previewCommand();
    await flushPromises();

    expect(axios.post).toHaveBeenCalledWith("/backup/preview_command/", {
      database_index: props.databaseIndex,
      workspaceId: props.workspaceId,
      data: wrapper.vm.backupOptions,
      backup_type: wrapper.vm.type,
    });
    expect(showAlert).toHaveBeenCalledWith("pg_dump command");
  });

  it("calls previewCommand and shows an error toast on failure", async () => {
    const errorResponse = {
      response: { data: { data: "Error previewing command" } },
    };
    axios.post.mockRejectedValueOnce(errorResponse);

    await wrapper.vm.previewCommand();
    await flushPromises();

    expect(handleError).toHaveBeenCalledWith(errorResponse);
  });
});
