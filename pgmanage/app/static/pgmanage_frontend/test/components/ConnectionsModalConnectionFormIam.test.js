import { describe, it, expect, beforeEach, vi } from "vitest";
import { mount } from "@vue/test-utils";
import ConnectionsModalConnectionForm from "@src/components/ConnectionsModalConnectionForm.vue";
import { mountConnectionForm, postgresqlConnection } from "../helpers/fixtures.js";

vi.mock("bootstrap", () => ({
  Modal: { getOrCreateInstance: vi.fn(() => ({ hide: vi.fn(), show: vi.fn() })) },
}));

// the authentication method is only shown in an enterprise build
vi.mock("@src/constants", async (importOriginal) => ({
  ...(await importOriginal()),
  isEnterprise: true,
}));

describe("ConnectionsModalConnectionForm with IAM authentication", () => {
  let wrapper;

  beforeEach(async () => {
    wrapper = await mountConnectionForm(
      mount, ConnectionsModalConnectionForm, postgresqlConnection()
    );
  });

  it("does not report a change on load", () => {
    expect(wrapper.vm.isChanged).toBe(false);
  });

  it("shows the password field with password authentication", () => {
    expect(wrapper.vm.isIamAuth).toBe(false);
    expect(wrapper.find("#authMethod").exists()).toBe(true);
    expect(wrapper.find("#connectionPassword").exists()).toBe(true);
    expect(wrapper.find("#awsRegion").exists()).toBe(false);
  });

  it("shows the IAM fields when IAM is selected", async () => {
    await wrapper.find("#authMethod").setValue("iam");

    expect(wrapper.vm.isIamAuth).toBe(true);
    expect(wrapper.find("#connectionPassword").exists()).toBe(false);
    expect(wrapper.find("#awsRegion").exists()).toBe(true);
    expect(wrapper.find("#accessKeyId").exists()).toBe(true);
    expect(wrapper.find("#secretAccessKey").exists()).toBe(true);
  });

  it("keeps the IAM fields of a stored connection", async () => {
    wrapper = await mountConnectionForm(
      mount,
      ConnectionsModalConnectionForm,
      postgresqlConnection({
        credentials_extra: {
          auth_method: "iam",
          aws_region: "us-east-1",
          access_key_id: "",
          secret_access_key: "",
        },
      })
    );

    expect(wrapper.vm.isIamAuth).toBe(true);
    expect(wrapper.vm.isChanged).toBe(false);
    expect(wrapper.find("#awsRegion").element.value).toBe("us-east-1");
  });

  it("has no authentication method for other technologies", async () => {
    wrapper = await mountConnectionForm(
      mount,
      ConnectionsModalConnectionForm,
      postgresqlConnection({ technology: "mysql", credentials_extra: {} })
    );

    expect(wrapper.vm.showAuthMethod).toBe(false);
    expect(wrapper.find("#authMethod").exists()).toBe(false);
  });
});
