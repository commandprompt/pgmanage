import { describe, it, expect, beforeEach, vi } from "vitest";
import { mount } from "@vue/test-utils";
import ConnectionsModalConnectionForm from "@src/components/ConnectionsModalConnectionForm.vue";
import { mountConnectionForm, postgresqlConnection } from "../helpers/fixtures.js";

vi.mock("bootstrap", () => ({
  Modal: { getOrCreateInstance: vi.fn(() => ({ hide: vi.fn(), show: vi.fn() })) },
}));

describe("ConnectionsModalConnectionForm", () => {
  let wrapper;

  beforeEach(async () => {
    wrapper = await mountConnectionForm(
      mount, ConnectionsModalConnectionForm, postgresqlConnection()
    );
  });

  it("does not report a change on load", () => {
    expect(wrapper.vm.isChanged).toBe(false);
  });

  it("hides the authentication method outside an enterprise build", () => {
    expect(wrapper.vm.showAuthMethod).toBe(false);
    expect(wrapper.find("#authMethod").exists()).toBe(false);
    expect(wrapper.find("#awsRegion").exists()).toBe(false);
  });

  it("shows the password field", () => {
    expect(wrapper.find("#connectionPassword").exists()).toBe(true);
  });

  it("resets credentials_extra when the technology changes", async () => {
    await wrapper.find("#connectionType").setValue("mysql");

    expect(wrapper.vm.connectionLocal.credentials_extra).toEqual({});

    await wrapper.find("#connectionType").setValue("postgresql");

    expect(wrapper.vm.connectionLocal.credentials_extra).toEqual({
      auth_method: "user-pass",
    });
  });
});
