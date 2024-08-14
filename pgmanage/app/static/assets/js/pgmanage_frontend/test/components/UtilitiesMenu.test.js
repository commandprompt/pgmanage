import { mount } from "@vue/test-utils";
import UtilitiesMenu from "@/components/UtilitiesMenu.vue";
import {
  settingsStore,
  messageModalStore,
  utilitiesMenuStore,
} from "@/stores/stores_initializer";
import { vi, describe, afterEach, expect, beforeEach, it } from "vitest";

vi.mock("@/stores/stores_initializer");
vi.stubGlobal("short_version", "1.0.0");
vi.stubGlobal("user_name", "admin");

describe("UtilitiesMenu.vue", () => {
  let wrapper;

  beforeEach(() => {
    settingsStore.desktopMode = false;
    utilitiesMenuStore.items = [
      {
        name: "Extra Item",
        icon: "fas fa-extra-icon",
        show: true,
        clickFunction: vi.fn(),
      },
    ];
    wrapper = mount(UtilitiesMenu);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("should render the correct version and username", () => {
    expect(
      wrapper.find("#omnidb__utilities-menu__link-versioning .badge").text()
    ).toBe(window.short_version);
    expect(
      wrapper.find("#omnidb__utilities-menu__link-username span").text()
    ).toBe(window.user_name);
  });

  it("should toggle menuExpanded when toggleUtilitiesMenu is called", async () => {
    expect(wrapper.vm.menuExpanded).toBe(false);

    await wrapper.find("#omnidb__utilities-menu__link-toggle").trigger("click");

    expect(wrapper.vm.menuExpanded).toBe(true);

    await wrapper.find("#omnidb__utilities-menu__link-toggle").trigger("click");

    expect(wrapper.vm.menuExpanded).toBe(false);
  });

  it("should show settings modal when clicking the settings icon", async () => {
    await wrapper.find("#omnidb__utilities-menu__link-config").trigger("click");

    expect(settingsStore.showModal).toHaveBeenCalled();
  });

  it("should confirm signout when clicking the signout icon", async () => {
    window.open = vi.fn();

    await wrapper
      .find("#omnidb__utilities-menu__link-signout")
      .trigger("click");

    expect(messageModalStore.showModal).toHaveBeenCalledWith(
      "Are you sure you want to sign out?",
      expect.any(Function)
    );
  });

  it("should call clickFunction of extra menu items when clicked", async () => {
    const extraItem = wrapper.find(".fas.fa-extra-icon").element;

    await wrapper.find(".fas.fa-extra-icon").trigger("click");

    expect(utilitiesMenuStore.items[0].clickFunction).toHaveBeenCalled();
  });

  it("should close the menu when clicking outside", async () => {
    wrapper.vm.menuExpanded = true;

    const outsideElement = document.createElement("div");
    document.body.appendChild(outsideElement);

    await wrapper.vm.handleOutsideClick({ target: outsideElement });

    expect(wrapper.vm.menuExpanded).toBe(false);

    document.body.removeChild(outsideElement);
  });

  it("should not close the menu when clicking inside the toggle button", async () => {
    wrapper.vm.menuExpanded = true;

    const toggleButton = wrapper.find(
      "#omnidb__utilities-menu__link-toggle"
    ).element;

    await wrapper.vm.handleOutsideClick({ target: toggleButton });

    expect(wrapper.vm.menuExpanded).toBe(true);
  });
});
