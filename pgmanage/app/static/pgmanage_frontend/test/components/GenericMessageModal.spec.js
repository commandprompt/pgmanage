import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { mount } from "@vue/test-utils";
import GenericMessageModal from "@src/components/GenericMessageModal.vue";
import { useMessageModalStore } from "@src/stores/message_modal";
import { Modal } from "bootstrap";

vi.mock("bootstrap", () => ({
  Modal: {
    getOrCreateInstance: vi.fn(() => ({
      show: vi.fn(),
      hide: vi.fn(),
    })),
  },
}));

describe("GenericMessageModal.vue", () => {
  let store;

  beforeEach(() => {
    store = useMessageModalStore();
  });

  afterEach(() => {
    store.$reset();
  });

  it("renders message and checkboxes from store", async () => {
    store.message = "Test Message";
    store.checkboxes = [
      { label: "Option 1", checked: false },
      { label: "Option 2", checked: true },
    ];

    const wrapper = mount(GenericMessageModal, {
      global: {
        stubs: {
          teleport: true,
        },
      },
    });

    expect(wrapper.text()).toContain("Test Message");
    const checkboxInputs = wrapper.findAll('input[type="checkbox"]');
    expect(checkboxInputs.length).toBe(2);
    expect(checkboxInputs[0].element.checked).toBe(false);
    expect(checkboxInputs[1].element.checked).toBe(true);
  });

  it("calls store methods on button clicks", async () => {
    store.executeSuccess = vi.fn();
    store.executeCancel = vi.fn();
    const wrapper = mount(GenericMessageModal, {
      global: {
        stubs: {
          teleport: true,
        },
      },
    });

    await wrapper.find("#generic_modal_message_yes").trigger("click");
    expect(store.executeSuccess).toHaveBeenCalled();

    await wrapper.find("#generic_modal_message_no").trigger("click");
    expect(store.executeCancel).toHaveBeenCalled();
  });

  it("conditionally shows close button when closable is true", () => {
    store.closable = true;
    const wrapper = mount(GenericMessageModal, {
      global: {
        stubs: {
          teleport: true,
        },
      },
    });
    const closeBtn = wrapper.find("button.btn-close");
    expect(closeBtn.exists()).toBe(true);
  });

  it("hides close button when closable is false", () => {
    store.closable = false;
    const wrapper = mount(GenericMessageModal, {
      global: {
        stubs: {
          teleport: true,
        },
      },
    });
    const closeBtn = wrapper.find("button.btn-close");
    expect(closeBtn.exists()).toBe(false);
  });

  it("calls store.hideModal when close button is clicked", async () => {
    const hideModalSpy = vi.spyOn(store, "hideModal");
    const wrapper = mount(GenericMessageModal, {
      global: {
        stubs: {
          teleport: true,
        },
      },
    });
    store.showModal("test message");
    await wrapper.find("button.btn-close").trigger("click");
    expect(hideModalSpy).toHaveBeenCalled();
  });

  it("shows modal on store.showModal and hides on store.hideModal", async () => {
    const wrapper = mount(GenericMessageModal, {
      global: {
        stubs: {
          teleport: true,
        },
      },
    });

    store.showModal("test message");

    expect(Modal.getOrCreateInstance).toHaveBeenCalled();
    expect(wrapper.vm.modalInstance.show).toHaveBeenCalled();

    store.hideModal();

    expect(wrapper.vm.modalInstance.hide).toHaveBeenCalled();
  });
});
