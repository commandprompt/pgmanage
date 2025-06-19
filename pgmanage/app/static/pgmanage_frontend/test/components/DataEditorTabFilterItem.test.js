import { mount } from "@vue/test-utils";
import { describe, test, expect, beforeEach } from "vitest";
import DataEditorTabFilterItem from "@/components/DataEditorTabFilterItem.vue";

describe("FilterItem.vue", () => {
  let wrapper;
  const columns = ["id", "name", "email"];
  const comparisonOperators = ["=", "!=", "<", "<=", ">", ">=", "in"];
  const value = {
    column: "id",
    operator: "=",
    value: "123",
    condition: "AND",
  };

  beforeEach(() => {
    wrapper = mount(DataEditorTabFilterItem, {
      props: { columns, comparisonOperators, value },
    });
  });

  test("renders correctly with initial props", () => {
    const columnSelect = wrapper.find('select[data-testid="column-select"]');
    const operatorSelect = wrapper.find(
      'select[data-testid="operator-select"]'
    );
    const valueInput = wrapper.find('input[data-testid="value-input"]');

    expect(columnSelect.exists()).toBe(true);
    expect(operatorSelect.exists()).toBe(true);
    expect(valueInput.exists()).toBe(true);

    expect(columnSelect.element.value).toBe("id");
    expect(operatorSelect.element.value).toBe("=");
    expect(valueInput.element.value).toBe("123");
  });

  test("emits update event when inputs change", async () => {
    const columnSelect = wrapper.find('select[data-testid="column-select"]');
    const operatorSelect = wrapper.find(
      'select[data-testid="operator-select"]'
    );
    const valueInput = wrapper.find('input[data-testid="value-input"]');

    await columnSelect.setValue("name");
    expect(wrapper.emitted("update")[0][0]).toEqual({
      column: "name",
      operator: "=",
      value: "123",
      condition: "AND",
    });

    await operatorSelect.setValue("!=");
    expect(wrapper.emitted("update")[1][0]).toEqual({
      column: "name",
      operator: "!=",
      value: "123",
      condition: "AND",
    });

    await valueInput.setValue("456");
    expect(wrapper.emitted("update")[2][0]).toEqual({
      column: "name",
      operator: "!=",
      value: "456",
      condition: "AND",
    });
  });

  test("updates local data when value prop changes", async () => {
    await wrapper.setProps({
      value: {
        column: "email",
        operator: "in",
        value: "test@example.com,example@test.com",
        condition: "OR",
      },
    });

    expect(wrapper.vm.selectedColumn).toBe("email");
    expect(wrapper.vm.selectedOperator).toBe("in");
    expect(wrapper.vm.filterValue).toBe("test@example.com,example@test.com");
    expect(wrapper.vm.condition).toBe("OR");

    const columnSelect = wrapper.find('select[data-testid="column-select"]');
    const operatorSelect = wrapper.find(
      'select[data-testid="operator-select"]'
    );
    const valueInput = wrapper.find('input[data-testid="value-input"]');

    expect(columnSelect.element.value).toBe("email");
    expect(operatorSelect.element.value).toBe("in");
    expect(valueInput.element.value).toBe("test@example.com,example@test.com");
  });
});
