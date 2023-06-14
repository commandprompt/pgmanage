import CronCore, { RenderlessSelect } from '@vue-js-cron/core';
import { resolveComponent, openBlock, createBlock, mergeProps, withCtx, createElementVNode, toDisplayString, createElementBlock, Fragment, renderList, normalizeClass, withModifiers, createCommentVNode, createVNode, toHandlers } from 'vue';

var script$1 = {
  inheritAttrs: false,
  components: {
    RenderlessSelect
  },
  name: 'CustomSelect',
  props: {},
  emits: ['update:model-value'],
  data () {
    return {
      menu: false
    }
  },
  methods: {
    menuEvtListener (evt) {
      this.menu = false;
      document.removeEventListener('click', this.menuEvtListener);
    },
    toggleMenu () {
      this.menu = !this.menu;

      if (this.menu) {
        setTimeout(() => {
          document.addEventListener('click', this.menuEvtListener);
        }, 1);
      } else {
        document.removeEventListener('click', this.menuEvtListener);
      }
    }
  }
};

const _hoisted_1$1 = { class: "vcron-select-container" };
const _hoisted_2$1 = {
  key: 0,
  class: "vcron-select-list"
};
const _hoisted_3$1 = ["onClick"];
const _hoisted_4 = { key: 0 };

function render$1(_ctx, _cache, $props, $setup, $data, $options) {
  const _component_renderless_select = resolveComponent("renderless-select");

  return (openBlock(), createBlock(_component_renderless_select, mergeProps(_ctx.$attrs, {
    "onUpdate:modelValue": _cache[1] || (_cache[1] = $event => (_ctx.$emit('update:model-value', $event)))
  }), {
    default: withCtx(({ selectedStr, itemRows, select, isSelected, multiple }) => [
      createElementVNode("div", _hoisted_1$1, [
        createElementVNode("span", {
          class: "vcron-select-input",
          onClick: _cache[0] || (_cache[0] = (...args) => ($options.toggleMenu && $options.toggleMenu(...args)))
        }, toDisplayString(selectedStr), 1 /* TEXT */),
        ($data.menu)
          ? (openBlock(), createElementBlock("div", _hoisted_2$1, [
              (openBlock(true), createElementBlock(Fragment, null, renderList(itemRows, (row, i) => {
                return (openBlock(), createElementBlock("div", {
                  class: "vcron-select-row",
                  key: i
                }, [
                  (openBlock(true), createElementBlock(Fragment, null, renderList(row, (item, j) => {
                    return (openBlock(), createElementBlock("div", {
                      key: i+'-'+j,
                      class: normalizeClass(["vcron-select-col", {'vcron-select-selected': isSelected(item)}]),
                      onClick: [
                        $event => (select(item)),
                        withModifiers($event => (multiple ? () => {} : $options.toggleMenu()), ["stop"])
                      ]
                    }, [
                      item
                        ? (openBlock(), createElementBlock("div", _hoisted_4, toDisplayString(item.text), 1 /* TEXT */))
                        : createCommentVNode("v-if", true)
                    ], 10 /* CLASS, PROPS */, _hoisted_3$1))
                  }), 128 /* KEYED_FRAGMENT */))
                ]))
              }), 128 /* KEYED_FRAGMENT */))
            ]))
          : createCommentVNode("v-if", true)
      ])
    ]),
    _: 1 /* STABLE */
  }, 16 /* FULL_PROPS */))
}

script$1.render = render$1;
script$1.__file = "src/components/CustomSelect.vue";

var script = {
  name: 'VueCronEditor',
  components: {
    CronCore: CronCore.component,
    CustomSelect: script$1
  },
  props: {
    cols: {
      type: Object,
      default: () => {
        return {
          minute: 5,
          hour: 4,
          day: 4
        }
      }
    }
  },
  emits: ['update:model-value', 'error']
};

const _hoisted_1 = { class: "vcron-editor" };
const _hoisted_2 = { class: "vcron-l-spacer" };
const _hoisted_3 = { class: "vcron-l-spacer" };

function render(_ctx, _cache, $props, $setup, $data, $options) {
  const _component_custom_select = resolveComponent("custom-select");
  const _component_CronCore = resolveComponent("CronCore");

  return (openBlock(), createBlock(_component_CronCore, mergeProps(_ctx.$attrs, {
    "onUpdate:modelValue": _cache[0] || (_cache[0] = $event => (_ctx.$emit('update:model-value', $event))),
    onError: _cache[1] || (_cache[1] = $event => (_ctx.$emit('error', $event)))
  }), {
    default: withCtx(({fields, period}) => [
      createElementVNode("span", _hoisted_1, [
        createElementVNode("span", null, toDisplayString(period.prefix), 1 /* TEXT */),
        createElementVNode("div", _hoisted_2, [
          createVNode(_component_custom_select, mergeProps(period.attrs, toHandlers(period.events), {
            items: period.items,
            "item-value": "id",
            cols: $props.cols['period'] || 1
          }), null, 16 /* FULL_PROPS */, ["items", "cols"])
        ]),
        createElementVNode("span", null, toDisplayString(period.suffix), 1 /* TEXT */),
        (openBlock(true), createElementBlock(Fragment, null, renderList(fields, (f) => {
          return (openBlock(), createElementBlock(Fragment, {
            key: f.id
          }, [
            createElementVNode("span", null, toDisplayString(f.prefix), 1 /* TEXT */),
            createElementVNode("div", _hoisted_3, [
              createVNode(_component_custom_select, mergeProps(f.attrs, toHandlers(f.events), {
                items: f.items,
                cols: $props.cols[f.id] || 1,
                selection: f.selectedStr,
                multiple: ""
              }), null, 16 /* FULL_PROPS */, ["items", "cols", "selection"])
            ]),
            createElementVNode("span", null, toDisplayString(f.suffix), 1 /* TEXT */)
          ], 64 /* STABLE_FRAGMENT */))
        }), 128 /* KEYED_FRAGMENT */))
      ])
    ]),
    _: 1 /* STABLE */
  }, 16 /* FULL_PROPS */))
}

script.render = render;
script.__file = "src/CronEditor.vue";

// Import vue component

// Declare install function executed by Vue.use()
function install (Vue) {
  if (install.installed) return
  install.installed = true;
  Vue.component('CronLight', script);
}

// Create module definition for Vue.use()
const plugin = {
  install,
  component: script,
  util: CronCore.util
};

export { script as CronLight, plugin as CronLightPlugin, plugin as default, install };
