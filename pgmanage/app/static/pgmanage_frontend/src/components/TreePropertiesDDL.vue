<template>
  <div class="omnidb__tree-tabs h-100 position-relative">
    <Transition :duration="100">
      <div v-if="showLoading" class="div_loading d-block" style="z-index: 1000">
        <div class="div_loading_cover"></div>
        <div class="div_loading_content">
          <div
            class="spinner-border text-primary"
            style="width: 4rem; height: 4rem"
            role="status"
          >
            <span class="sr-only">Loading...</span>
          </div>
        </div>
      </div>
    </Transition>

    <button
      data-testid="tree-tabs-toggler"
      type="button"
      class="btn btn-icon btn-icon-secondary omnidb__tree-tabs__toggler me-2"
      @click="$emit('toggleTreeTabs')"
    >
      <i class="fas fa-arrows-alt-v"></i>
    </button>
    <div
      class="omnidb__tree-tabs__container omnidb__tab-menu--container h-100 position-relative"
    >
      <div class="omnidb__tab-menu border-bottom">
        <nav>
          <div class="nav nav-tabs">
            <a
              :id="`${workspaceId}_tree_properties_nav`"
              class="omnidb__tab-menu__link nav-item nav-link active"
              data-bs-toggle="tab"
              role="tab"
              aria-selected="true"
              ref="treePropertiesNav"
              :href="`#${workspaceId}_tree_properties`"
              :aria-controls="`${workspaceId}_tree_properties`"
              >Properties</a
            >
            <a
              :id="`${workspaceId}_tree_ddl_nav`"
              class="omnidb__tab-menu__link nav-item nav-link"
              data-bs-toggle="tab"
              role="tab"
              aria-selected="false"
              :href="`#${workspaceId}_tree_ddl`"
              :aria-controls="`${workspaceId}_tree_ddl`"
              >DDL</a
            >
          </div>
        </nav>
      </div>

      <div
        class="tab-content omnidb__tab-content h-100 pb-2"
        style="min-width: 100px"
      >
        <div
          class="tab-pane active"
          :id="`${workspaceId}_tree_properties`"
          role="tabpanel"
          :aria-labelledby="`${workspaceId}_tree_properties_nav`"
          style="height: 90%"
        >
          <div ref="tabulator" class="tabulator-custom simple pt-2"></div>
        </div>

        <div
          class="tab-pane h-100"
          :id="`${workspaceId}_tree_ddl`"
          role="tabpanel"
          :aria-labelledby="`${workspaceId}_tree_ddl_nav`"
        >
          <div ref="editor" class="pb-3" style="height: 90%"></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { TabulatorFull as Tabulator } from "tabulator-tables";
import { settingsStore } from "../stores/stores_initializer";

export default {
  props: {
    workspaceId: String,
    databaseTechnology: String,
    ddlData: String,
    propertiesData: Array,
    showLoading: {
      type: Boolean,
      default: false,
    },
  },
  emits: ["toggleTreeTabs"],
  data() {
    return {
      table: null,
      editor: null,
    };
  },
  watch: {
    ddlData(newValue, oldValue) {
      this.editor.setValue(this.ddlData);
      this.editor.clearSelection();
      this.editor.gotoLine(0, 0, true);
    },
    propertiesData(newValue, oldValue) {
      this.table.setData(newValue);
    },
  },
  mounted() {
    this.setupEditor();
    this.setupTable();

    settingsStore.$subscribe((mutation, state) => {
      this.editor.setTheme(`ace/theme/${state.editorTheme}`);
      this.editor.setFontSize(state.fontSize);
    });

    this.$refs.treePropertiesNav.addEventListener("shown.bs.tab", () => {
      this.table.redraw(true);
    });
  },
  methods: {
    setupTable() {
      this.table = new Tabulator(this.$refs.tabulator, {
        renderVertical: "basic",
        columnDefaults: {
          headerHozAlign: "left",
          headerSort: false,
        },
        height: "100%",
        data: [],
        columns: [
          {
            title: "property",
            field: "0",
            resizable: false,
          },
          {
            title: "value",
            field: "1",
            resizable: false,
          },
        ],
        layout: "fitColumns",
        selectableRows: false,
      });
    },
    setupEditor() {
      // TODO: DRY editor initialization. possibly move common setup stuff into a mixin
      const EDITOR_MODEMAP = {
        postgresql: "pgsql",
        mysql: "mysql",
        mariadb: "mysql",
        oracle: "plsql",
      };

      let editor_mode = EDITOR_MODEMAP[this.databaseTechnology] || "sql";

      this.editor = ace.edit(this.$refs.editor);
      this.editor.$blockScrolling = Infinity;
      this.editor.setTheme("ace/theme/" + settingsStore.editorTheme);
      this.editor.session.setMode(`ace/mode/${editor_mode}`);

      this.editor.setFontSize(Number(settingsStore.fontSize));

      this.editor.commands.bindKey("ctrl-space", null);

      //Remove shortcuts from ace in order to avoid conflict with omnidb shortcuts
      this.editor.commands.bindKey("Cmd-,", null);
      this.editor.commands.bindKey("Ctrl-,", null);
      this.editor.commands.bindKey("Cmd-Delete", null);
      this.editor.commands.bindKey("Ctrl-Delete", null);
      this.editor.commands.bindKey("Ctrl-Up", null);
      this.editor.commands.bindKey("Ctrl-Down", null);
      this.editor.setReadOnly(true);
      this.editor.resize();
    },
  },
};
</script>
