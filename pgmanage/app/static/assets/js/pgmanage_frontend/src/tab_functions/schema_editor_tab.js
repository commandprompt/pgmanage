import { beforeCloseTab } from "../create_tab_functions";
import { removeTab, showMenuNewTab } from "../workspace";
import SchemaEditorTab from "../components/SchemaEditorTab.vue";
import { createApp } from "vue";

let createSchemaEditorTab = function (node, mode, dialect) {
    // Removing last tab of the inner tab list
  v_connTabControl.selectedTab.tag.tabControl.removeLastTab();
  let table_name = node.title.replace(/^"(.*)"$/, '$1')
  let tab_title = mode === 'alter' ? `Alter: ${table_name}` : 'New Table'
  let tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_name: tab_title,
    p_closeFunction: function(e, tab) {
        let current_tab = tab;
        beforeCloseTab(e,
          function() {
            current_tab.app.unmount()
            removeTab(current_tab);
          });
      }
  });

  // Selecting the newly created tab
  v_connTabControl.selectedTab.tag.tabControl.selectTab(tab);

  tab.elementDiv.innerHTML =`
  <div id='schema_editor_tab_${tab.id}' class="pt-3">
    <schema-editor-tab></schema-editor-tab>
  </div>`

  let tag = {
    tab_id : tab.id,
    div_result: document.getElementById(`schema_editor_tab_${tab.id}`,
    )
  }
  tab.tag = tag
  tab.tag.tempData = [];

  let app = createApp({
    components: {
        'schema-editor-tab': SchemaEditorTab,
    },

  },{ dialect: dialect || 'postgres',
      database_index: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
      database_name:v_connTabControl.selectedTab.tag.selectedDatabase,
      tab_id: v_connTabControl.selectedTab.id,
      mode: mode,
      schema: node.data.schema,
      table: mode === 'alter' ? table_name : null,
      tree_node: node,
      tree: window.v_connTabControl.selectedTab.tag.tree
    })

  // save app referece in the tab, it will be later used to restroy app instance on tab close
  tab.app = app;
  app.mount(`#schema_editor_tab_${tab.id}`);

  let add_tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_name: '+',
    p_close: false,
    p_isDraggable: false,
    p_selectable: false,
    p_clickFunction: function(e) {
      showMenuNewTab(e);
    }
  });

  add_tab.tag = {
    mode: 'add',
    database: v_connTabControl.selectedTab.tag.selectedDatabase
  }

}

export { createSchemaEditorTab }