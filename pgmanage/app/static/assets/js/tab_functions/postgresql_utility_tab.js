let createUtilityTab = function (node, utility, backup_type = "objects") {
  // Removing last tab of the inner tab list
  v_connTabControl.selectedTab.tag.tabControl.removeLastTab();
  let utility_title =
    backup_type === "objects" ? `(${node.tag.type}:${node.text})` : backup_type;
  let tab_name = `${utility} ${utility_title}`;
  let mode = utility.toLowerCase();

  let tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_name: `<span>${tab_name}</span>`,
    p_closeFunction: function (e, tab) {
      let current_tab = tab;
      beforeCloseTab(e, function () {
        current_tab.app.unmount();
        removeTab(current_tab);
      });
    },
  });

  // Selecting the newly created tab
  v_connTabControl.selectedTab.tag.tabControl.selectTab(tab);

  tab.elementDiv.innerHTML = `
  <div id='${mode}_tab_${tab.id}' class="backup-tab-scrollable pt-3">
    <component :is="currentComp" v-bind="currentProps"></component>
  </div>`;
  let tag = {
    tab_id: tab.id,
    mode: mode,
    div_result: document.getElementById(`${mode}_tab_${tab.id}`),
  };
  tab.tag = tag;

  const { createApp } = Vue;

  const app = createApp({
    data() {
      return {
        currentUtility: utility,
        backupType: backup_type,
        treeNode: node,
      };
    },
    computed: {
      currentComp() {
        const currentComponent = this.currentUtility;
        return Vue.defineAsyncComponent(() =>
          loadModule(
            `../static/assets/js/vuejs/components/${currentComponent}Tab.vue`,
            options
          )
        );
      },
      currentProps() {
        if (this.currentUtility === "Backup") {
          return { "backup-type": this.backupType, "tree-node": this.treeNode };
        } else if (this.currentUtility === "Restore") {
          return {
            "tree-node": this.treeNode,
            "restore-type": this.backupType,
          };
        }
      },
    },
  });
  app.use(VueToast.ToastPlugin, {
    duration: 0,
  });

  // save app referece in the tab, it will be later used to destroy app instance on tab close
  tab.app = app;
  app.mount(`#${mode}_tab_${tab.id}`);

  let add_tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_name: "+",
    p_close: false,
    p_selectable: false,
    p_clickFunction: function (e) {
      showMenuNewTab(e);
    },
  });
  add_tab.tag = {
    mode: "add",
  };
};
