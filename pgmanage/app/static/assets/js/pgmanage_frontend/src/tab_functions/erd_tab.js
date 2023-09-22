import {
    removeTab,
    showMenuNewTab,
  } from "../workspace";

import { createApp } from "vue";
import ERDTab from "../components/ERDTab.vue";
import { beforeCloseTab } from "../create_tab_functions";

export let  createERDTabFunction = function(schema = '') {
    let tab_name = schema ? `ERD: ${schema}` : 'ERD'
    v_connTabControl.selectedTab.tag.tabControl.removeLastTab();

    let tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
      p_icon: `<i class="fab fa-hubspot icon-tab-title"></i>`,
      p_name: '<span id="tab_title">' + tab_name + '</span><span id="tab_loading" style="visibility:hidden;"><i class="tab-icon node-spin"></i></span><i title="" id="tab_check" style="display: none;" class="fas fa-check-circle tab-icon icon-check"></i>',
      p_selectFunction: function() {
        document.title = 'PgManage'
        if(this.tag != null) {
          this.tag.resize();
        }
      },
      p_closeFunction: function(e,p_tab) {
        var current_tab = p_tab;
        beforeCloseTab(e,
          function() {
            current_tab.app.unmount();
            removeTab(current_tab);
        });
      }
    });

    v_connTabControl.selectedTab.tag.tabControl.selectTab(tab);

    tab.elementDiv.innerHTML = `
    <div id='erd_tab_${tab.id}' class="pt-3">
      <erd-tab></erd-tab>
    </div>`;

    let tag = {
        tab_id: tab.id,
        div_result: document.getElementById(`erd_tab_${tab.id}`),
    };
    tab.tag = tag;

    let app = createApp({
      components: {
          "erd-tab": ERDTab,
      }
      },{
        database_index: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        database_name: v_connTabControl.selectedTab.tag.selectedDatabase,
        tab_id: v_connTabControl.selectedTab.id,
        schema: schema,
    });

    tab.app = app;
    app.mount(`#erd_tab_${tab.id}`);

    let add_tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
        p_name: "+",
        p_close: false,
        p_isDraggable: false,
        p_selectable: false,
        p_clickFunction: function (e) {
          showMenuNewTab(e);
        },
    });

    add_tab.tag = {
        mode: "add",
    };
  };