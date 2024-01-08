import { removeTab } from "../workspace";
import { beforeCloseTab } from "../create_tab_functions";
import { createApp } from "vue";
import SnippetTab from "../components/SnippetTab.vue";
import { emitter } from "../emitter";

export let createSnippetTabFunction = function (snippet = null) {
  let snippet_name = "New Snippet";
  let snippet_details = {
    id: null,
    name: null,
    parent: null,
    type: "snippet",
  };

  if (snippet) {
    snippet_name = snippet.name;
    snippet_details = {
      id: snippet.id,
      name: snippet_name,
      parent: snippet.id_parent,
      type: "snippet",
    };
  }

  // Removing last tab of the inner tab list.
  v_connTabControl.snippet_tag.tabControl.removeLastTab();

  let name_html = `
  <span id="tab_title">
  ${snippet_name}
  </span>
  <span id="tab_loading" style="visibility:hidden;">
    <i class="tab-icon node-spin"></i>
  </span>
  <i title="" id="tab_check" style="display: none;" class="fas fa-check-circle tab-icon icon-check"></i>
`;

  let tab = v_connTabControl.snippet_tag.tabControl.createTab({
    p_name: name_html,
    p_selectFunction: function () {
      if (this.tag != null) {
        emitter.emit(`${this.id}_editor_focus`);
        emitter.emit(`${this.id}_resize`)
      }
    },
    p_closeFunction: function (e, tab) {
      let current_tab = tab;
      beforeCloseTab(e, function () {
        current_tab.app.unmount();
        removeTab(current_tab);
      });
    },
  });
  v_connTabControl.snippet_tag.tabControl.selectTab(tab);

  //Adding unique names to spans
  let tab_title_span = document.getElementById("tab_title");
  tab_title_span.id = `tab_title_${tab.id}`;
  let tab_loading_span = document.getElementById("tab_loading");
  tab_loading_span.id = `tab_loading_${tab.id}`;
  let tab_check_span = document.getElementById("tab_check");
  tab_check_span.id = `tab_check_${tab.id}`;

  tab.elementDiv.innerHTML = `<snippet-tab :tab-id="tabId" :snippet="snippet"> </snippet-tab>`;

  const app = createApp({
    components: {
      SnippetTab,
    },
    data() {
      return {
        tabId: tab.id,
        snippet: snippet_details
      };
    },
  });
  tab.app = app;

  app.mount(`#${tab.elementDiv.id}`);

  let tag = {
    tab_id: tab.id,
    mode: "snippet",
    tab_title_span: tab_title_span,
    tab_loading_span: tab_loading_span,
    tab_check_span: tab_check_span,
    tabControl: v_connTabControl.snippet_tag.tabControl,
    snippetObject: snippet_details,
  };

  tab.tag = tag;

  // Creating + tab in the outer tab list
  let add_tab = v_connTabControl.snippet_tag.tabControl.createTab({
    p_name: "+",
    p_close: false,
    p_isDraggable: false,
    p_selectable: false,
    p_clickFunction: function (e) {
      v_connTabControl.tag.createSnippetTextTab();
    },
  });
  add_tab.tag = {
    mode: "add",
  };
};
