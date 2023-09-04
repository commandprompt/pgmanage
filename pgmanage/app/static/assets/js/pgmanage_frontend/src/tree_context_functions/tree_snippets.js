/*
This file is part of OmniDB.
OmniDB is open-source software, distributed "AS IS" under the MIT license in the hope that it will be useful.

The MIT License (MIT)

Portions Copyright (c) 2015-2020, The OmniDB Team
Portions Copyright (c) 2017-2020, 2ndQuadrant Limited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import { createApp } from 'vue'
import TreeSnippets from '../components/TreeSnippets.vue'
import ContextMenu from '@imengyu/vue3-context-menu';
import axios from 'axios';
import { showConfirm, showToast } from '../notification_control';
import { snippetsStore } from '../stores/snippets';

function getAllSnippets() {
  axios.get("/get_all_snippets/").then((resp) => {
    v_connTabControl.tag.globalSnippets = resp.data;
    snippetsStore.$patch({
      files: resp.data.files,
      folders: resp.data.folders
    })
  })
  .catch((error) => {
    console.log(error)
  })
}

/// <summary>
/// Retrieving tree.
/// </summary>
function getTreeSnippets(div) {
  const div_tree = document.getElementById(div);
  div_tree.innerHTML =
    '<tree-snippets :snippet-tag="snippetTag"></tree-snippets>';
  const app = createApp({
    components: {
      "tree-snippets": TreeSnippets
    },
    data() {
      return {
        snippetTag: v_connTabControl.snippet_tag,
      };
    },
  });
  app.mount(`#${div}`);
}

function saveSnippetText(event) {
  let callback = function (return_object) {
    v_connTabControl.snippet_tag.tabControl.selectedTab.tag.snippetObject =
      return_object;
    v_connTabControl.snippet_tag.tabControl.selectedTab.tag.tab_title_span.innerHTML =
      return_object.name;
  };
  let snippet_object =
    v_connTabControl.snippet_tag.tabControl.selectedTab.tag.snippetObject;
  if (snippet_object.id !== null) {
    let save_object = {
      id: snippet_object.id,
      name: snippet_object.name,
      parent: snippet_object.parent,
    };
    saveSnippetTextConfirm(
      save_object,
      v_connTabControl.snippet_tag.tabControl.selectedTab.tag.editor.getValue(),
      callback
    );
  } else {
    ContextMenu.showContextMenu({
      theme: 'pgmanage',
      x: event.x,
      y: event.y,
      zIndex: 1000,
      minWidth: 230,
      items: buildSnippetContextMenuObjects(
        "save",
        snippetsStore,
        v_connTabControl.snippet_tag.tabControl.selectedTab.tag.editor,
        callback
      )
    })
  }
}

function snippetTreeFindNode(id, current_node) {
  let node = null;

  current_node.children.forEach((el) => {
    if (el.data.id === id) {
      return el;
    } else {
      node = snippetTreeFindNode(id, el);
      if (node !== null) return node;
    }
  });

  return node;
}

function saveSnippetTextConfirm(save_object, text, callback) {
  axios
    .post("/save_snippet_text/", {
      id: save_object.id,
      parent_id: save_object.parent,
      name: save_object.name,
      text: text,
    })
    .then((resp) => {
      let node = null;
      if (resp.data.parent === null) {
        node = v_connTabControl.snippet_tree.getRootNode();
      } else {
        node = snippetTreeFindNode(
          resp.data.parent,
          v_connTabControl.snippet_tree.getRootNode()
        );
      }
      if (node != null) {
        v_connTabControl.snippet_tree.refreshTree(node);
      }

      if (callback != null) {
        callback(resp.data);
      }

      showToast("success", "Snippet saved.")

      getAllSnippets();
    });
}

function executeSnippet(id, editor) {
  axios
    .post("/get_snippet_text/", {
      snippet_id: id,
    })
    .then((resp) => {
      editor.insert(resp.data.data);
      editor.clearSelection();
    });
}

function buildSnippetContextMenuObjects(mode, object, editor, callback) {
  let elements = [];

  if (mode == "save") {
    elements.push({
      label: "New Snippet",
      icon: "fas cm-all fa-save",
      onClick: function () {
        showConfirm(
          '<input id="element_name" class="form-control" placeholder="Snippet Name" style="width: 100%;">',
          function () {
            saveSnippetTextConfirm(
              {
                id: null,
                name: document.getElementById("element_name").value,
                parent: object.id,
              },
              editor.getValue(),
              callback
            );
          },
          null,
          function () {
            let input = document.getElementById("element_name");
            input.focus();
            input.selectionStart = 0;
            input.selectionEnd = 10000;
          }
        );
      },
    });
  }

  for (let i = 0; i < object.files.length; i++)
    (function (i) {
      let file = object.files[i];

      if (mode == "save")
        elements.push({
          label: `Overwrite ${file.name}`,
          icon: "fas cm-all fa-align-left",
          onClick: function () {
            showConfirm(
              `<b>WARNING</b>, are you sure you want to overwrite file ${file.name}?`,
              function () {
                saveSnippetTextConfirm(
                  {
                    id: file.id,
                    name: null,
                    parent: null,
                  },
                  editor.getValue(),
                  callback
                );
              }
            );
          },
        });
      else
        elements.push({
          label: file.name,
          icon: "fas cm-all fa-align-left",
          onClick: function () {
            executeSnippet(file.id, editor);
          },
        });
    })(i);

  for (let i = 0; i < object.folders.length; i++)
    (function (i) {
      let folder = object.folders[i];
      elements.push({
        label: folder.name,
        icon: "fas cm-all fa-folder",
        children: buildSnippetContextMenuObjects(mode, folder, editor, callback),
      });
    })(i);

  return elements;
}

export { buildSnippetContextMenuObjects, getTreeSnippets, getAllSnippets, saveSnippetText }