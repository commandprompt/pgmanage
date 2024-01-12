import { createApp } from "vue";
import TreeSnippets from "../components/TreeSnippets.vue";
import axios from "axios";
import { showConfirm, showToast } from "../notification_control";
import { snippetsStore } from "../stores/stores_initializer";
import { emitter } from "../emitter";

function getAllSnippets() {
  axios
    .get("/get_all_snippets/")
    .then((resp) => {
      snippetsStore.$patch({
        files: resp.data.files,
        folders: resp.data.folders,
      });
    })
    .catch((error) => {
      showToast("error", error);
    });
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
      "tree-snippets": TreeSnippets,
    },
    data() {
      return {
        snippetTag: v_connTabControl.snippet_tag,
      };
    },
  });
  app.mount(`#${div}`);
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
      emitter.emit(
        `${v_connTabControl.snippet_tag.tab_id}_refresh_snippet_tree`,
        resp.data.parent
      );

      if (callback != null) {
        callback(resp.data);
      }

      showToast("success", "Snippet saved.");

      getAllSnippets();
    })
    .catch((error) => {
      showToast("error", error);
    });
}

function executeSnippet(id) {
  axios
    .post("/get_snippet_text/", {
      snippet_id: id,
    })
    .then((resp) => {
      emitter.emit(
        `${v_connTabControl.selectedTab.tag.tabControl.selectedTab.id}_insert_to_editor`,
        resp.data.data
      );
    })
    .catch((error) => {
      showToast("error", error.response?.data?.data ?? error);
    });
}

function buildSnippetContextMenuObjects(mode, object, snippetText, callback) {
  let elements = [];
  const isSaveMode = mode === "save";

  const handleSaveConfirmation = (file, folder) => {
    showConfirm(
      `<b>WARNING</b>, are you sure you want to overwrite file ${file.name}?`,
      () => {
        saveSnippetTextConfirm(
          {
            id: file.id,
            name: null,
            parent: folder.id,
          },
          snippetText,
          callback
        );
      }
    );
  };

  if (isSaveMode) {
    elements.push({
      label: "New Snippet",
      icon: "fas cm-all fa-save",
      onClick: function () {
        showConfirm(
          '<input id="element_name" class="form-control" placeholder="Snippet Name" style="width: 100%;">',
          function () {
            const snippetName = document.getElementById("element_name").value;

            if (!snippetName) {
              showToast("error", "Name cannot be empty.");
              return;
            }
            saveSnippetTextConfirm(
              {
                id: null,
                name: snippetName,
                parent: object.id,
              },
              snippetText,
              callback
            );
          },
          null,
          function () {
            let input = document.getElementById("element_name");
            input.focus();
            input.select();
          }
        );
      },
    });
  }

  object.files.forEach((file) => {
    elements.push({
      label: isSaveMode ? `Overwrite ${file.name}` : file.name,
      icon: "fas cm-all fa-align-left",
      onClick: isSaveMode
        ? () => handleSaveConfirmation(file, object)
        : () => executeSnippet(file.id),
    });
  });

  object.folders.forEach((folder) => {
    elements.push({
      label: folder.name,
      icon: "fas cm-all fa-folder",
      children: buildSnippetContextMenuObjects(
        mode,
        folder,
        snippetText,
        callback
      ),
    });
  });

  return elements;
}

export {
  buildSnippetContextMenuObjects,
  getTreeSnippets,
  getAllSnippets,
  saveSnippetTextConfirm,
  executeSnippet,
};
