import axios from "axios";
import { showConfirm, showToast } from "../notification_control";
import { emitter } from "../emitter";
import { tabsStore } from "../stores/stores_initializer";
import { handleError } from "../logging/utils";

function executeSnippet(id) {
  axios
    .post("/get_snippet_text/", {
      snippet_id: id,
    })
    .then((resp) => {
      emitter.emit(
        `${tabsStore.selectedPrimaryTab.metaData.selectedTab.id}_insert_to_editor`,
        resp.data.data
      );
    })
    .catch((error) => {
      handleError(error);
    });
}

function buildSnippetContextMenuObjects(mode, object, snippetText, callback) {
  let elements = [];
  const isSaveMode = mode === "save";

  const handleSaveConfirmation = (file, folder) => {
    showConfirm(
      `<b>WARNING</b>, are you sure you want to overwrite file ${file.name}?`,
      () => {
        emitter.emit("save_snippet_text_confirm", {
          saveObject: {
            id: file.id,
            name: null,
            parent: folder.id,
          },
          text: snippetText,
          callback: callback,
        });
      }
    );
  };

  if (isSaveMode) {
    elements.push({
      label: "New Snippet",
      icon: "fas fa-save",
      onClick: function () {
        showConfirm(
          `<div class="form-group">
              <input id="element_name" class="form-control" placeholder="Snippet Name" style="width: 100%;">
            </div>`,
          function () {
            const snippetName = document.getElementById("element_name").value;

            if (!snippetName) {
              showToast("error", "Name cannot be empty.");
              return;
            }
            emitter.emit("save_snippet_text_confirm", {
              saveObject: {
                id: null,
                name: snippetName,
                parent: object.id,
              },
              text: snippetText,
              callback: callback,
            });
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
      icon: "fas fa-align-left",
      onClick: isSaveMode
        ? () => handleSaveConfirmation(file, object)
        : () => executeSnippet(file.id),
    });
  });

  object.folders.forEach((folder) => {
    elements.push({
      label: folder.name,
      icon: "fas fa-folder",
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

export { buildSnippetContextMenuObjects, executeSnippet };
