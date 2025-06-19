import { showToast } from "../notification_control";
import {
  allowedFileTypes,
  maxFileSizeInMB,
  maxFileSizeInKB,
  mimeTypeMap,
} from "../constants";
import { messageModalStore, tabsStore } from "../stores/stores_initializer";

function setupAceDragDrop(editor, isSnippetTab = false) {
  function handleFileDrop(e, file) {
    try {
      if (window.FileReader) {
        let reader = new FileReader();
        reader.onload = () => {
          editor.session.setValue(reader.result);
        };
        reader.readAsText(file);

        let selectedTab;

        if (isSnippetTab) {
          let snippetPanel = tabsStore.tabs.find(
            (tab) => tab.name === "Snippets"
          );
          selectedTab = snippetPanel.metaData.selectedTab;
        } else {
          selectedTab = tabsStore.selectedPrimaryTab.metaData.selectedTab;
        }
        selectedTab.name = file.name;
        selectedTab.metaData.editingFile = true;
      }
      return e.preventDefault();
    } catch (err) {
      showToast("error", err);
      e.preventDefault();
      e.stopPropagation();
      return false;
    }
  }

  editor.container.addEventListener("dragover", (e) => {
    let types = e.dataTransfer.types;
    if (types && Array.prototype.indexOf.call(types, "Files") !== -1) {
      return e.preventDefault(e);
    }
  });

  editor.container.addEventListener("drop", (e) => {
    let file, fileType, fileExtension;

    if (e?.dataTransfer?.files?.length > 1) {
      showToast("error", "Only one file at a time is possible to drop");
      e.preventDefault();
      e.stopPropagation();
      return false;
    }
    file = e.dataTransfer.files[0];

    fileType = file.type === "" ? "" : file.type;

    if (fileType === "") {
      let splitName = file.name.split(".");
      fileExtension = splitName[splitName.length - 1].toLowerCase();

      fileType = mimeTypeMap[fileExtension] ?? "text/plain";
    }

    if (!allowedFileTypes.includes(fileType)) {
      showToast("error", `File with type '${fileType}' is not supported.`);
      e.preventDefault();
      e.stopPropagation();
      return false;
    }

    if (file.size > maxFileSizeInKB) {
      showToast(
        "error",
        `Please drop a file that is ${maxFileSizeInMB}MB or less.`
      );
      e.preventDefault();
      e.stopPropagation();
      return false;
    }

    if (!!editor.getValue()) {
      e.preventDefault();
      messageModalStore.showModal(
        "Are you sure you wish to discard the current changes?",
        () => {
          handleFileDrop(e, file);
        },
        () => {
          e.preventDefault();
          e.stopPropagation();
        }
      );
    } else {
      handleFileDrop(e, file);
    }
  });
}

function setupAceSelectionHighlight(editor) {
  editor.selection.on("changeSelection", () => {
    // Get the selected text
    let selectedText = editor.getSelectedText();

    if (selectedText.length > 0) {
      let escapedText = selectedText.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

      // Find other occurrences of the selected text
      let pattern = new RegExp(escapedText, "gi");

      editor.getSession().highlight(pattern);
      editor.getSession().$searchHighlight.clazz = "ace_selection";
    }
  });
}

export { setupAceDragDrop, setupAceSelectionHighlight };
