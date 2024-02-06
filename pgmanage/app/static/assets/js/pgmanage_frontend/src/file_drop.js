import { createMessageModal, showToast } from "./notification_control";
import {
  allowedFileTypes,
  maxFileSizeInMB,
  maxFileSizeInKB,
} from "./constants";

function setupAceDragDrop(editor) {
  function handleFileDrop(e, file) {
    try {
      if (window.FileReader) {
        let reader = new FileReader();
        reader.onload = () => {
          editor.session.setValue(reader.result);
        };
        reader.readAsText(file);
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
    let file;

    if (e?.dataTransfer?.files?.length > 1) {
      showToast("error", "Only one file at a time is possible to drop");
      e.preventDefault();
      e.stopPropagation();
      return false;
    }
    file = e.dataTransfer.files[0];

    if (!file?.type || !allowedFileTypes.includes(file.type)) {
      showToast("error", `File with type '${file.type}' is not supported.`);
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
      createMessageModal(
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

export { setupAceDragDrop };
