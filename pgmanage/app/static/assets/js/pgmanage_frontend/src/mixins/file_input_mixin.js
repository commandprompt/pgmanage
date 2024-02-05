import {
  allowedFileTypes,
  maxFileSizeInKB,
  maxFileSizeInMB,
} from "../constants";
import { showToast } from "../notification_control";
import { emitter } from "../emitter";

export default {
  methods: {
    handleFileInputChange(e) {
      const [file] = e.target.files;
      if (!file?.type || !allowedFileTypes.includes(file.type)) {
        showToast("error", `File with type '${file.type}' is not supported.`);
        return;
      }

      if (file.size > maxFileSizeInKB) {
        showToast(
          "error",
          `Please select a file that is ${maxFileSizeInMB}MB or less.`
        );
        return;
      }
      try {
        if (window.FileReader) {
          let reader = new FileReader();
          reader.onload = () => {
            emitter.emit(`${this.tabId}_copy_to_editor`, reader.result);
          };
          reader.readAsText(file);
        }
      } catch (err) {
        showToast("error", err);
        e.preventDefault();
        e.stopPropagation();
      }
    },
  },
};
