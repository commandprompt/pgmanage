import { allowedFileTypes } from "../constants";
import { showToast } from "../notification_control";
import { emitter } from "../emitter";

export default {
  methods: {
    handleFileInputChange(e) {
      const [file] = e.target.files;
      if (!file?.type || !allowedFileTypes.includes(file.type))
        return showToast(
          "error",
          `File with type '${file.type}' is not supported.`
        );
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
