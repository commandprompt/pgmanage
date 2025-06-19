ace.define(
  "ace/ext/hoverlink",
  ["require", "exports", "module", "ace/editor", "ace/config", "ace/range"],
  function (require, exports, module) {
    const Editor = require("ace/editor").Editor;
    const Range = require("ace/range").Range;
    const linkRegex = /(?<=\s)https?:\/\/www\.postgresql\.org\/docs\/[^\s"']+/g;
    let hoverState = {
      activeMarker: null,
      activeLink: null,
    };
    require("ace/config").defineOptions(Editor.prototype, "editor", {
      enableHoverLinking: {
        set: function (val) {
          if (val) {
            this.on("click", onClick);
            this.on("mouseout", onMouseOut);
            this.on("mousemove", onMouseMove);
          } else {
            this.off("click", onClick);
            this.off("mouseout", onMouseOut);
            this.off("mousemove", onMouseMove);
          }
        },
        value: false,
      },
      docRegexUrl: {
        set: function (val) {
          this._docRegexUrl = val || linkRegex;
        },
        value: linkRegex,
      },
    });

    function onClick(e) {
      const editor = e.editor;

      if (hoverState.activeLink) {
        editor._emit("linkClick", hoverState.activeLink);
        window.open(hoverState.activeLink.value, "_blank"); // Open link in a new tab
      }
    }
    function onMouseMove(e) {
      const editor = e.editor;
      const position = e.getDocumentPosition();
      const token = findLink(editor, position.row, position.column);
      if (!token) {
        clearHover(editor);
        return;
      }

      // Get pixel position of the token range
      const screenStart = editor.renderer.textToScreenCoordinates(
        position.row,
        token.start
      );
      const screenEnd = editor.renderer.textToScreenCoordinates(
        position.row,
        token.start + token.value.length
      );

      const mouseX = e.domEvent.clientX;

      // Check if the mouse is within the link's pixel bounds
      if (mouseX < screenStart.pageX || mouseX > screenEnd.pageX) {
        clearHover(editor);
        return;
      }
      highlightLink(
        editor,
        position.row,
        token.start,
        token.start + token.value.length
      );
      hoverState.activeLink = token;
    }
    function onMouseOut(e) {
      const editor = e.editor;
      clearHover(editor);
    }

    function findLink(editor, row, column) {
      let session = editor.session;
      let line = session.getLine(row);
      const regExp = editor._docRegexUrl;

      let match = getMatchAround(regExp, line, column);
      if (!match) return;
      match.row = row;
      return match;
    }
    function getMatchAround(regExp, string, col) {
      for (const match of string.matchAll(regExp)) {
        const offset = match.index;
        const length = match[0].length;

        if (offset <= col && offset + length >= col) {
          return {
            start: offset,
            value: match[0],
          };
        }
      }

      return null;
    }

    function clearHover(editor) {
      const session = editor.session;

      if (hoverState.activeMarker !== null) {
        session.removeMarker(hoverState.activeMarker);
        hoverState.activeMarker = null;
      }

      editor.renderer.setCursorStyle("");
      hoverState.activeLink = null;
    }

    function highlightLink(editor, row, start, end) {
      clearHover(editor);

      editor.renderer.setCursorStyle("pointer");
      const session = editor.session;
      const range = new Range(row, start, row, end);
      const markerId = session.addMarker(
        range,
        "ace_link_marker",
        "text",
        true
      );
      hoverState.activeMarker = markerId;
    }
  }
);
(function () {
  ace.require(["ace/ext/hoverlink"], function (m) {
    if (typeof module == "object" && typeof exports == "object" && module) {
      module.exports = m;
    }
  });
})();
