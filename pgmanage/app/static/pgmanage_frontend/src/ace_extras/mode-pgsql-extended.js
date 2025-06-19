ace.define(
  "ace/mode/pgsql_extended",
  ["require", "exports", "ace/lib/oop", "ace/mode/text", "ace/range"],
  function (acequire, exports) {
    const oop = acequire("ace/lib/oop");
    const PgsqlMode = acequire("ace/mode/pgsql").Mode;
    const SqlFoldMode = acequire("./folding/sql").FoldMode;
    const PgsqlHighlightRules = acequire(
      "ace/mode/pgsql_highlight_rules"
    ).PgsqlHighlightRules;

    const CustomPgsqlHighlightRules = function () {
      PgsqlHighlightRules.call(this);

      this.$rules.start.unshift(
        {
          token: "comment",
          regex: /--.*(?=https?:\/\/)/,
        },
        {
          token: "url",
          regex: /https?:\/\/www\.postgresql\.org\/docs\/[^\s"']+/,
        },
        {
          token: "comment",
          regex: /--.*$/,
        }
      );
    };

    oop.inherits(CustomPgsqlHighlightRules, PgsqlHighlightRules);
    const ExtendedPgsqlMode = function () {
      PgsqlMode.call(this);
      this.foldingRules = new SqlFoldMode();
      this.HighlightRules = CustomPgsqlHighlightRules;
    };
    oop.inherits(ExtendedPgsqlMode, PgsqlMode);

    exports.Mode = ExtendedPgsqlMode;
  }
);
