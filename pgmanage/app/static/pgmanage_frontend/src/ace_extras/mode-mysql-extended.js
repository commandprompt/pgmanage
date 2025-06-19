ace.define(
  "ace/mode/mysql_extended",
  ["require", "exports", "ace/lib/oop", "ace/mode/text", "ace/range"],
  function (acequire, exports) {
    const oop = acequire("ace/lib/oop");
    const MysqlMode = acequire("ace/mode/mysql").Mode;
    const SqlFoldMode = acequire("./folding/sql").FoldMode;

    const ExtendedMysqlMode = function () {
      MysqlMode.call(this);
      this.foldingRules = new SqlFoldMode();
    };
    oop.inherits(ExtendedMysqlMode, MysqlMode);

    exports.Mode = ExtendedMysqlMode;
  }
);
