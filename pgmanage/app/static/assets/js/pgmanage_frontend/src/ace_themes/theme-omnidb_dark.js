import ace from 'ace-builds'

ace.define("ace/theme/omnidb_dark",["require","exports","module","ace/lib/dom"], function(require, exports, module) {

exports.isDark = true;
exports.cssClass = "ace-omnidb_dark";
exports.cssText = ".ace-omnidb_dark .ace_gutter {\
background: #16171E;\
font-size: 1em;\
color: #747D8D;\
}\
.ace-omnidb_dark .ace_print-margin {\
width: 1px;\
background: #25282c\
}\
.ace-omnidb_dark {\
background-color: #16171E;\
color: #F8FAFC\
}\
.ace-omnidb_dark .ace_cursor {\
color: #F8FAFC\
}\
.ace-omnidb_dark .ace_marker-layer .ace_selection {\
background: #1560AD;\
}\
.ace-omnidb_dark.ace_multiselect .ace_selection.ace_start {\
box-shadow: 0 0 3px 0px #1D1F21;\
}\
.ace-omnidb_dark .ace_marker-layer .ace_step {\
background: rgb(102, 82, 0)\
}\
.ace-omnidb_dark .ace_marker-layer .ace_bracket {\
margin: 0 0 0 -1px;\
border: 1px solid rgb(245, 159, 0);\
background-color: rgba(245, 159, 0, 0.5);\
}\
.ace-omnidb_dark .ace_marker-layer .ace_active-line {\
background: #162d4e\
}\
.ace-omnidb_dark .ace_gutter-active-line {\
background-color: #162d4e\
}\
.ace-omnidb_dark .ace_marker-layer .ace_selected-word {\
border: 1px solid #373B41\
}\
.ace-omnidb_dark .ace_invisible {\
color: #4B4E55\
}\
.ace-omnidb_dark .ace_keyword,\
.ace-omnidb_dark .ace_meta,\
.ace-omnidb_dark .ace_storage,\
.ace-omnidb_dark .ace_storage.ace_type,\
.ace-omnidb_dark .ace_support.ace_type {\
color: #3987e4;\
font-weight: 700;\
}\
.ace-omnidb_dark .ace_keyword.ace_operator {\
color: #adc1d8\
}\
.ace-omnidb_dark .ace_constant.ace_character,\
.ace-omnidb_dark .ace_constant.ace_language,\
.ace-omnidb_dark .ace_constant.ace_numeric,\
.ace-omnidb_dark .ace_keyword.ace_other.ace_unit,\
.ace-omnidb_dark .ace_support.ace_constant,\
.ace-omnidb_dark .ace_variable.ace_parameter {\
color: #ef578c\
}\
.ace-omnidb_dark .ace_constant.ace_other {\
color: #CED1CF\
}\
.ace-omnidb_dark .ace_invalid {\
color: #CED2CF;\
background-color: #DF5F5F\
}\
.ace-omnidb_dark .ace_invalid.ace_deprecated {\
color: #CED2CF;\
background-color: #B798BF\
}\
.ace-omnidb_dark .ace_fold {\
background-color: #81A2BE;\
border-color: #C5C8C6\
}\
.ace-omnidb_dark .ace_entity.ace_name.ace_function,\
.ace-omnidb_dark .ace_support.ace_function,\
.ace-omnidb_dark .ace_variable {\
color: #df76f7\
}\
.ace-omnidb_dark .ace_support.ace_class,\
.ace-omnidb_dark .ace_support.ace_type {\
color: #F0C674\
}\
.ace-omnidb_dark .ace_heading,\
.ace-omnidb_dark .ace_markup.ace_heading,\
.ace-omnidb_dark .ace_string {\
color: #0ca678\
}\
.ace-omnidb_dark .ace_entity.ace_name.ace_tag,\
.ace-omnidb_dark .ace_entity.ace_other.ace_attribute-name,\
.ace-omnidb_dark .ace_meta.ace_tag,\
.ace-omnidb_dark .ace_string.ace_regexp,\
.ace-omnidb_dark .ace_variable {\
color: #CC6666\
}\
.ace-omnidb_dark .ace_comment {\
color: #767f8f\
}\
.ace-omnidb_dark .ace_indent-guide {\
background: url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAACCAYAAACZgbYnAAAAEklEQVQImWNgYGBgYHB3d/8PAAOIAdULw8qMAAAAAElFTkSuQmCC) right repeat-y\
}";

var dom = require("../lib/dom");
dom.importCssString(exports.cssText, exports.cssClass);
});
