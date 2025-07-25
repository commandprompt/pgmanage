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
.ace-omnidb_dark .ace_gutter-cell.ace_error {\
    background:url(\"data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 51.976 51.976'%3e %3ccircle style='fill:white' cx='25.75' cy='25.75' r='15.160686' /%3e %3cpath fill='%23D72239' d='M44.373 7.603c-10.137-10.137-26.632-10.138-36.77 0-10.138 10.138-10.137 26.632 0 36.77s26.632 10.138 36.77 0c10.137-10.138 10.137-26.633 0-36.77zm-8.132 28.638a2 2 0 01-2.828 0l-7.425-7.425-7.778 7.778a2 2 0 11-2.828-2.828l7.778-7.778-7.425-7.425a2 2 0 112.828-2.828l7.425 7.425 7.071-7.071a2 2 0 112.828 2.828l-7.071 7.071 7.425 7.425a2 2 0 010 2.828z'/%3e %3c/svg%3e\") no-repeat 4px/0.775rem\
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
background: url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAACCAYAAACZgbYnAAAAEklEQVQImWNgYGBgYHB3d/8PAAOIAdULw8qMAAAAAElFTkSuQmCC) right repeat-y;\
}\
.ace-omnidb_dark .ace_search.right {\
display: flex;\
flex-direction: column;\
align-items: stretch;\
background-color: #162D4E;\
padding: 8px 24px 8px 8px;\
right: 6px;\
top: 3px;\
font-family: 'Poppins', sans-serif;\
font-size: 0.75rem;\
border-radius: 6px;\
border-right: initial;\
border: 0; \
color: #F8FAFD;\
}\
.ace-omnidb_dark .ace_search_form, .ace-omnidb_dark .ace_replace_form {\
display: flex;\
margin-bottom: 16px;\
margin-right: 0 !important;\
position: relative;\
overflow: visible;\
width: 385px;\
}\
.ace-omnidb_dark .ace_search_form.ace_nomatch, .ace_search_form.ace_nomatch > .ace_search_field {\
outline: 0;\
}\
.ace_search_form.ace_nomatch:after {\
content: 'No matches';\
color: #D72239;\
display: block;\
position: absolute;\
left: 0.75rem;\
bottom: 0;\
transform: translateY(100%);\
font-size: 0.6rem;\
font-weight: 500;\
}\
.ace-omnidb_dark .ace_replace_form {\
margin-right: 20px;\
}\
.ace-omnidb_dark .ace_search_field {\
background-color: #1A2234;\
border: 1px solid #243049;\
color: #F8FAFC;\
border-radius: 6px 0 0 6px;\
border-right: 0;\
width: 70%;\
padding: 0.375rem 0.75rem;\
height: calc(1.5em + 0.75rem + 2px);\
box-sizing: border-box;\
}\
.ace-omnidb_dark .ace_searchbtn {\
display: flex;\
align-items: center;\
justify-content: center;\
flex: 1 1 2.2rem;\
padding: 0.375rem 0.75rem;\
height: calc(1.5em + 0.75rem + 2px);\
flex-shrink: 0;\
box-sizing: border-box;\
border-color: #243049;\
color: #747D8D;\
font-weight: 500;\
background-color: #1A2234;\
}\
.ace-omnidb_dark .ace_searchbtn:hover {\
background-color: #2c3a58;\
color: #F8FAFD;\
}\
.ace-omnidb_dark .ace_searchbtn.prev, .ace-omnidb_dark .ace_searchbtn.next {\
padding: 0.375rem 0.75rem;\
}\
.ace-omnidb_dark .ace_searchbtn.prev:after, .ace-omnidb_dark .ace_searchbtn.next:after {\
border-color: #747D8D;\
}\
.ace-omnidb_dark .ace_searchbtn.prev:hover:after, .ace-omnidb_dark .ace_searchbtn.next:hover:after {\
border-color: #F8FAFD;\
}\
.ace-omnidb_dark .ace_searchbtn:last-child {\
border-radius: 0 6px 6px 0;\
border-right-color: #243049;\
}\
.ace-omnidb_dark .ace_searchbtn_close {\
width: 16px;\
height: 16px;\
background-size: cover;\
top: 5px;\
right: 5px;\
}\
.ace-omnidb_dark .ace_searchbtn_close:hover {\
background-color: #1560AD;\
}\
.ace-omnidb_dark.ace_search_options {\
margin-bottom: 0;\
}\
.ace-omnidb_dark .ace_search_options .ace_button {\
color: #F8FAFD;\
padding: 0.2rem;\
min-width: 1.25rem;\
height: 1.3rem;\
display: inline-block;\
text-align: center;\
border-radius: 6px;\
background-color: #747D8D;\
border: 0;\
margin-left: 6px;\
opacity: 1;\
}\
.ace-omnidb_dark .ace_search_options .ace_button:hover {\
background-color: #96a3bb;\
}\
.ace-omnidb_dark .ace_search_options .ace_button.checked {\
background-color: #1560AD;\
}\
.ace-omnidb_dark .ace_search_options .ace_button[action='toggleReplace'] {\
padding: 0.25rem !important;\
margin-top: 0 !important;\
}\
.ace-omnidb_dark .ace_search_options .ace_search_counter {\
padding-top: 5px;\
font-family: inherit;\
}\
.ace-omnidb_dark.ace_editor.ace_autocomplete {\
background-color: #161D2D !important;\
border: 1px solid #162D4E !important;\
border-radius: 6px !important;\
color: #F8FAFD !important;\
box-shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15) !important;\
}\
.ace-omnidb_dark.ace_autocomplete .ace_marker-layer .ace_active-line {\
background-color: #162d4e !important;\
}\
.ace-omnidb_dark.ace_autocomplete .ace_marker-layer .ace_line-hover {\
background-color: rgba(21, 97, 172, 0.25) !important;\
border: 0 !important;\
}\
.ace-omnidb_dark.ace_editor.ace_autocomplete .ace_completion-highlight {\
color: #F76707 !important;\
}\
.ace-omnidb_dark.ace_editor .ace_completion-meta {\
font-style: italic;\
}\
.ace-omnidb_dark .ace_tooltip {\
border-radius: 6px;\
padding: 8px;\
box-shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15);\
border: 0;\
}\
.ace-omnidb_dark .ace_tooltip.ace_dark {\
background-color: #164171;\
color: #F8FAFD;\
}\
.ace-omnidb_dark .ace_icon.ace_error {\
background:url(\"data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 51.976 51.976'%3e %3ccircle style='fill:white' cx='25.75' cy='25.75' r='15.160686' /%3e %3cpath fill='%23D72239' d='M44.373 7.603c-10.137-10.137-26.632-10.138-36.77 0-10.138 10.138-10.137 26.632 0 36.77s26.632 10.138 36.77 0c10.137-10.138 10.137-26.633 0-36.77zm-8.132 28.638a2 2 0 01-2.828 0l-7.425-7.425-7.778 7.778a2 2 0 11-2.828-2.828l7.778-7.778-7.425-7.425a2 2 0 112.828-2.828l7.425 7.425 7.071-7.071a2 2 0 112.828 2.828l-7.071 7.071 7.425 7.425a2 2 0 010 2.828z'/%3e %3c/svg%3e\") no-repeat 4px/0.775rem\
}\
.ace-omnidb_dark .ace_url {\
color: #767f8f;\
}\
.ace-omnidb_dark .ace_link_marker {\
position: absolute;\
border-radius: 0px;\
border-bottom: 2px solid #1560AD;\
}\
}";

var dom = require("../lib/dom");
dom.importCssString(exports.cssText, exports.cssClass);
});
