/*
This file is part of OmniDB.
OmniDB is open-source software, distributed "AS IS" under the MIT license in the hope that it will be useful.

The MIT License (MIT)

Portions Copyright (c) 2015-2020, The OmniDB Team
Portions Copyright (c) 2017-2020, 2ndQuadrant Limited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
import { showPasswordPrompt } from "./passwords";
import axios from "axios";
import { showToast } from "./notification_control";

/// <summary>
/// Retrieving Properties.
/// </summary>
function getProperties(view, data) {
  let tab_tag = v_connTabControl.selectedTab.tag;
  $(tab_tag.divLoading).fadeIn(100);
  axios
    .post(view, {
      database_index: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
      tab_id: v_connTabControl.selectedTab.id,
      data: data,
    })
    .then((resp) => {
      tab_tag.gridProperties.setData(resp.data.properties);
      tab_tag.ddlEditor.setValue(resp.data.ddl);
      tab_tag.ddlEditor.clearSelection();
      tab_tag.ddlEditor.gotoLine(0, 0, true);
      $(tab_tag.divLoading).fadeOut(100);
      tab_tag.gridPropertiesCleared = false;
    })
    .catch((error) => {
      if (error.response.data.password_timeout) {
        showPasswordPrompt(
          v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
          function () {
            getProperties(view, data);
          },
          null,
          error.response.data.data
        );
      } else {
        showToast("error", error.response.data.data)
      }
    });
}
/// <summary>
/// Clear property grid.
/// </summary>
function clearProperties() {
  var tab_tag = v_connTabControl.selectedTab.tag;
  if (!tab_tag.gridPropertiesCleared) {
    tab_tag.gridProperties.clearData();
    tab_tag.gridPropertiesCleared = true;

    tab_tag.ddlEditor.setValue("");
    tab_tag.ddlEditor.clearSelection();
    tab_tag.ddlEditor.gotoLine(0, 0, true);
  }
}

export { getProperties, clearProperties}