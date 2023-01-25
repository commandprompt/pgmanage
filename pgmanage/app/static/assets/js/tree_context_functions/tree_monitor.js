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

/// <summary>
/// Retrieving tree.
/// </summary>
function getTreeMonitor(p_div) {

  var context_menu = {
    'cm_node_root' : {
      elements : [
        {
          text : 'Refresh',
          icon: '/static/assets/images/refresh.png',
          action : function(node) {
            if (node.childNodes==0)
              refreshTreeMonitor(node);
            else {
              node.collapseNode();
              node.expandNode();
            }
          }
        },
        {
          text : 'New Node',
          icon: '/static/assets/images/monitoring.png',
          action : function(node) {
            newNodeSnippet(node,'node');
          }
        }
      ]
    },
    'cm_node' : {
      elements : [
        {
          text : 'Refresh',
          icon: '/static/assets/images/refresh.png',
          action : function(node) {
            if (node.childNodes==0)
              refreshTreeSnippets(node);
            else {
              node.collapseNode();
              node.expandNode();
            }
          }
        },
        {
          text : 'New Folder',
          icon: '/static/assets/images/folder.png',
          action : function(node) {
            newNodeSnippet(node,'node');
          }
        },
        {
          text : 'New Snippet',
          icon: '/static/assets/images/snippet_medium.png',
          action : function(node) {
            newNodeSnippet(node,'snippet');
          }
        },
        {
          text : 'Rename Folder',
          icon: '/static/assets/images/rename.png',
          action : function(node) {
            renameNodeSnippet(node);
          }
        },
        {
          text : 'Delete Folder',
          icon: '/static/assets/images/tab_close.png',
          action : function(node) {
            deleteNodeSnippet(node);
          }
        }
      ]
    },
    'cm_snippet' : {
      elements : [
        {
          text : 'Edit',
          icon: '/static/assets/images/text_edit.png',
          action : function(node) {
            startEditSnippetText(node);
          }
        },
        {
          text : 'Rename',
          icon: '/static/assets/images/rename.png',
          action : function(node) {
            renameNodeSnippet(node);
          }
        },
        {
          text : 'Delete',
          icon: '/static/assets/images/tab_close.png',
          action : function(node) {
            deleteNodeSnippet(node);
          }
        }
      ]
    }
  };

  var tree = createTree(p_div,'#fcfdfd',context_menu);
  tree.tag = {
  }


  tree.nodeAfterOpenEvent = function(node) {
    refreshTreeMonitor(node);
  }

  var node1 = tree.createNode('Monitoring Nodes',false,'/static/assets/images/monitoring.png',null,{ type: 'node_root', id:null},'cm_node_root');
  node1.createChildNode('',true,'/static/assets/images/spin.svg',null,null);

  tree.drawTree();
  v_connTabControl.selectedTab.tag.tree = tree;

}

/// <summary>
/// Refreshing tree node.
/// </summary>
/// <param name="node">Node object.</param>
function refreshTreeMonitor(node) {
	if (node.tag!=undefined)
    if (node.tag.type=='node_root') {
      getMonitorNodes(node);
    }
}

/// <summary>
/// Retrieving monitor nodes.
/// </summary>
/// <param name="node">Node object.</param>
function getMonitorNodes(node) {

	node.removeChildNodes();
	node.createChildNode('',false,'/static/assets/images/spin.svg',null,null);


	execAjax('/get_monitor_nodes/',
			null,
			function(p_return) {

				if (node.childNodes.length > 0)
					node.removeChildNodes();

				for (i=0; i<p_return.v_data.length; i++) {
          var v_node = node.createChildNode(p_return.v_data[i].v_name,false,'/static/assets/images/monitoring.png',{ type: 'node', id: p_return.v_data[i].v_id, name: p_return.v_data[i].v_name},'cm_node');
          v_node.createChildNode('DBMS: ' + p_return.v_data[i].v_technology,true,'/static/assets/images/circle_blue.png',null,null);
          v_node.createChildNode('Server: ' + p_return.v_data[i].v_server,true,'/static/assets/images/circle_blue.png',null,null);
          v_node.createChildNode('Port: ' + p_return.v_data[i].v_port,true,'/static/assets/images/circle_blue.png',null,null);
          v_node.createChildNode('Database: ' + p_return.v_data[i].v_service,true,'/static/assets/images/circle_blue.png',null,null);
          v_node.createChildNode('User: ' + p_return.v_data[i].v_user,true,'/static/assets/images/circle_blue.png',null,null);
        }


			},
			null,
			'box',
			false);
}
