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

function createOmnis() {
  return {
    id: 'omnis',
    div: null,
    template:
    `<?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <svg
       viewBox="0 0 512 512"
       version="1.1"
       x="0px" y="0px"
       width="36px" height="36px"
       id="svg4"
       xmlns="http://www.w3.org/2000/svg"
       xmlns:svg="http://www.w3.org/2000/svg">
      <defs
         id="defs8" />
      <ellipse
         id="path32"
         cx="261.3259"
         cy="255.18886"
         rx="147.55562"
         ry="147.48106"
         class="icon-bg"
         style="stroke:none;" />
      <!--! Font Awesome Free 6.2.1 by @fontawesome - https://fontawesome.com License - https://fontawesome.com/license/free (Icons: CC BY 4.0, Fonts: SIL OFL 1.1, Code: MIT License) Copyright 2022 Fonticons, Inc. -->
      <path
         d="M256 512c141.4 0 256-114.6 256-256S397.4 0 256 0S0 114.6 0 256S114.6 512 256 512zM216 336h24V272H216c-13.3 0-24-10.7-24-24s10.7-24 24-24h48c13.3 0 24 10.7 24 24v88h8c13.3 0 24 10.7 24 24s-10.7 24-24 24H216c-13.3 0-24-10.7-24-24s10.7-24 24-24zm40-144c-17.7 0-32-14.3-32-32s14.3-32 32-32s32 14.3 32 32s-14.3 32-32 32z"
         id="path2"
         class="icon-body" />
    </svg>`
  }
}

function createOmnisUiAssistant({p_callback_end = false, p_omnis, p_steps = []}) {

  // tmp steps
  var v_steps = (p_steps.length !== 0) ? p_steps : [
    {
      p_message: 'This contains the outer connection and global panels [ connections_list_manager, snippets_panel, [conn_1, conn_2, ...], add_connection]',
      p_target: document.getElementsByClassName('omnidb__tab-menu omnidb__tab-menu--primary')[0],
      p_title: 'Primary menu'
    },
    {
      p_message: 'This contains general settings and options, such as [ versioning, connections_list_manager, user_setting, plugins...]',
      p_target: document.getElementsByClassName('omnidb__utilities-menu')[0],
      p_title: 'Utilities menu'
    }
  ];

  // tmp state control
  var v_state_active = true;

  var v_omnisControl = {
    // Params
    callback_end: p_callback_end,
		id: 'omnis_control_' + Date.now(),
		stateActive: v_state_active,
    stepCounter: 0,
    stepList: [],
    stepSelected: null,
    z_index: 999999,
    // Actions
    getPosition : function(p_el) {
  		var xPos = 0;
  		var yPos = 0;
      var el = p_el;

  		while (el) {
  			if (el.tagName == "BODY") {
  				var xScroll = el.scrollLeft || document.documentElement.scrollLeft;
  				var yScroll = el.scrollTop || document.documentElement.scrollTop;

  				xPos += (el.offsetLeft - xScroll + el.clientLeft);
  				yPos += (el.offsetTop - yScroll + el.clientTop);
  			}
  			else {
  				xPos += (el.offsetLeft - el.scrollLeft + el.clientLeft);
  				yPos += (el.offsetTop - el.scrollTop + el.clientTop);
  			}

  			el = el.offsetParent;
  		}

  		return {
  			x: xPos,
  			y: yPos
  		};
  	},
    self_destruct: function() {
      var v_control = this;
      v_control.setStateDisabled();
      document.getElementById('omnidb__main').removeChild(v_control.divElement);
      for (let i = 0; i < v_control.stepList.length; i++) {
        if (v_control.stepList[i].callback_end !== false) {
          v_control.stepList[i].callback_end();
        }
      }
      if (this.callback_end) {
        this.callback_end();
      }
      var v_omnis_div = p_omnis.div;
      v_omnis_div.style.top = p_omnis.root.getBoundingClientRect().height - 45 + 'px';
      v_omnis_div.style.left = p_omnis.root.getBoundingClientRect().width - 45 + 'px';
    },
    emptyStepList : function() {
      this.stepCounter = 0;
      this.stepList = [];
    },
    updateStepList : function(p_list) {
      this.emptyStepList();
      for (let i = 0; i < p_list.length; i++) {
        this.createStep(p_list[i]);
      }
    },
    goToStep : async function(p_index) {
      for (let i = 0; i < this.stepList.length; i++) {
        if (p_index !== i) {
          if (this.stepList[i].callback_end !== false) {
            this.stepList[i].callback_end();
          }
        }
      }
      if (this.stepList[p_index].callback_start) {
        this.stepList[p_index].callback_start();
      }
      var v_control = this;
      v_control.stepSelected = p_index;

      var v_step_item = await v_control.renderStep();

      if (v_step_item !== 'stop') {

        var get_v_target = new Promise(resolve => {
          setTimeout(function(){
            var v_next_btn = document.getElementById('omnis_step_btn_next');
            if (v_next_btn !== undefined && v_next_btn !== null) {
              v_next_btn.onclick = function(){v_control.goToStep(v_control.stepSelected + 1)};
            }

            var v_target;
            if (typeof v_step_item.target === 'function') {
              v_target = v_step_item.target();
            }
            else {
              v_target = v_step_item.target;
            }

            v_control.updateOmnisPosition(v_target,v_step_item.position);
            resolve(v_target);

          }, v_step_item.update_delay);


        });

        await get_v_target.then(function(v_target){
          if (v_step_item.clone_target && v_target) {
            let v_update_delay = v_step_item.update_delay;

            if (v_target !== null) {

              let v_target_bounding_rect = v_target.getBoundingClientRect();
              let v_target_bounding_rect_left = v_target_bounding_rect.x + 'px';
              let v_target_bounding_rect_top = v_target_bounding_rect.y + 'px';
              let v_target_bounding_rect_width = v_target_bounding_rect.width + 'px';
              // Account for getBoundingClientRect slowness.
              setTimeout(function(){

                var v_cloned_element = v_target.cloneNode(true);
                v_cloned_element.setAttribute('id','omnis_temp_clone');
                v_control.divClonedElement.style.left = v_target_bounding_rect_left;
                v_control.divClonedElement.style.top = v_target_bounding_rect_top;
                v_cloned_element.style.width = v_target_bounding_rect_width;
                v_control.updateClonedElementContent(v_cloned_element);
                v_control.divBackdropElement.style.display = '';
                v_cloned_element.addEventListener('click',function(){v_control.goToStep(v_control.stepSelected + 1)});
              },50);
            }
            else {
              // Emptying the divClonedElement.
              v_control.divClonedElement.innerHTML = '';
              v_control.divClonedElement.style.left = '';
              v_control.divClonedElement.style.top = '';
              v_control.divBackdropElement.style.display = 'none';
            }
          }
          else {
            // Emptying the divClonedElement.
            v_control.divClonedElement.innerHTML = '';
            v_control.divClonedElement.style.left = '';
            v_control.divClonedElement.style.top = '';
            v_control.divBackdropElement.style.display = 'none';
          }


          var v_previous_btn = document.getElementById('omnis_step_btn_previous');
          if (v_previous_btn !== undefined && v_previous_btn !== null) {
            v_previous_btn.onclick = function(){v_control.goToStep(v_control.stepSelected - 1)};
          }

          var v_close_btn = document.getElementById('omnis_step_btn_close');
          if (v_close_btn !== undefined && v_close_btn !== null) {
            v_close_btn.onclick = function(){
              v_control.self_destruct();
            }
          }

          if (v_control.stepList[v_control.stepSelected].callback_after_update_start) {
            v_control.stepList[v_control.stepSelected].callback_after_update_start();
          }
        });

      }
    },
    // Template
    createStep : function({
      p_callback_after_update_start = false,
      p_callback_end = false,
      p_callback_start = false,
      p_clone_target = false,
      p_message = 'Example',
      p_next_button = true,
      p_position = ()=>{return false},
      p_target = null,
      p_title = 'Omnis',
      p_update_delay = 0
    }) {
			var v_control = this;
			var v_index = v_control.stepCounter;

			v_control.stepCounter++;

			var v_step = {
        callback_after_update_start: p_callback_after_update_start,
        callback_end: p_callback_end,
        callback_start: p_callback_start,
        clone_target: p_clone_target,
				id: v_control.id + '_step_' + v_index,
        message: p_message,
        next_button: p_next_button,
        position: p_position(),
        target: p_target,
        title: p_title,
        update_delay: p_update_delay
			};


      this.stepList.push(v_step);

    },
    renderStep : function() {
      if (this.stateActive) {
        var v_control = this;
        var v_step_item = this.stepList[this.stepSelected];
        // Emptying the divClonedElement.
        v_control.divClonedElement.innerHTML = '';
        v_control.divClonedElement.style.left = '';
        v_control.divClonedElement.style.top = '';
        v_control.divBackdropElement.style.display = '';
        // Emptying the divWavesElement.
        v_control.divWavesElement.innerHTML = '';

        var v_title = '';
        if (v_step_item.title) {
          v_title += '<div class="omnis__step__title card-title p-2 mt-2 mb-0"><h2 class="mb-0">' + v_step_item.title + '</h2></div>';
        }

        var v_message = '';
        if (v_step_item.message) {
          v_message += '<div class="omnis__step__body card-body p-2 mb-4">' + v_step_item.message + '</div>';
        }

        var v_step_btn_next = '';
        if (this.stepList[this.stepSelected].next_button && this.stepSelected < this.stepCounter - 1) {
          v_step_btn_next += '<button id="omnis_step_btn_next" type="button" class="btn btn-sm btn-primary ml-2">Next</button>';
        }

        // Temporarily disabling previous button.
        // TODO: implement a better goto method when going to previous steps, allowing the UI not to break because of callbacks.
        var v_step_btn_previous = '';
        // if (this.stepSelected > 0) {
        //   v_step_btn_previous += '<button id="omnis_step_btn_previous" type="button" class="btn btn-sm btn-secondary mr-2">Previous</button>';
        // }

        var v_step_btn_close = '<button id="omnis_step_btn_close" type="button" class="btn btn-sm btn-danger ml-auto">End walkthrough</button>';

        var v_step_title =
        '<div class="mb-4 text-center" style="position: relative;">' +
          // '<div style="background: none; display: inline-block; height: 64px; width:64px;">' + v_animated_omnis + '</div>' +
          v_title +
        '</div>';

        var v_step_html =
        '<div class="omnis__step card">' +
          v_step_title +
          v_message +
          '<div class="omnis__step__footer card-footer d-flex align-items-center p-2">' +
            v_step_btn_previous +
            v_step_btn_next +
            v_step_btn_close +
          '</div>' +
        '</div>';

        this.divCardElement.innerHTML = v_step_html;

        this.divElement.style.display = 'block';

        return new Promise(resolve => {
          resolve(v_step_item);
        });
      }
      else {
        this.divElement.style.display = 'none';
        // Emptying the divWavesElement.
        this.divWavesElement.innerHTML = '';

        return new Promise(resolve => {
          resolve('stop');
        });
      }
    },
    setStateEnabled: function() {
      this.stateActive = true;
      this.renderStep();
    },
    setStateDisabled: function() {
      this.stateActive = false;
      this.renderStep();
    },
    updateClonedElementContent: function(p_content_element) {
      var v_control = this;
      var v_cloned_element = v_control.divClonedElement;
      var v_waves_element = v_control.divWavesElement;
      v_cloned_element.innerHTML = '';
      v_waves_element.innerHTML = '';
      v_cloned_element.appendChild(p_content_element);
      v_waves_element.innerHTML =
      '<span id="' + v_control.id + '_cloned_element_waves" class="omnis__cloned-element__waves">' +
        '<span></span>' +
        '<span></span>' +
        '<span></span>' +
        '<span></span>' +
      '</span>';
      let v_target = (typeof v_control.stepList[v_control.stepSelected].target === 'function')
      ? v_control.stepList[v_control.stepSelected].target()
      : v_control.stepList[v_control.stepSelected].target;
      let v_cloned_element_bounding_rect = v_target.getBoundingClientRect();
      v_waves_element.style.left = v_cloned_element_bounding_rect.x + 'px';
      v_waves_element.style.top = v_cloned_element_bounding_rect.y + 'px';
      v_waves_element.style.width = v_cloned_element_bounding_rect.width + 'px';
      v_waves_element.style.height = v_cloned_element_bounding_rect.height + 'px';
      v_waves_element.style.display = 'block';
      var v_cloned_element_waves = document.getElementById(v_control.id + '_cloned_element_waves');
    },
    updateOmnisPosition : function(p_target, p_pos = false) {
      try {
        let v_root = document.getElementById('omnidb__main');
        let v_window_width = v_root.offsetWidth;
        let v_window_width_half = Math.round(v_window_width / 2);
        let v_window_height = v_root.offsetHeight;
        let v_window_height_half = Math.round(v_window_height / 2);
        var v_control = this;
        var v_target = p_target;
        if (!v_target) {
          v_target = (typeof v_control.stepList[v_control.stepSelected].target === 'function')
          ? v_control.stepList[v_control.stepSelected].target()
          : v_control.stepList[v_control.stepSelected].target;
        }
        var v_target_position;
        var v_target_offset_width = 0;
        if (p_pos) {
          v_target_position = {x:p_pos.x, y:p_pos.y}
        }
        else if (v_target) {
          v_target_position = v_control.getPosition(v_target);
          v_target_offset_width = v_target.offsetWidth;
        }
        else {
          v_target_position = {x:v_window_width - 5, y:v_window_height - 5}
        }
        var v_omnis_div = p_omnis.div;

        // Target contextual cases for positioning:
        // Right side of the screen.
        if (v_target_position.x >= v_window_width_half) {
          v_omnis_div.style.left = v_target_position.x - 56 + 'px';
          v_control.divCardElement.style.left = v_target_position.x - v_control.divCardElement.offsetWidth - 56 + 'px';
          // Above vertical middle of the screen.
          if (v_target_position.y <= v_window_height_half) {
            v_omnis_div.style.top = v_target_position.y + 16 + 'px';
            v_control.divCardElement.style.top = v_target_position.y + 20 + 'px';
          }
          // Below vertical middle of the screen.
          else {
            v_omnis_div.style.top = v_target_position.y - 56 + 'px';
            v_control.divCardElement.style.top = v_target_position.y - v_control.divCardElement.offsetHeight - 20 + 'px';
          }
        }
        // Left side of the screen.
        else {
          v_omnis_div.style.left = v_target_position.x + v_target_offset_width + 16 + 'px';
          v_control.divCardElement.style.left = v_target_position.x + v_target_offset_width + 56 + 'px';
          // Above vertical middle of the screen.
          if (v_target_position.y <= v_window_height_half) {
            v_omnis_div.style.top = v_target_position.y + 16 + 'px';
            v_control.divCardElement.style.top = v_target_position.y + 20 + 'px';
          }
          // Below vertical middle of the screen.
          else {
            v_omnis_div.style.top = v_target_position.y - 56 + 'px';
            v_control.divCardElement.style.top = v_target_position.y - v_control.divCardElement.offsetHeight - 20 + 'px';
          }
        }
      }
      catch(e) {
        console.warn('omnis-ui-assistant couldnt process the positioning of the target. Details:');
        console.warn(e);
      }
    }
  }

  v_omnisControl.divCardElement = document.createElement('div');
  v_omnisControl.divCardElement.setAttribute('style', 'position:fixed; width:280px; max-width: 90vw; z-index: ' + v_omnisControl.z_index + 3 + '; box-shadow: 1px 0px 3px rgba(0,0,0,0.15); transition: all 0.45s ease 0.1s;');

  v_omnisControl.divClonedElement = document.createElement('div');
  v_omnisControl.divClonedElement.setAttribute('style', 'position:absolute; width:0px; height:0px; overflow:visible; z-index:' + v_omnisControl.z_index + 2 + ';');

  v_omnisControl.divWavesElement = document.createElement('div');
  v_omnisControl.divWavesElement.setAttribute('style', 'position:absolute; width:0px; height:0px; overflow:visible; z-index:' + v_omnisControl.z_index + 1 + ';');

  v_omnisControl.divBackdropElement = document.createElement('div');
  v_omnisControl.divBackdropElement.setAttribute('style', 'position:fixed; width:100vw; height:100vh; top: 0; left: 0; z-index:' + v_omnisControl.z_index + '; background-color:rgba(0,0,0,0.25);');

  v_omnisControl.divElement = document.createElement('div');
  v_omnisControl.divElement.setAttribute('id', v_omnisControl.id);

  v_omnisControl.divElement.appendChild(v_omnisControl.divCardElement);
  v_omnisControl.divElement.appendChild(v_omnisControl.divClonedElement);
  v_omnisControl.divElement.appendChild(v_omnisControl.divWavesElement);
  v_omnisControl.divElement.appendChild(v_omnisControl.divBackdropElement);
  document.getElementById('omnidb__main').appendChild(v_omnisControl.divElement);

  return v_omnisControl;

}
