import { createApp } from "vue";
import WelcomeScreen from "../components/WelcomeScreen.vue";

export let welcomeScreenInit = function() {
    let tab = v_connTabControl.createTab({
        p_icon: '<i class="fas fa-hand-spock"></i>',
        p_name: 'Welcome',
        p_selectFunction: function() {
          document.title = 'Welcome to PgManage'
          $('[data-toggle="tooltip"]').tooltip({animation:true});// Loads or Updates all tooltips
        },
        p_close: false,
        p_tooltip: 'Welcome to PgManage'
      });

    v_connTabControl.selectTab(tab);

    tab.elementDiv.innerHTML = `
    <div id='welcome_screen' class="pt-3">
      <welcome-screen></welcome-screen>
    </div>`;

    let tag = {
        tab_id: tab.id,
        div_result: document.getElementById('welcome_screen'),
    };
    tab.tag = tag;

    let app = createApp({
        components: {
            "welcome-screen": WelcomeScreen,
        }
      },
      {}
    )

    tab.app = app;
    app.mount('#welcome_screen');
    $('[data-toggle="tooltip"]').tooltip({animation:true})
};