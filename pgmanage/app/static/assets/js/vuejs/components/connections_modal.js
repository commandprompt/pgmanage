const {createApp} = Vue;

const conn_app = createApp({
    components: {
        'connections-modal': Vue.defineAsyncComponent(() => loadModule('/static/assets/js/vuejs/components/ConnectionsModal.vue', options)),
    },
  })

// this is a temporary supporting code to glue the new vue app and old app code
// connection manager just emits the event with a selected connection as a payload
// the glue code does the necessary stuff in the old app
document.addEventListener("DOMContentLoaded", function () {
    document.addEventListener('connection:selected', function(event){
        let connection = event.detail
        $('#connections-modal').modal('hide');
        if (connection.technology === 'terminal') {
            v_connTabControl.tag.createOuterTerminalTab(connection.id, connection.alias, connection.tunnel.user + '@' + connection.tunnel.server + ':' + connection.tunnel.port);
        }
        else {
            v_connTabControl.tag.createConnTab(connection.id);
        }
    })

    // document.addEventListener('connection:tested', function(event){
    //     let result = event.detail
    //     if(result.status !== 'success') {
    //         showError(result.data);
    //     } else {
    //         showAlert(result.data);
    //     }
    // })
});