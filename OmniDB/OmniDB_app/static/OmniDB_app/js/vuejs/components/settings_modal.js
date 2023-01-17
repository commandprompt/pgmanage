const settingsModal = createApp({
	components: {
		'settings-modal': Vue.defineAsyncComponent(() => loadModule('../static/OmniDB_app/js/vuejs/components/SettingsModal.vue', options)),
	},
})

settingsModal.mount('#settings-modal-wrap')