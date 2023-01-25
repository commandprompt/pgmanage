const settingsModal = createApp({
	components: {
		'settings-modal': Vue.defineAsyncComponent(() => loadModule('../static/assets/js/vuejs/components/SettingsModal.vue', options)),
	},
})

settingsModal.mount('#settings-modal-wrap')