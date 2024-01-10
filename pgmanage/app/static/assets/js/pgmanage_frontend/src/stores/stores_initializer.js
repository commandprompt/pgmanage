import { useSnippetsStore } from "./snippets";
import { useSettingsStore } from "./settings";
import { createPinia, setActivePinia } from "pinia";

const pinia = createPinia();
setActivePinia(pinia);

const snippetsStore = useSnippetsStore();

const settingsStore = useSettingsStore();

export { snippetsStore, settingsStore };
