import { useSnippetsStore } from "./snippets";
import { createPinia, setActivePinia } from "pinia";

const pinia = createPinia();
setActivePinia(pinia);

const snippetsStore = useSnippetsStore();

export { snippetsStore };
