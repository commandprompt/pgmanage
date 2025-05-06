import { defineStore } from "pinia";
import { settingsStore } from "./stores_initializer";

const useUtilitiesMenuStore = defineStore("utitiesMenu", {
  state: () => ({
    items: [],
  }),
  actions: {
    addItem({
      id,
      name,
      icon,
      clickFunction,
      desktop = true,
      superuserRequired = true,
    }) {
      const isUserMatch = superuserRequired ? window.v_super_user : true;
      const isModeMatch = settingsStore.desktopMode === desktop;
      let item = {
        id: id,
        name: name,
        icon: icon,
        clickFunction: clickFunction,
        show: isUserMatch && isModeMatch,
      };

      this.items.push(item);
    },
  },
});

export { useUtilitiesMenuStore };
