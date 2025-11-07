export default {
  watch: {
    tabulator: {
      handler() {
        this.tabulator.element.addEventListener("keydown", (e) => {
          if (e.key === "ContextMenu" || (e.shiftKey && e.code === "F10")) {
            e.preventDefault();
            let selectedRange = this.tabulator.getRanges()[0];
            let selectedCell = selectedRange.getCells()[0][0];
            let cellElement = selectedCell.getElement();
            const rect = cellElement.getBoundingClientRect();
            const event = new MouseEvent("contextmenu", {
              bubbles: true,
              clientX: rect.left + rect.width / 2,
              clientY: rect.top + rect.height / 2,
            });

            cellElement.dispatchEvent(event);

            this.$nextTick(() => {
              const menu = document.querySelector(".tabulator-menu");
              if (!menu || menu.style.display === "none") return;

              const items = [...menu.querySelectorAll(".tabulator-menu-item")];
              if (!items.length) return;
              items.forEach((item) => {
                item.addEventListener("mouseenter", () => {
                  item.focus();
                });
              });
              items[0].focus();
            });
          }
        });

        this.tabulator.on("menuOpened", (component) => {
          document
            .querySelectorAll(".tabulator-menu-item")
            .forEach((el) => el.setAttribute("tabindex", "0"));
        });

        this.tabulator.on("menuClosed", (component) => {
          component.getElement().focus(); // restores cell focus after context menu is closed
        });
      },
      once: true,
    },
  },
  mounted() {
    document.addEventListener("keydown", this.handleMenuNavigation);
  },
  unmounted() {
    document.removeEventListener("keydown", this.handleMenuNavigation);
  },
  methods: {
    handleMenuNavigation(e) {
      const menu = document.querySelector(".tabulator-menu");
      if (!menu || menu.style.display === "none") return;

      const items = [...menu.querySelectorAll(".tabulator-menu-item")];
      if (!items.length) return;

      let index = items.findIndex((item) => item === document.activeElement);

      if (e.key === "ArrowDown") {
        e.preventDefault();
        index = (index + 1) % items.length;
        items[index].focus();
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        index = (index - 1 + items.length) % items.length;
        items[index].focus();
      } else if (e.key === "Enter") {
        e.preventDefault();
        document.activeElement.click(); // trigger Tabulator's action
      }
    },
  },
};
