import { fileURLToPath, URL } from "node:url";

import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import Components from "unplugin-vue-components/vite";
import { BootstrapVueNextResolver } from "unplugin-vue-components/resolvers";
import path from "node:path";
import { nodePolyfills } from "vite-plugin-node-polyfills";
import inject from "@rollup/plugin-inject";

const outDir = path.join("..", "dist");

export default defineConfig({
  plugins: [
    inject({
      $: "jquery",
      jQuery: "jquery",
      include: '**/*.js',
      sourceMap: false
    },),
    vue(),
    Components({
      resolvers: [BootstrapVueNextResolver()],
    }),
    nodePolyfills({
      globals: {
        process: true,
      },
    }),
  ],
  server: {
    host: "127.0.0.1",
    port: 3000,
    open: false,
    watch: {
      usePolling: true,
      disableGlobbing: false,
    },
  },
  build: {
    manifest: true,
    rollupOptions: {
      input: ["./src/main.js", "./src/login.js"],
      output: {
        entryFileNames: `assets/[name].js`,
        chunkFileNames: `assets/[name].js`,
        assetFileNames: `assets/[name].[ext]`
      }
    },
    outDir: outDir,
  },
  base: "/static/",
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
      vue: "vue/dist/vue.esm-bundler.js",
      "~bootstrap": path.resolve(__dirname, "node_modules/bootstrap"),
      'moment': path.resolve(__dirname, 'node_modules/moment/moment.js')
    },
  },
});
