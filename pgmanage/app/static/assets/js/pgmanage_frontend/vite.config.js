/// <reference types="vitest" />
import { fileURLToPath, URL } from "node:url";

import { defineConfig, loadEnv } from "vite";
import vue from "@vitejs/plugin-vue";
import Components from "unplugin-vue-components/vite";
import { BootstrapVueNextResolver } from "unplugin-vue-components/resolvers";
import path from "node:path";
import { nodePolyfills } from "vite-plugin-node-polyfills";
import inject from "@rollup/plugin-inject";

const outDir = path.join("..", "dist");
const isDebugBuild = process.env.DEBUG === 'true'

export default defineConfig(({ command, mode }) => {
  const env = loadEnv(mode, process.cwd());
  const isEnterprise = mode === 'enterprise';
  return {
    plugins: [
      inject({
        $: "jquery",
        jQuery: "jquery",
        include: "**/*.js",
        sourceMap: isDebugBuild,
      }),
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
      minify: !isDebugBuild,
      rollupOptions: {
        input: ["./src/main.js", "./src/login.js"],
        output: {
          entryFileNames: `assets/[name].js`,
          chunkFileNames: `assets/[name].js`,
          assetFileNames: `assets/[name].[ext]`,
        },
      },
      outDir: outDir,
    },
    base: "/static/",
    define: {
      __VITE_ENTERPRISE__: isEnterprise
    },
    resolve: {
      alias: {
        "@": fileURLToPath(new URL("./src", import.meta.url)),
        "@enterprise": isEnterprise ? fileURLToPath(new URL("./enterprise", import.meta.url)) : fileURLToPath(new URL("./src", import.meta.url)),
        vue: "vue/dist/vue.esm-bundler.js",
        "~bootstrap": path.resolve(__dirname, "node_modules/bootstrap"),
        moment: path.resolve(__dirname, "node_modules/moment/moment.js"),
      },
      preserveSymlinks: true,
    },
    test: {
      environment: "happy-dom",
      coverage: {
        reporter: ["html"],
        enabled: true
      }
    },
  };
});