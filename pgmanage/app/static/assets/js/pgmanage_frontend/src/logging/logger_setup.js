import * as log from "loglevel";
import prefix from "loglevel-plugin-prefix";
import remote from "loglevel-plugin-remote";
import { getCookie } from "../ajax_control";
import { vueHooks, axiosHooks } from "./service";
import axios from "axios";
import {
  settingsStore,
  tabsStore,
  connectionsStore,
  snippetsStore,
} from "../stores/stores_initializer";

const DEFAULT_LOGLEVEL = "trace";
const DEFAULT_LOGLEVEL_REMOTE = "warn";
const logger = log.noConflict();

logger.setLevel(DEFAULT_LOGLEVEL); // my default log level

prefix.reg(logger); // prefix every print with <loglevel><HH:MM:SS>:
prefix.apply(logger);

const getCounter = () => {
  let count = 1;
  return () => count++;
};

const counter = getCounter();

const customPlain = (log) => log.message;

const customJSON = (log) => ({
  request_id: counter(),
  msg: customPlain(log),
  level: log.level.label,
});

remote.apply(logger, {
  url: "/log/",
  method: "POST",
  level: DEFAULT_LOGLEVEL_REMOTE,
  headers: { "X-CSRFToken": getCookie(v_csrf_cookie_name) },
  token: "",
  onUnauthorized: () => {},
  timeout: 0,
  backoff: {
    multiplier: 2,
    jitter: 0.1,
    limit: 30000,
  },
  capacity: 500,
  stacktrace: {
    depth: 10,
  },
  timestamp: () => new Date().toISOString(),
  format: customJSON,
});

const stores = [settingsStore, tabsStore, connectionsStore, snippetsStore];

export function setupLogger(app) {
  vueHooks(logger, app, stores);
  axiosHooks(logger, axios);
}

export { logger };
