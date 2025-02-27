import axios from "axios";
import { logger } from "./logger_setup";
import { showToast } from "../notification_control";

export function handleError(error) {
  if (axios.isAxiosError(error)) {
    showToast("error", error.response?.data?.data || error.message);
  } else {
    // Handle general JavaScript errors
    logger.error(`[JS Error] ${error.stack}`);
    showToast("error", error.message || "An unexpected error occurred.");
  }
}
