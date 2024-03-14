import { showAlert } from "../notification_control";

export function vueHooks(logger, Vue, stores) {
  if (!logger)
    throw new Error(
      "vueHooks must be initiate with logLevel as first argument"
    );
  if (!Vue)
    throw new Error(
      "vueHooks must be initiate with Vue instance as second argument"
    );

  // Vue Hooks
  Vue.config.errorHandler = (error, vm, info) => {
    logger.error(`Vue Global ${error.stack}`);
  };

  // Hook stores Actions
  if (stores) {
    stores.forEach((store) => {
      store.$onAction(({ onError }) => {
        onError((error) => {
          logger.error(`Pinia ${store.$id}Store ${error.stack}`);
        });
      });
    });
  }
}
export function axiosHooks(logger, axiosInstance) {
  axiosInstance.interceptors.response.use(
    (response) => {
      return response;
    },
    (error) => {
      logger.error(
        `${error?.config?.url || ""} \n ${error.stack} \n request_data: ${
          error?.config?.data
        } \n response_data: ${error?.response?.data?.data}`
      );
      if (error.response && error.response.status === 401) {
        showAlert("User not authenticated, please reload the page.");
      } else if (error.code === "ERR_NETWORK") {
        showAlert(
          `${error.message}. Try reloading the application if the issue persists.`
        );
      }
      return Promise.reject(error);
    }
  );
}
