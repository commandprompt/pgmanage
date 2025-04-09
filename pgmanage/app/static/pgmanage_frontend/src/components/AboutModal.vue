<template>
  <div class="modal fade" id="modal_about" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">About</h2>
          <button
            type="button"
            class="btn-close"
            data-bs-dismiss="modal"
            aria-label="Close"
          ></button>
        </div>

        <div class="modal-body">
          <div class="row mb-3">
            <div class="col-5">
              <p class="font-weight-semibold">Version:</p>
            </div>
            <div class="col-7">{{ pgmanageVersion }}</div>
          </div>

          <div class="row mb-3">
            <div class="col-5 d-flex align-items-center">
              <p class="font-weight-semibold mb-0">Supported databases:</p>
            </div>

            <div
              class="col-7 modal-about__db d-flex justify-content-between align-items-center"
            >
              <img
                :src="postgresqlIcon"
                title="PostgreSQL"
              />
              <img
                :src="oracleIcon"
                title="Oracle"
              />
              <img
                :src="mysqlIcon"
                title="MySQL"
              />
              <img
                :src="sqliteIcon"
                title="SQLite"
              />
              <img
                :src="mariadbIcon"
                title="MariaDB"
              />
            </div>
          </div>

          <div class="row mb-3">
            <div class="col-5">
              <p class="font-weight-semibold">Primary Supporter:</p>
            </div>
            <div class="col-7">
              <a
                data-testid="supporter-link"
                target="_blank"
                class="font-weight-bold"
                href="https://www.commandprompt.com/"
              >
                Command Prompt Inc
                <!-- <img src="{{url_folder}}/static/OmniDB_app/images/supporters/command_prompt_inc.png" title="CommandPrompt Inc."/> -->
              </a>
            </div>
          </div>

          <div class="row mb-3">
            <div class="col-5">
              <p class="font-weight-semibold">Website:</p>
            </div>
            <div class="col-7">
              <a
                data-testid="pgmanage-github-link"
                class="font-weight-bold"
                target="_blank"
                href="https://github.com/commandprompt/pgmanage"
                >PgManage</a
              >
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import postgresqlLargeIcon from '@src/assets/images/db_icons/postgresql_large.png'
import mysqlLargeIcon from '@src/assets/images/db_icons/mysql_large.png'
import mariadbLargeIcon from '@src/assets/images/db_icons/mariadb_large.png'
import oracleLargeIcon from '@src/assets/images/db_icons/oracle_large.png'
import sqliteLargeIcon from '@src/assets/images/db_icons/sqlite_large.png'


export default {
    name: "About Modal",
    data() {
        return {
            postgresqlIcon: postgresqlLargeIcon,
            mysqlIcon: mysqlLargeIcon,
            mariadbIcon: mariadbLargeIcon,
            oracleIcon: oracleLargeIcon,
            sqliteIcon: sqliteLargeIcon,
            pgmanageVersion: window.pgmanage_version
        }
    }
}
</script>
