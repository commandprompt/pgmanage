<template>
  <PowerTree ref="tree" v-model="nodes" @nodedblclick="doubleClickNode" @toggle="onToggle"
    @nodecontextmenu="onContextMenu" :allow-multiselect="false" @nodeclick="onClickHandler">
    <template v-slot:toggle="{ node }">
      <i v-if="node.isExpanded" class="exp_col fas fa-chevron-down"></i>
      <i v-if="!node.isExpanded" class="exp_col fas fa-chevron-right"></i>
    </template>

    <template v-slot:title="{ node }">
      <span class="item-icon">
        <i :class="['icon_tree', node.data.icon]"></i>
      </span>
      <span v-if="node.data.raw_html" v-html="node.title"> </span>
      <span v-else-if="node.data.type === 'database' && node.title === selectedDatabase
        ">
        <b>{{ node.title }}</b>
      </span>
      <span v-else>
        {{ formatTitle(node) }}
      </span>
    </template>
  </PowerTree>
</template>

<script>
import TreeMixin from "../mixins/power_tree.mjs";
const { PowerTree } = window["VuePowerTree"];

export default {
  name: "TreePostgresql",
  components: {
    PowerTree,
  },
  mixins: [TreeMixin],
  props: {
    databaseIndex: {
      type: Number,
      required: true,
    },
    tabId: {
      type: String,
      required: true,
    },
  },
  data() {
    return {
      nodes: [
        {
          title: "PostgreSQL",
          isExpanded: false,
          isDraggable: false,
          data: {
            icon: "node-postgresql",
            type: "server",
            contextMenu: "cm_server",
          },
        },
      ],
      selectedDatabase:
        window.v_connTabControl.selectedTab.tag.selectedDatabase,
    };
  },
  computed: {
    contextMenu() {
      return {
        cm_server: [this.cmRefreshObject],
        cm_databases: [
          this.cmRefreshObject,
          {
            label: "Create Database",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate("Create Database", this.templates.create_database);
            },
          },
          {
            label: "Doc: Databases",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/managing-databases.html`
              );
            },
          },
        ],
        cm_database: [
          {
            label: "Alter Database",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              //FIXME rewrite to use vue instance
              tabSQLTemplate(
                "Alter Database",
                this.templates.alter_database.replace(
                  "#database_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Database",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Database",
                this.templates.drop_database.replace(
                  "#database_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Backup",
            icon: "fa-solid fa-download cm-all",
            onClick: () => {
              createUtilityTab(this.selectedNode, "Backup");
            },
          },
          {
            label: "Restore",
            icon: "fa-solid fa-upload cm-all",
            onClick: () => {
              createUtilityTab(this.selectedNode, "Restore");
            },
          },
        ],
        cm_schemas: [
          this.cmRefreshObject,
          {
            label: "Create Schema",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate("Create Schema", this.templates.create_schema);
            },
          },
          {
            label: "Doc: Schemas",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-schemas.html`
              );
            },
          },
        ],
        cm_schema: [
          {
            label: "Render Graph",
            icon: "fab cm-all fa-hubspot",
            children: [
              {
                label: "Simple Graph",
                icon: "fab cm-all fa-hubspot",
                onClick: () => {
                  v_connTabControl.tag.createGraphTab(this.selectedNode.title);
                  drawGraph(false, this.selectedNode.title);
                },
              },
              {
                label: "Complete Graph",
                icon: "fab cm-all fa-hubspot",
                onClick: () => {
                  v_connTabControl.tag.createGraphTab(this.selectedNode.title);
                  drawGraph(true, this.selectedNode.title);
                },
              },
            ],
          },
          {
            label: "Backup",
            icon: "fa-solid fa-download cm-all",
            onClick: () => {
              createUtilityTab(this.selectedNode, "Backup");
            },
          },
          {
            label: "Restore",
            icon: "fa-solid fa-upload cm-all",
            onClick: () => {
              createUtilityTab(node, "Restore");
            },
          },
          {
            label: "Alter Schema",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Schema",
                this.templates.alter_schema.replace(
                  "#schema_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Schema",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Schema",
                this.templates.drop_schema.replace(
                  "#schema_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_tables: [
          this.cmRefreshObject,
          {
            label: "Create Table",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Table",
                this.templates.create_table.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Basics",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-basics.html`
              );
            },
          },
          {
            label: "Doc: Constraints",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-constraints.html`
              );
            },
          },
          {
            label: "Doc: Modifying",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-alter.html`
              );
            },
          },
        ],
        cm_table: [
          this.cmRefreshObject,
          {
            label: "Data Actions",
            icon: "fas cm-all fa-list",
            children: [
              {
                label: "Query Data",
                icon: "fas cm-all fa-search",
                onClick: () => {
                  TemplateSelectPostgresql(
                    this.selectedNode.data.schema,
                    this.selectedNode.title,
                    "t"
                  );
                },
              },
              {
                label: "Edit Data",
                icon: "fas cm-all fa-table",
                onClick: () => {
                  v_startEditData(
                    this.selectedNode.title,
                    this.selectedNode.data.schema
                  );
                },
              },
              {
                label: "Insert Record",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateInsertPostgresql(
                    this.selectedNode.data.schema,
                    this.selectedNode.title
                  );
                },
              },
              {
                label: "Update Records",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateUpdatePostgresql(
                    this.selectedNode.data.schema,
                    this.selectedNode.title
                  );
                },
              },
              {
                label: "Delete Records",
                icon: "fas cm-all fa-times",
                onClick: () => {
                  tabSQLTemplate(
                    "Delete Records",
                    this.templates.delete.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Truncate Table",
                icon: "fas cm-all fa-cut",
                onClick: () => {
                  tabSQLTemplate(
                    "Truncate Table",
                    this.templates.truncate.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
            ],
          },
          {
            label: "Table Actions",
            icon: "fas cm-all fa-list",
            children: [
              {
                label: "Vacuum Table",
                icon: "fas cm-all fa-broom",
                onClick: () => {
                  tabSQLTemplate(
                    "Vacuum table",
                    this.templates.vacuum_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Analyze Table",
                icon: "fas cm-all fa-search-plus",
                onClick: () => {
                  tabSQLTemplate(
                    "Analyze Table",
                    this.templates.analyze_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Alter Table",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  tabSQLTemplate(
                    "Alter Table",
                    this.templates.alter_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Edit Comment",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  this.getObjectDescriptionPostgresql(this.selectedNode);
                },
              },
              {
                label: "Drop Table",
                icon: "fas cm-all fa-times",
                onClick: () => {
                  tabSQLTemplate(
                    "Drop Table",
                    this.templates.drop_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Backup",
                icon: "fa-solid fa-download cm-all",
                onClick: () => {
                  createUtilityTab(this.selectedNode, "Backup");
                },
              },
              {
                label: "Restore",
                icon: "fa-solid fa-upload cm-all",
                onClick: () => {
                  createUtilityTab(this.selectedNode, "Restore");
                },
              },
            ],
          },
        ],
        cm_columns: [
          {
            label: "Create Column",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Column",
                this.templates.create_column.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_column: [
          {
            label: "Alter Column",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Column",
                this.templates.alter_column
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace(/#column_name#/g, this.selectedNode.title)
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Column",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Drop Column",
                this.templates.drop_column
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace(/#column_name#/g, this.selectedNode.title)
              );
            },
          },
        ],
        cm_pks: [
          this.cmRefreshObject,
          {
            label: "Create Primary Key",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Primary Key",
                this.templates.create_primarykey.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_pk: [
          this.cmRefreshObject,
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Primary Key",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Primary Key",
                this.templates.drop_primarykey
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#constraint_name#", this.selectedNode.title)
              );
            },
          },
        ],
        cm_fks: [
          this.cmRefreshObject,
          {
            label: "Create Foreign Key",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Foreign Key",
                this.templates.create_foreignkey.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_fk: [
          this.cmRefreshObject,
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Foreign Key",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Foreign Key",
                this.templates.drop_foreignkey
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#constraint_name#", this.selectedNode.title)
              );
            },
          },
        ],
        cm_uniques: [
          this.cmRefreshObject,
          {
            label: "Create Unique",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Unique",
                this.templates.create_unique.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_unique: [
          this.cmRefreshObject,
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Unique",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Unique",
                this.templates.drop_unique
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#constraint_name#", this.selectedNode.title)
              );
            },
          },
        ],
        cm_checks: [
          this.cmRefreshObject,
          {
            label: "Create Check",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Check",
                this.templates.create_check.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_check: [
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Check",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Check",
                this.templates.drop_check
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#constraint_name#", this.selectedNode.title)
              );
            },
          },
        ],
        cm_excludes: [
          this.cmRefreshObject,
          {
            label: "Create Exclude",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Exclude",
                this.templates.create_exclude.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_exclude: [
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Exclude",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Exclude",
                this.templates.drop_exclude
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#constraint_name#", this.selectedNode.title)
              );
            },
          },
        ],
        cm_indexes: [
          this.cmRefreshObject,
          {
            label: "Create Index",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Index",
                this.templates.create_index.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
          {
            label: "Doc: Indexes",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/indexes.html`
              );
            },
          },
        ],
        cm_index: [
          this.cmRefreshObject,
          {
            label: "Alter Index",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Index",
                this.templates.alter_index.replace(
                  "#index_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Reindex",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Reindex",
                this.templates.reindex.replace(
                  "#index_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Index",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Index",
                this.templates.drop_index.replace(
                  "#index_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
        ],
        cm_rules: [
          this.cmRefreshObject,
          {
            label: "Create Rule",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Rule",
                this.templates.create_rule.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
          {
            label: "Doc: Rules",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/rules.html`
              );
            },
          },
        ],
        cm_rule: [
          {
            label: "Alter Rule",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Rule",
                this.templates.alter_rule
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#rule_name#", this.selectedNode.title)
              );
            },
          },
          {
            label: "Edit Rule",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getRuleDefinitionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Rule",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Rule",
                this.templates.drop_rule
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#rule_name#", this.selectedNode.title)
              );
            },
          },
        ],
        cm_triggers: [
          this.cmRefreshObject,
          {
            label: "Create Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Trigger",
                this.templates.create_trigger.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
          {
            label: "Doc: Triggers",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/trigger-definition.html`
              );
            },
          },
        ],
        cm_trigger: [
          {
            label: "Alter Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Trigger",
                this.templates.alter_trigger
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#trigger_name#", this.selectedNode.title)
              );
            },
          },
          {
            label: "Enable Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Enable Trigger",
                this.templates.enable_trigger
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#trigger_name#", this.selectedNode.title)
              );
            },
          },
          {
            label: "Disable Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Disable Trigger",
                this.templates.disable_trigger
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#trigger_name#", this.selectedNode.title)
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Trigger",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Trigger",
                this.templates.drop_trigger
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#trigger_name#", this.selectedNode.title)
              );
            },
          },
          {
            label: "Restore",
            icon: "fa-solid fa-upload cm-all",
            onClick: () => {
              createUtilityTab(this.selectedNode, "Restore");
            },
          },
        ],
        cm_direct_trigger_function: [
          this.cmRefreshObject,
          {
            label: "Edit Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getTriggerFunctionDefinitionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Alter Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Trigger Function",
                this.templates.alter_triggerfunction.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Trigger Function",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Trigger Function",
                this.templates.drop_triggerfunction.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
        ],
        cm_inheriteds: [
          this.cmRefreshObject,
          {
            label: "Create Inherited",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Inherited",
                this.templates.create_inherited.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
          {
            label: "Doc: Partitioning",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-partitioning.html`
              );
            },
          },
        ],
        cm_inherited: [
          {
            label: "No Inherit Table",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "No Inherit Partition",
                this.templates.noinherit_partition
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#partition_name#", this.selectedNode.title)
              );
            },
          },
          {
            label: "Drop Inherited",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Partition",
                this.templates.drop_partition.replace(
                  "#partition_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_partitions: [
          this.cmRefreshObject,
          {
            label: "Create Partition",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Partition",
                this.templates.create_partition.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
          {
            label: "Doc: Partitioning",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-partitioning.html`
              );
            },
          },
        ],
        cm_partition: [
          {
            label: "Detach Partition",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Detach Partition",
                this.templates.detach_partition
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#partition_name#", this.selectedNode.title)
              );
            },
          },
          {
            label: "Drop Partition",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Partition",
                this.templates.drop_partition.replace(
                  "#partition_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_statistics: [
          this.cmRefreshObject,
          {
            label: "Create Statistics",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Statistics",
                this.templates.create_statistics
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                    }`
                  )
                  .replace("#schema_name#", this.selectedNode.data.schema)
              );
            },
          },
          {
            label: "Doc: Statistics",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/planner-stats.html`
              );
            },
          },
        ],
        cm_statistic: [
          this.cmRefreshObject,
          {
            label: "Alter Statistics",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Statistics",
                this.templates.alter_statistics.replace(
                  "#statistics_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Statistics",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Statistics",
                this.templates.drop_statistics.replace(
                  "#statistics_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_partitioned_tables: [
          this.cmRefreshObject,
          {
            label: "Doc: Partitioning",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-partitioning.html`
              );
            },
          },
        ],
        cm_partitioned_parent: [this.cmRefreshObject],
        cm_inherited_tables: [
          this.cmRefreshObject,
          {
            label: "Doc: Inheritance",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/tutorial-inheritance.html`
              );
            },
          },
        ],
        cm_inherited_parent: [this.cmRefreshObject],
        cm_foreign_tables: [
          this.cmRefreshObject,
          {
            label: "Create Foreign Table",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Foreign Table",
                this.templates.create_foreign_table.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
        ],
        cm_foreign_table: [
          this.cmRefreshObject,
          {
            label: "Data Actions",
            icon: "fas cm-all fa-list",
            children: [
              {
                label: "Query Data",
                icon: "fas cm-all fa-search",
                onClick: () => {
                  TemplateSelectPostgresql(
                    this.selectedNode.data.schema,
                    this.selectedNode.title,
                    "f"
                  );
                },
              },
              {
                label: "Edit Data",
                icon: "fas cm-all fa-table",
                onClick: () => {
                  v_startEditData(
                    this.selectedNode.title,
                    this.selectedNode.data.schema
                  );
                },
              },
              {
                label: "Insert Record",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateInsertPostgresql(
                    this.selectedNode.data.schema,
                    this.selectedNode.title
                  );
                },
              },
              {
                label: "Update Records",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateUpdatePostgresql(
                    this.selectedNode.data.schema,
                    this.selectedNode.title
                  );
                },
              },
              {
                label: "Delete Records",
                icon: "fas cm-all fa-times",
                onClick: () => {
                  tabSQLTemplate(
                    "Delete Records",
                    this.templates.delete.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
            ],
          },
          {
            label: "Table Actions",
            icon: "fas cm-all fa-list",
            children: [
              {
                label: "Analyze Foreign Table",
                icon: "fas cm-all fa-table",
                onClick: () => {
                  tabSQLTemplate(
                    "Analyze Foreign Table",
                    this.templates.analyze_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Alter Foreign Table",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  tabSQLTemplate(
                    "Alter Foreign Table",
                    this.templates.alter_foreign_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Edit Comment",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  this.getObjectDescriptionPostgresql(this.selectedNode);
                },
              },
              {
                label: "Drop Foreign Table",
                icon: "fas cm-all fa-times",
                onClick: () => {
                  tabSQLTemplate(
                    "Drop Foreign Table",
                    this.templates.drop_foreign_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
            ],
          },
        ],
        cm_foreign_columns: [
          {
            label: "Create Foreign Column",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Foreign Column",
                this.templates.create_foreign_column.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_foreign_column: [
          {
            label: "Alter Foreign Column",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Foreign Column",
                this.templates.alter_foreign_column
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace(/#column_name#/g, this.selectedNode.title)
              );
            },
          },
          {
            label: "Drop Foreign Column",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Foreign Column",
                this.templates.drop_foreign_column
                  .replace(
                    "#table_name#",
                    `${this.selectedNode.data.schema}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace(/#column_name#/g, this.selectedNode.title)
              );
            },
          },
        ],
        cm_sequences: [
          this.cmRefreshObject,
          {
            label: "Create Sequence",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Sequence",
                this.templates.create_sequence.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Sequences",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/sql-createsequence.html`
              );
            },
          },
        ],
        cm_sequence: [
          {
            label: "Alter Sequence",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Sequence",
                this.templates.alter_sequence.replace(
                  "#sequence_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Sequence",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Sequence",
                this.templates.drop_sequence.replace(
                  "#sequence_name#",
                  `${this.getParentNodeDeep(this.selectedNode, 2).title}.${this.selectedNode.title
                  }`
                )
              );
            },
          },
        ],
        cm_views: [
          this.cmRefreshObject,
          {
            label: "Create View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create View",
                this.templates.create_view.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Views",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/sql-createview.html`
              );
            },
          },
        ],
        cm_view: [
          this.cmRefreshObject,
          {
            label: "Query Data",
            icon: "fas cm-all fa-search",
            onClick: () => {
              TemplateSelectPostgresql(
                this.getParentNodeDeep(this.selectedNode, 2).title,
                this.selectedNode.title,
                "v"
              );
            },
          },
          {
            label: "Edit View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              //FIXME:  do not use v_connTabControl
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getViewDefinitionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Alter View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter View",
                this.templates.alter_view.replace(
                  /#view_name#/g,
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop View",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop View",
                this.templates.drop_view.replace(
                  "#view_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
        ],
        cm_view_triggers: [
          this.cmRefreshObject,
          {
            label: "Create Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Trigger",
                this.templates.create_view_trigger.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
          {
            label: "Doc: Triggers",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/trigger-definition.html`
              );
            },
          },
        ],
        cm_mviews: [
          this.cmRefreshObject,
          {
            label: "Create Mat. View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Materialized View",
                this.templates.create_mview.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Mat. Views",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/sql-creatematerializedview.html`
              );
            },
          },
        ],
        cm_mview: [
          this.cmRefreshObject,
          {
            label: "Query Data",
            icon: "fas cm-all fa-search",
            onClick: () => {
              TemplateSelectPostgresql(
                this.selectedNode.data.schema,
                this.selectedNode.title,
                "m"
              );
            },
          },
          {
            label: "Edit Mat. View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getMaterializedViewDefinitionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Alter Mat. View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Materialized View",
                this.templates.alter_mview.replace(
                  "#view_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Refresh Mat. View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Refresh Materialized View",
                this.templates.refresh_mview.replace(
                  "#view_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Analyze Mat. View",
            icon: "fas cm-all fa-search-plus",
            onClick: () => {
              tabSQLTemplate(
                "Analyze Mat. View",
                this.templates.analyze_table.replace(
                  "#table_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Mat. View",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Materialized View",
                this.templates.drop_mview.replace(
                  "#view_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
        ],
        cm_functions: [
          this.cmRefreshObject,
          {
            label: "Create Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Function",
                this.templates.create_function.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Functions",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/sql-createfunction.html`
              );
            },
          },
        ],
        cm_function: [
          this.cmRefreshObject,
          {
            label: "Select Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              TemplateSelectFunctionPostgresql(
                this.selectedNode.data.schema,
                this.selectedNode.title,
                this.selectedNode.data.id
              );
            },
          },
          {
            label: "Edit Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getFunctionDefinitionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Alter Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Function",
                this.templates.alter_function.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
          {
            label: "Restore",
            icon: "fa-solid fa-upload cm-all",
            onClick: () => {
              createUtilityTab(this.selectedNode, "Restore");
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Function",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Function",
                this.templates.drop_function.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
        ],
        cm_trigger_functions: [
          this.cmRefreshObject,
          {
            label: "Create Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Trigger Function",
                this.templates.create_triggerfunction.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Trigger Functions",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/plpgsql-trigger.html`
              );
            },
          },
        ],
        cm_trigger_function: [
          {
            label: "Edit Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getTriggerFunctionDefinitionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Alter Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Trigger Function",
                this.templates.alter_triggerfunction.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Trigger Function",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Trigger Function",
                this.templates.drop_triggerfunction.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
        ],
        cm_event_trigger_functions: [
          this.cmRefreshObject,
          {
            label: "Create Event Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Event Trigger Function",
                this.templates.create_eventtriggerfunction.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Event Trigger Functions",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/functions-event-triggers.html`
              );
            },
          },
        ],
        cm_event_trigger_function: [
          {
            label: "Edit Event Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getEventTriggerFunctionDefinitionPostgresql(
                this.selectedNode
              );
            },
          },
          {
            label: "Alter Event Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Event Trigger Function",
                this.templates.alter_eventtriggerfunction.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Event Trigger Function",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Event Trigger Function",
                this.templates.drop_eventtriggerfunction.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
        ],
        cm_procedures: [
          this.cmRefreshObject,
          {
            label: "Create Procedure",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Procedure",
                this.templates.create_procedure.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Procedures",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/sql-createprocedure.html`
              );
            },
          },
        ],
        cm_procedure: [
          this.cmRefreshObject,
          {
            label: "Call Procedure",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              TemplateCallProcedurePostgresql(
                this.selectedNode.data.schema,
                this.selectedNode.title,
                this.selectedNode.data.id
              );
            },
          },
          {
            label: "Edit Procedure",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getProcedureDefinitionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Alter Procedure",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Procedure",
                this.templates.alter_procedure.replace(
                  "#procedure_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Procedure",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Procedure",
                this.templates.drop_procedure.replace(
                  "#procedure_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
        ],
        cm_aggregates: [
          this.cmRefreshObject,
          {
            label: "Create Aggregate",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Aggregate",
                this.templates.create_aggregate.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Aggregates",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/sql-createaggregate.html`
              );
            },
          },
        ],
        cm_aggregate: [
          this.cmRefreshObject,
          {
            label: "Alter Aggregate",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Aggregate",
                this.templates.alter_aggregate.replace(
                  "#aggregate_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Aggregate",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Aggregate",
                this.templates.drop_aggregate.replace(
                  "#aggregate_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
        ],
        cm_types: [
          this.cmRefreshObject,
          {
            label: "Create Type",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Type",
                this.templates.create_type.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Types",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/sql-createtype.html`
              );
            },
          },
        ],
        cm_type: [
          {
            label: "Alter Type",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Type",
                this.templates.alter_type.replace(
                  "#type_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Type",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Type",
                this.templates.drop_type.replace(
                  "#type_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
        ],
        cm_domains: [
          this.cmRefreshObject,
          {
            label: "Create Domain",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Domain",
                this.templates.create_domain.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Domains",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/sql-createdomain.html`
              );
            },
          },
        ],
        cm_domain: [
          {
            label: "Alter Domain",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Domain",
                this.templates.alter_domain.replace(
                  "#domain_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Domain",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Domain",
                this.templates.drop_domain.replace(
                  "#domain_name#",
                  `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                )
              );
            },
          },
        ],
        cm_extensions: [
          this.cmRefreshObject,
          {
            label: "Create Extension",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Extension",
                this.templates.create_extension
              );
            },
          },
          {
            label: "Create Extension UI",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              createExtensionModal(this.selectedNode, "Create");
            },
          },
          {
            label: "Doc: Extensions",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/extend-extensions.html`
              );
            },
          },
        ],
        cm_extension: [
          {
            label: "Alter Extension UI",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              createExtensionModal(this.selectedNode, "Alter");
            },
          },
          {
            label: "Alter Extension",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Extension",
                this.templates.alter_extension.replace(
                  "#extension_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Extension UI",
            icon: "fas cm-all fa-times",
            onClick: () => {
              createExtensionModal(this.selectedNode, "Drop");
            },
          },
          {
            label: "Drop Extension",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Role",
                this.templates.drop_extension.replace(
                  "#extension_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_foreign_data_wrappers: [
          this.cmRefreshObject,
          {
            label: "Create Foreign Data Wrapper",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Foreign Data Wrapper",
                this.templates.create_fdw
              );
            },
          },
          {
            label: "Doc: Foreign Data Wrappers",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/postgres-fdw.html`
              );
            },
          },
        ],
        cm_foreign_data_wrapper: [
          {
            label: "Alter Foreign Data Wrapper",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Foreign Data Wrapper",
                this.templates.alter_fdw.replace(
                  "#fdwname#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Foreign Data Wrapper",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Foreign Data Wrapper",
                this.templates.drop_fdw.replace(
                  "#fdwname#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_foreign_servers: [
          this.cmRefreshObject,
          {
            label: "Create Foreign Server",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Foreign Server",
                this.templates.create_foreign_server.replace(
                  "#fdwname#",
                  this.getParentNode(this.selectedNode).title
                )
              );
            },
          },
        ],
        cm_foreign_server: [
          {
            label: "Alter Foreign Server",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Foreign Server",
                this.templates.alter_foreign_server.replace(
                  "#srvname#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Import Foreign Schema",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Import Foreign Schema",
                this.templates.import_foreign_schema.replace(
                  "#srvname#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Foreign Server",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Foreign Server",
                this.templates.drop_foreign_server.replace(
                  "#srvname#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_user_mappings: [
          this.cmRefreshObject,
          {
            label: "Create User Mapping",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create User Mapping",
                this.templates.create_user_mapping.replace(
                  "#srvname#",
                  this.getParentNode(this.selectedNode).title
                )
              );
            },
          },
        ],
        cm_user_mapping: [
          {
            label: "Alter User Mapping",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter User Mapping",
                this.templates.alter_user_mapping
                  .replace("#user_name#", this.selectedNode.title)
                  .replace(
                    "#srvname#",
                    this.getParentNodeDeep(this.selectedNode, 2).title
                  )
              );
            },
          },
          {
            label: "Drop User Mapping",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop User Mapping",
                this.templates.drop_user_mapping
                  .replace("#user_name#", this.selectedNode.title)
                  .replace(
                    "#srvname#",
                    this.getParentNodeDeep(this.selectedNode, 2).title
                  )
              );
            },
          },
        ],
        cm_event_triggers: [
          this.cmRefreshObject,
          {
            label: "Create Event Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Event Trigger",
                this.templates.create_eventtrigger
              );
            },
          },
          {
            label: "Doc: Event Triggers",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/event-triggers.html`
              );
            },
          },
        ],
        cm_event_trigger: [
          {
            label: "Alter Event Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Trigger",
                this.templates.alter_eventtrigger.replace(
                  "#trigger_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Enable Event Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Enable Event Trigger",
                this.templates.enable_eventtrigger.replace(
                  "#trigger_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Disable Event Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Disable Event Trigger",
                this.templates.disable_eventtrigger.replace(
                  "#trigger_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Event Trigger",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Event Trigger",
                this.templates.drop_eventtrigger.replace(
                  "#trigger_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_direct_event_trigger_function: [
          {
            label: "Edit Event Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getEventTriggerFunctionDefinitionPostgresql(
                this.selectedNode
              );
            },
          },
          {
            label: "Alter Event Trigger Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Event Trigger Function",
                this.templates.alter_eventtriggerfunction.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Event Trigger Function",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Event Trigger Function",
                this.templates.drop_eventtriggerfunction.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
        ],
        cm_publications: [
          this.cmRefreshObject,
          {
            label: "Create Publication",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Publication",
                this.templates.create_publication
              );
            },
          },
          {
            label: "Doc: Publications",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/logical-replication-publication.html`
              );
            },
          },
        ],
        cm_publication: [
          {
            label: "Alter Publication",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Publication",
                this.templates.alter_publication.replace(
                  "#pub_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Publication",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Publication",
                this.templates.drop_publication.replace(
                  "#pub_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_pubtables: [
          this.cmRefreshObject,
          {
            label: "Add Table",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Add Table",
                this.templates.add_pubtable.replace(
                  "#pub_name#",
                  this.getParentNode(this.selectedNode).title
                )
              );
            },
          },
        ],
        cm_pubtable: [
          {
            label: "Drop Table",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Table",
                this.templates.drop_pubtable
                  .replace(
                    "#pub_name#",
                    this.getParentNodeDeep(this.selectedNode, 2).title
                  )
                  .replace("#table_name#", this.selectedNode.title)
              );
            },
          },
        ],
        cm_subscriptions: [
          this.cmRefreshObject,
          {
            label: "Create Subscription",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Subscription",
                this.templates.create_subscription
              );
            },
          },
          {
            label: "Doc: Subscriptions",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/logical-replication-subscription.html`
              );
            },
          },
        ],
        cm_subscription: [
          {
            label: "Alter Subscription",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Subscription",
                this.templates.alter_subscription.replace(
                  "#sub_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Subscription",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Subscription",
                this.templates.drop_subscription.replace(
                  "#sub_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_tablespaces: [
          this.cmRefreshObject,
          {
            label: "Create Tablespace",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Tablespace",
                this.templates.create_tablespace
              );
            },
          },
          {
            label: "Doc: Tablespaces",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/manage-ag-tablespaces.html`
              );
            },
          },
        ],
        cm_tablespace: [
          {
            label: "Alter Tablespace",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Tablespace",
                this.templates.alter_tablespace.replace(
                  "#tablespace_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              this.getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Tablespace",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Tablespace",
                this.templates.drop_tablespace.replace(
                  "#tablespace_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_roles: [
          this.cmRefreshObject,
          {
            label: "Create Role",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate("Create Role", this.templates.create_role);
            },
          },
          {
            label: "Doc: Roles",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/user-manag.html`
              );
            },
          },
        ],
        cm_role: [
          {
            label: "Change Password",
            icon: "fas cm-all fa-key",
            onClick: () => {
              //FIXME: rewrite this properly
              let html_text = `<div class="form-row">
                            <div class="col-md-12 mb-3">
                                <label for="change_pwd_role">Password</label>
                                <input type="password" id="change_pwd_role" class="form-control" placeholder="password" />
                            </div>
                            <div class="col-md-12 mb-3">
                                <label for="change_pwd_role_confirm">Password confirmation</label>
                                <input type="password" id="change_pwd_role_confirm" class="form-control" placeholder="password confirmation" />
                            </div>
                        </div>`;

              showConfirm(html_text, () => {
                let password = document.getElementById("change_pwd_role").value;
                let password_confirm = document.getElementById(
                  "change_pwd_role_confirm"
                ).value;

                if (password == "") {
                  this.$toast.error("Password is empty.");
                  return;
                } else if (password_confirm == "") {
                  this.$toast.error("Password confirmation is empty.");
                  return;
                } else if (password != password_confirm) {
                  this.$toast.error("Passwords do not match.");
                  return;
                }

                this.api
                  .post("/change_role_password_postgresql/", {
                    role: this.selectedNode.title,
                    password: password,
                  })
                  .then((resp) => {
                    this.$toast.success("Password changed successfully.");
                  })
                  .catch((error) => {
                    this.nodeOpenError(error, this.selectedNode);
                  });
              });
            },
          },
          {
            label: "Alter Role",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Role",
                this.templates.alter_role.replace(
                  "#role_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Role",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Role",
                this.templates.drop_role.replace(
                  "#role_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_physical_replication_slots: [
          this.cmRefreshObject,
          {
            label: "Create Slot",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Physical Replication Slot",
                this.templates.create_physicalreplicationslot
              );
            },
          },
          {
            label: "Doc: Replication Slots",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/warm-standby.html#streaming-replication-slots`
              );
            },
          },
        ],
        cm_physical_replication_slot: [
          {
            label: "Drop Slot",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Physical Replication Slot",
                this.templates.drop_physicalreplicationslot.replace(
                  "#slot_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_logical_replication_slots: [
          this.cmRefreshObject,
          {
            label: "Create Slot",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Logical Replication Slot",
                this.templates.create_logicalreplicationslot
              );
            },
          },
          {
            label: "Doc: Replication Slots",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/logicaldecoding-explanation.html#logicaldecoding-replication-slots`
              );
            },
          },
        ],
        cm_logical_replication_slot: [
          {
            label: "Drop Slot",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Logical Replication Slot",
                this.templates.drop_logicalreplicationslot.replace(
                  "#slot_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
      };
    },
  },
  methods: {
    refreshTreePostgresqlConfirm(node) {
      if (node.children.length == 0) this.insertSpinnerNode(node);
      if (node.data.type == "server") {
        this.getTreeDetailsPostgresql(node);
      } else if (node.data.type == "database_list") {
        this.getDatabasesPostgresql(node);
      } else if (node.data.type == "database") {
        this.getDatabaseObjectsPostgresql(node);
      } else if (node.data.type == "schema_list") {
        this.getSchemasPostgresql(node);
      } else if (node.data.type == "schema") {
        this.getSchemasObjectsPostgresql(node);
      } else if (node.data.type == "table_list") {
        this.getTablesPostgresql(node);
      } else if (node.data.type == "table") {
        this.getColumnsPostgresql(node);
      } else if (node.data.type == "primary_key") {
        this.getPKPostgresql(node);
      } else if (node.data.type == "pk") {
        this.getPKColumnsPostgresql(node);
      } else if (node.data.type == "foreign_keys") {
        this.getFKsPostgresql(node);
      } else if (node.data.type == "foreign_key") {
        this.getFKsColumnsPostgresql(node);
      } else if (node.data.type == "uniques") {
        this.getUniquesPostgresql(node);
      } else if (node.data.type == "unique") {
        this.getUniquesColumnsPostgresql(node);
      } else if (node.data.type == "check_list") {
        this.getChecksPostgresql(node);
      } else if (node.data.type == "exclude_list") {
        this.getExcludesPostgresql(node);
      } else if (node.data.type == "indexes") {
        this.getIndexesPostgresql(node);
      } else if (node.data.type == "index") {
        this.getIndexesColumnsPostgresql(node);
      } else if (node.data.type == "rule_list") {
        this.getRulesPostgresql(node);
      } else if (node.data.type == "trigger_list") {
        this.getTriggersPostgresql(node);
      } else if (node.data.type == "inherited_list") {
        this.getInheritedsPostgresql(node);
      } else if (node.data.type == "partition_list") {
        this.getPartitionsPostgresql(node);
      } else if (node.data.type == "statistics_list") {
        this.getStatisticsPostgresql(node);
      } else if (node.data.type == "statistic") {
        this.getStatisticsColumnsPostgresql(node);
      } else if (node.data.type == "partitioned_table_list") {
        this.getPartitionedParentsPostgresql(node);
      } else if (node.data.type == "partitioned_parent") {
        this.getPartitionedChildrenPostgresql(node);
      } else if (node.data.type == "inherited_table_list") {
        this.getInheritedsParentsPostgresql(node);
      } else if (node.data.type == "inherited_parent") {
        this.getInheritedsChildrenPostgresql(node);
      } else if (node.data.type == "foreign_table_list") {
        this.getForeignTablesPostgresql(node);
      } else if (node.data.type == "foreign_table") {
        this.getForeignColumnsPostgresql(node);
      } else if (node.data.type == "sequence_list") {
        this.getSequencesPostgresql(node);
      } else if (node.data.type == "view_list") {
        this.getViewsPostgresql(node);
      } else if (node.data.type == "view") {
        this.getViewsColumnsPostgresql(node);
      } else if (node.data.type == "mview_list") {
        this.getMaterializedViewsPostgresql(node);
      } else if (node.data.type == "mview") {
        this.getMaterializedViewsColumnsPostgresql(node);
      } else if (node.data.type == "function_list") {
        this.getFunctionsPostgresql(node);
      } else if (node.data.type == "function") {
        this.getFunctionFieldsPostgresql(node);
      } else if (node.data.type == "trigger_function_list") {
        this.getTriggerFunctionsPostgresql(node);
      } else if (node.data.type == "event_trigger_function_list") {
        this.getEventTriggerFunctionsPostgresql(node);
      } else if (node.data.type == "procedure_list") {
        this.getProceduresPostgresql(node);
      } else if (node.data.type == "procedure") {
        this.getProcedureFieldsPostgresql(node);
      } else if (node.data.type == "aggregate_list") {
        this.getAggregatesPostgresql(node);
      } else if (node.data.type == "aggregate") {
        this.getFunctionFieldsPostgresql(node);
      } else if (node.data.type == "type_list") {
        this.getTypesPostgresql(node);
      } else if (node.data.type == "domain_list") {
        this.getDomainsPostgresql(node);
      } else if (node.data.type == "extension_list") {
        this.getExtensionsPostgresql(node);
      } else if (node.data.type == "foreign_data_wrapper_list") {
        this.getForeignDataWrappersPostgresql(node);
      } else if (node.data.type == "foreign_server_list") {
        this.getForeignServersPostgresql(node);
      } else if (node.data.type == "user_mapping_list") {
        this.getUserMappingsPostgresql(node);
      } else if (node.data.type == "event_trigger_list") {
        this.getEventTriggersPostgresql(node);
      } else if (node.data.type == "publication_list") {
        this.getPublicationsPostgresql(node);
      } else if (node.data.type == "publication_table_list") {
        this.getPublicationTablesPostgresql(node);
      } else if (node.data.type == "subscription_list") {
        this.getSubscriptionsPostgresql(node);
      } else if (node.data.type == "subscription_table_list") {
        this.getSubscriptionTablesPostgresql(node);
      } else if (node.data.type == "tablespace_list") {
        this.getTablespacesPostgresql(node);
      } else if (node.data.type == "role_list") {
        this.getRolesPostgresql(node);
      } else if (node.data.type == "physical_replication_slot_list") {
        this.getPhysicalReplicationSlotsPostgresql(node);
      } else if (node.data.type == "logical_replication_slot_list") {
        this.getLogicalReplicationSlotsPostgresql(node);
      }
    },
    refreshTree(node) {
      this.checkCurrentDatabase(
        node,
        true,
        () => {
          this.refreshTreePostgresqlConfirm(node);
        },
        () => {
          this.toggleNode(node);
        }
      );
    },
    checkCurrentDatabase(
      node,
      complete_check,
      callback_continue,
      callback_stop
    ) {
      if (
        !!node.data.database &&
        node.data.database !== this.selectedDatabase &&
        (complete_check || (!complete_check && node.data.type !== "database"))
      ) {
        let isAllowed = checkBeforeChangeDatabase(callback_stop);
        if (isAllowed) {
          this.api
            .post("/change_active_database/", {
              database: node.data.database,
            })
            .then((resp) => {
              v_connTabControl.selectedTab.tag.divDetails.innerHTML = `<i class="fas fa-server mr-1"></i>selected DB: <b>${node.data.database}</b>`;
              const database_nodes = this.$refs.tree.getNode([0, 0]).children;

              database_nodes.forEach((el) => {
                if (node.data.database === el.title) {
                  this.selectedDatabase = node.data.database;
                  v_connTabControl.selectedTab.tag.selectedDatabase =
                    node.data.database;
                  v_connTabControl.selectedTab.tag.selectedDatabaseNode = el;

                  if (v_connTabControl.selectedTab.tag.selectedTitle != "")
                    v_connTabControl.selectedTab.tag.tabTitle.innerHTML = `<img src="${v_url_folder}/static/assets/images/${v_connTabControl.selectedTab.tag.selectedDBMS}_medium.png"/>${v_connTabControl.selectedTab.tag.selectedTitle} - ${this.selectedDatabase}`;
                  else
                    v_connTabControl.selectedTab.tag.tabTitle.innerHTML = `<img src="${v_url_folder}/static/assets/images/${v_connTabControl.selectedTab.tag.selectedDBMS}_medium.png"/>
                      ${this.selectedDatabase}`;
                }
              });
              if (callback_continue) callback_continue();
            })
            .catch((error) => {
              this.nodeOpenError(error, node);
            });
        }
      } else {
        callback_continue();
      }
    },
    getObjectDescriptionPostgresql(node) {
      let position;
      let oid;
      if (node.data.type === "table_field") {
        oid = this.getParentNodeDeep(node, 2).data.oid;
        position = node.data.position;
      } else if (
        [
          "function",
          "trigger_function",
          "direct_trigger_function",
          "event_trigger_function",
          "direct_event_trigger_function",
          "procedure",
        ].includes(node.data.type)
      ) {
        oid = node.data.function_oid;
        position = 0;
      } else {
        oid = node.data.oid;
        position = 0;
      }

      this.api
        .post("/get_object_description_postgresql/", {
          oid: oid,
          object_type: node.data.type,
          position: position,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.tag.createQueryTab(`${node.title} Comment`);
          let editor =
            v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor;
          editor.setValue(resp.data.data);
          editor.clearSelection();
          editor.gotoLine(0, 0, true);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getTreeDetailsPostgresql(node) {
      this.api
        .post("/get_tree_info_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.contextMenu.cm_server = [this.cmRefreshObject];
          this.contextMenu.cm_server.push(
            {
              label: "Server Configuration",
              icon: "fas cm-all fa-cog",
              onClick: () => {
                createConfTab();
              },
            },
            {
              label: "Backup Server",
              icon: "fa-solid fa-download cm-all",
              onClick: () => {
                createUtilityTab(this.selectedNode, "Backup", "server");
              },
            },
            {
              label: "Restore Server",
              icon: "fa-solid fa-upload cm-all",
              onClick: () => {
                createUtilityTab(this.selectedNode, "Restore", "server");
              },
            },
            {
              label: "Monitoring",
              icon: "fas cm-all fa-chart-line",
              children: [
                {
                  label: "Dashboard",
                  icon: "fas cm-all fa-chart-line",
                  onClick: () => {
                    v_connTabControl.tag.createMonitorDashboardTab();
                    startMonitorDashboard();
                  },
                },
                {
                  label: "Backends",
                  icon: "fas cm-all fa-tasks",
                  onClick: () => {
                    v_connTabControl.tag.createMonitoringTab(
                      "Backends",
                      "SELECT * FROM pg_stat_activity",
                      [
                        {
                          icon: "fas cm-all fa-times",
                          title: "Terminate",
                          action: "postgresqlTerminateBackend",
                        },
                      ]
                    );
                  },
                },
              ],
            },
            {
              label: "Doc: PostgreSQL",
              icon: "fas cm-all fa-globe-americas",
              onClick: () => {
                this.openWebSite(
                  `https://www.postgresql.org/docs/${this.getMajorVersion(
                    this.templates.version
                  )}/static/`
                );
              },
            },
            {
              label: "Doc: SQL Language",
              icon: "fas cm-all fa-globe-americas",
              onClick: () => {
                this.openWebSite(
                  `https://www.postgresql.org/docs/${this.getMajorVersion(
                    this.templates.version
                  )}/static/sql.html`
                );
              },
            },
            {
              label: "Doc: SQL Commands",
              icon: "fas cm-all fa-globe-americas",
              onClick: () => {
                this.openWebSite(
                  `https://www.postgresql.org/docs/${this.getMajorVersion(
                    this.templates.version
                  )}/static/sql-commands.html`
                );
              },
            }
          );

          this.templates = resp.data;

          this.$refs.tree.updateNode(node.path, {
            title: resp.data.version,
          });

          this.insertNode(node, "Replication Slots", {
            icon: "fas node-all fa-sitemap node-repslot-list",
            type: "replication",
          });

          const replication_node = this.getFirstChildNode(node);

          this.insertNode(replication_node, "Logical Replication Slots", {
            icon: "fas node-all fa-sitemap node-repslot-list",
            type: "logical_replication_slot_list",
            contextMenu: "cm_logical_replication_slots",
          });

          this.insertNode(replication_node, "Physical Replication Slots", {
            icon: "fas node-all fa-sitemap node-repslot-list",
            type: "physical_replication_slot_list",
            contextMenu: "cm_physical_replication_slots",
          });

          this.insertNode(node, "Roles", {
            icon: "fas node-all fa-users node-user-list",
            type: "role_list",
            contextMenu: "cm_roles",
          });

          this.insertNode(node, "Tablespaces", {
            icon: "fas node-all fa-folder-open node-tablespace-list",
            type: "tablespace_list",
            contextMenu: "cm_tablespaces",
          });

          this.insertNode(node, "Databases", {
            icon: "fas node-all fa-database node-database-list",
            type: "database_list",
            contextMenu: "cm_databases",
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getDatabasesPostgresql(node) {
      this.api
        .post("/get_databases_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Databases (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-database node-database",
              type: "database",
              contextMenu: "cm_database",
              database: el.name,
              oid: el.oid,
            });
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getDatabaseObjectsPostgresql(node) {
      this.api
        .post("/get_database_objects_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          if (resp.data.has_pg_cron) {
            this.insertNode(node, "Jobs", {
              icon: "fas node-all fa-clock",
              type: "job_list",
              contextMenu: "cm_jobs",
              database: this.selectedDatabase,
            });
          }
          this.insertNode(node, "Logical Replication", {
            icon: "fas node-all fa-sitemap node-logrep",
            type: "replication",
            database: this.selectedDatabase,
          });
          const logical_replication_node = this.getFirstChildNode(node);

          this.insertNode(logical_replication_node, "Subscriptions", {
            icon: "fas node-all fa-arrow-alt-circle-up node-subscription-list",
            type: "subscription_list",
            contextMenu: "cm_subscriptions",
            database: this.selectedDatabase,
          });

          this.insertNode(logical_replication_node, "Publications", {
            icon: "fas node-all fa-arrow-alt-circle-down node-publication-list",
            type: "publication_list",
            contextMenu: "cm_publications",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Event Triggers", {
            icon: "fas node-all fa-bolt node-eventtrigger",
            type: "event_trigger_list",
            contextMenu: "cm_event_triggers",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Foreign Data Wrappers", {
            icon: "fas node-all fa-cube node-fdw-list",
            type: "foreign_data_wrapper_list",
            contextMenu: "cm_foreign_data_wrappers",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Extensions", {
            icon: "fas node-all fa-cubes node-extension-list",
            type: "extension_list",
            contextMenu: "cm_extensions",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Schemas", {
            icon: "fas node-all fa-layer-group node-schema-list",
            type: "schema_list",
            contextMenu: "cm_schemas",
            database: this.selectedDatabase,
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getSchemasPostgresql(node) {
      this.api
        .post("/get_schemas_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Schemas (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-layer-group node-schema",
              type: "schema",
              contextMenu: "cm_schema",
              database: this.selectedDatabase,
              schema: el.name,
              oid: el.oid,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getSchemasObjectsPostgresql(node) {
      this.removeChildNodes(node);

      this.insertNode(node, "Domains", {
        icon: "fas node-all fa-square node-domain-list",
        type: "domain_list",
        contextMenu: "cm_domains",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Types", {
        icon: "fas node-all fa-square node-type-list",
        type: "type_list",
        contextMenu: "cm_types",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Aggregates", {
        icon: "fas node-all fa-cog node-aggregate-list",
        type: "aggregate_list",
        contextMenu: "cm_aggregates",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Procedures", {
        icon: "fas node-all fa-cog node-procedure-list",
        type: "procedure_list",
        contextMenu: "cm_procedures",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Event Trigger Functions", {
        icon: "fas node-all fa-cog node-etfunction-list",
        type: "event_trigger_function_list",
        contextMenu: "cm_event_trigger_functions",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Trigger Functions", {
        icon: "fas node-all fa-cog node-tfunction-list",
        type: "trigger_function_list",
        contextMenu: "cm_trigger_functions",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Functions", {
        icon: "fas node-all fa-cog node-function-list",
        type: "function_list",
        contextMenu: "cm_functions",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Materialized Views", {
        icon: "fas node-all fa-eye node-mview-list",
        type: "mview_list",
        contextMenu: "cm_mviews",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Views", {
        icon: "fas node-all fa-eye node-view-list",
        type: "view_list",
        contextMenu: "cm_views",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Sequences", {
        icon: "fas node-all fa-sort-numeric-down node-sequence-list",
        type: "sequence_list",
        contextMenu: "cm_sequences",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Foreign Tables", {
        icon: "fas node-all fa-th node-ftable-list",
        type: "foreign_table_list",
        contextMenu: "cm_foreign_tables",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Inheritance Tables", {
        icon: "fas node-all fa-th node-itable-list",
        type: "inherited_table_list",
        contextMenu: "cm_inherited_tables",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Partitioned Tables", {
        icon: "fas node-all fa-th node-ptable-list",
        type: "partitioned_table_list",
        contextMenu: "cm_partitioned_tables",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Tables", {
        icon: "fas node-all fa-th node-table-list",
        type: "table_list",
        contextMenu: "cm_tables",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });
    },
    getTablesPostgresql(node) {
      this.api
        .post("/get_tables_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Tables (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-table node-table",
              type: "table",
              contextMenu: "cm_table",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
            });
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getColumnsPostgresql(node) {
      this.api
        .post("/get_columns_postgresql/", {
          table: node.title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.insertNode(node, "Statistics", {
            icon: "fas node-all fa-chart-bar node-statistics",
            type: "statistics_list",
            contextMenu: "cm_statistics",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Partitions", {
            icon: "fas node-all fa-table node-partition",
            type: "partition_list",
            contextMenu: "cm_partitions",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Inherited Tables", {
            icon: "fas node-all fa-table node-inherited",
            type: "inherited_list",
            contextMenu: "cm_inheriteds",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Triggers", {
            icon: "fas node-all fa-bolt node-trigger",
            type: "trigger_list",
            contextMenu: "cm_triggers",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Rules", {
            icon: "fas node-all fa-lightbulb node-rule",
            type: "rule_list",
            contextMenu: "cm_rules",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Indexes", {
            icon: "fas node-all fa-thumbtack node-index",
            type: "indexes",
            contextMenu: "cm_indexes",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Excludes", {
            icon: "fas node-all fa-times-circle node-exclude",
            type: "exclude_list",
            contextMenu: "cm_excludes",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Checks", {
            icon: "fas node-all fa-check-square node-check",
            type: "check_list",
            contextMenu: "cm_checks",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Uniques", {
            icon: "fas node-all fa-key node-unique",
            type: "uniques",
            contextMenu: "cm_uniques",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Foreign Keys", {
            icon: "fas node-all fa-key node-fkey",
            type: "foreign_keys",
            contextMenu: "cm_fks",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Primary Key", {
            icon: "fas node-all fa-key node-pkey",
            type: "primary_key",
            contextMenu: "cm_pks",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, `Columns (${resp.data.length})`, {
            icon: "fas node-all fa-columns node-column",
            type: "column_list",
            contextMenu: "cm_columns",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          const columns_node = this.getFirstChildNode(node);

          resp.data.reduceRight((_, el) => {
            this.insertNode(columns_node, el.column_name, {
              icon: "fas node-all fa-columns node-column",
              type: "table_field",
              contextMenu: "cm_column",
              schema: node.data.schema,
              database: this.selectedDatabase,
              position: el.position,
            });
            const table_field = this.getFirstChildNode(columns_node);

            this.insertNode(
              table_field,
              `Nullable: ${el.nullable}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
            this.insertNode(
              table_field,
              `Type: ${el.data_type}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getPKPostgresql(node) {
      this.api
        .post("/get_pk_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Primary Key (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.constraint_name, {
              icon: "fas node-all fa-key node-pkey",
              type: "pk",
              contextMenu: "cm_pk",
              oid: el.oid,
              schema: node.data.schema,
              database: this.selectedDatabase,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getPKColumnsPostgresql(node) {
      this.api
        .post("/get_pk_columns_postgresql/", {
          key: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el,
              {
                icon: "fas node-all fa-columns node-column",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFKsPostgresql(node) {
      this.api
        .post("/get_fks_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Foreign Keys (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el.constraint_name, {
              icon: "fas node-all fa-key node-fkey",
              type: "foreign_key",
              contextMenu: "cm_fk",
              oid: el.oid,
              schema: node.data.schema,
              database: this.selectedDatabase,
            });
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFKsColumnsPostgresql(node) {
      this.api
        .post("/get_fks_columns_postgresql/", {
          fkey: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              `${el.column_name} <i class='fas node-all fa-arrow-right'></i> ${el.r_column_name}`,
              {
                icon: "fas node-all fa-columns node-column",
                raw_html: true,
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
            this.insertNode(
              node,
              `Update Rule: ${el.update_rule}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
            this.insertNode(
              node,
              `Delete Rule: ${el.delete_rule}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
            this.insertNode(
              node,
              `Referenced Table: ${el.r_table_name}`,
              {
                icon: "fas node-all fa-table node-table",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getUniquesPostgresql(node) {
      this.api
        .post("/get_uniques_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Uniques (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.constraint_name, {
              icon: "fas node-all fa-key node-unique",
              type: "unique",
              contextMenu: "cm_unique",
              oid: el.oid,
              schema: node.data.schema,
              database: this.selectedDatabase,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getUniquesColumnsPostgresql(node) {
      this.api
        .post("/get_uniques_columns_postgresql/", {
          unique: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el,
              {
                icon: "fas node-all fa-columns node-column",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getChecksPostgresql(node) {
      this.api
        .post("/get_checks_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Checks (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.constraint_name, {
              icon: "fas node-all fa-check-square node-check",
              type: "check",
              contextMenu: "cm_check",
              oid: el.oid,
              schema: node.data.schema,
              database: this.selectedDatabase,
            });

            const check_node = this.getFirstChildNode(node);

            this.insertNode(
              check_node,
              el.constraint_source,
              {
                icon: "fas node-all fa-edit node-check-value",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getExcludesPostgresql(node) {
      this.api
        .post("/get_excludes_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Excludes (${resp.data.length})`,
          });

          this.resp.data.forEach((el) => {
            this.insertNode(node, el.constraint_name, {
              icon: "fas node-all fa-times-circle node-exclude",
              type: "exclude",
              contextMenu: "cm_exclude",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
            });

            const exclude_node = this.getFirstChildNode(node);

            this.insertNode(
              exclude_node,
              `Operators: ${el.operations}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );

            this.insertNode(
              exclude_node,
              `Attributes: ${el.attributes}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getIndexesPostgresql(node) {
      this.api
        .post("/get_indexes_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Indexes (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.index_name, {
              icon: "fas node-all fa-thumbtack node-index",
              type: "index",
              contextMenu: "cm_index",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
              uniqueness: el.uniqueness,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getIndexesColumnsPostgresql(node) {
      this.api
        .post("/get_indexes_columns_postgresql/", {
          index: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el,
              {
                icon: "fas node-all fa-columns node-column",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getRulesPostgresql(node) {
      this.api
        .post("/get_rules_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Rules (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.rule_name,
              {
                icon: "fas node-all fa-lightbulb node-rule",
                type: "rule",
                contextMenu: "cm_rule",
                schema: node.data.schema,
                database: this.selectedDatabase,
                oid: el.oid,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getRuleDefinitionPostgresql(node) {
      this.api
        .post("/get_rule_definition_postgresql/", {
          rule: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getTriggersPostgresql(node) {
      this.api
        .post("/get_triggers_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Triggers (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.trigger_name, {
              icon: "fas node-all fa-bolt node-trigger",
              type: "trigger",
              contextMenu: "cm_trigger",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
            });

            const trigger_node = this.getFirstChildNode(node);

            this.insertNode(
              trigger_node,
              el.trigger_function,
              {
                icon: "fas node-all fa-cog node-tfunction",
                type: "direct_trigger_function",
                contextMenu: "cm_direct_trigger_function",
                schema: node.data.schema,
                database: this.selectedDatabase,
                id: el.id,
                function_oid: el.function_oid,
              },
              true
            );

            this.insertNode(
              trigger_node,
              `Enabled: ${el.enabled}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getInheritedsPostgresql(node) {
      this.api
        .post("/get_inheriteds_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Inherited Tables (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el,
              {
                icon: "fas node-all fa-table node-inherited",
                type: "inherit",
                contextMenu: "cm_inherited",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getPartitionsPostgresql(node) {
      this.api
        .post("/get_partitions_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Partitions (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el,
              {
                icon: "fas node-all fa-table node-partition",
                type: "partition",
                contextMenu: "cm_partition",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getStatisticsPostgresql(node) {
      this.api
        .post("/get_statistics_postgresql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Statistics (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, `${el.schema_name}.${el.statistic_name}`, {
              icon: "fas node-all fa-chart-bar node-statistic",
              type: "statistic",
              contextMenu: "cm_statistic",
              database: this.selectedDatabase,
              schema: el.schema_name,
              oid: el.oid,
              statistics: el.statistic_name,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getStatisticsColumnsPostgresql(node) {
      this.api
        .post("/get_statistics_columns_postgresql/", {
          statistics: node.data.statistics,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.column_name,
              {
                icon: "fas node-all fa-columns node-column",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getPartitionedParentsPostgresql(node) {
      this.api
        .post("/get_partitions_parents_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Partitioned Tables (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el, {
              icon: "fas node-all fa-layer-group node-ptable",
              type: "partitioned_parent",
              contextMenu: "cm_partitioned_parent",
              schema: node.data.schema,
              database: this.selectedDatabase,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getPartitionedChildrenPostgresql(node) {
      this.api
        .post("/get_partitions_children_postgresql/", {
          schema: node.data.schema,
          table: node.title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `${node.title} (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-table node-ptable",
              type: "table",
              contextMenu: "cm_table",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getInheritedsParentsPostgresql(node) {
      this.api
        .post("/get_inheriteds_parents_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Inheritance Tables (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el, {
              icon: "fas node-all fa-layer-group node-itable",
              type: "inherited_parent",
              contextMenu: "cm_inherited_parent",
              schema: node.data.schema,
              database: this.selectedDatabase,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getInheritedsChildrenPostgresql(node) {
      this.api
        .post("/get_inheriteds_children_postgresql/", {
          schema: node.data.schema,
          table: node.title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `${node.title} (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-table node-itable",
              type: "table",
              contextMenu: "cm_table",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getForeignTablesPostgresql(node) {
      this.api
        .post("/get_foreign_tables_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Foreign Tables (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-table node-ftable",
              type: "foreign_table",
              contextMenu: "cm_foreign_table",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getForeignColumnsPostgresql(node) {
      this.api
        .post("/get_foreign_columns_postgresql/", {
          table: node.title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.insertNode(node, "Statistics", {
            icon: "fas node-all fa-chart-bar node-statistics",
            type: "statistics_list",
            contextMenu: "cm_statistics",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          if (!!resp.data.length) {
            this.insertNode(
              node,
              resp.data[0].fdw,
              {
                icon: "fas node-all fa-cube node-fdw",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
            this.insertNode(
              node,
              resp.data[0].server,
              {
                icon: "fas node-all fa-server node-server",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );

            let options = resp.data[0].table_options.split(",");
            options.forEach((el) => {
              this.insertNode(
                node,
                el,
                {
                  icon: "fas node-all fa-ellipsis-h node-bullet",
                  schema: node.data.schema,
                  database: this.selectedDatabase,
                },
                true
              );
            });
          }

          this.insertNode(node, `Columns (${resp.data.length})`, {
            icon: "fas node-all fa-columns node-column",
            type: "foreign_column_list",
            contextMenu: "cm_foreign_columns",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          const foreign_columns_node = this.getFirstChildNode(node);

          resp.data.reduceRight((_, el) => {
            this.insertNode(foreign_columns_node, el.column_name, {
              icon: "fas node-all fa-columns node-column",
              type: "foreign_table_field",
              contextMenu: "cm_foreign_column",
              schema: node.data.schema,
              database: this.selectedDatabase,
            });
            const foreign_table_field =
              this.getFirstChildNode(foreign_columns_node);

            if (!!el.options) {
              let options = el.options.split(",");
              options.forEach((option_el) => {
                this.insertNode(
                  foreign_table_field,
                  option_el,
                  {
                    icon: "fas node-all fa-ellipsis-h node-bullet",
                    schema: node.data.schema,
                    database: this.selectedDatabase,
                  },
                  true
                );
              });
            }

            this.insertNode(
              foreign_table_field,
              `Nullable: ${el.nullable}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
            this.insertNode(
              foreign_table_field,
              `Type: ${el.data_type}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getSequencesPostgresql(node) {
      this.api
        .post("/get_sequences_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Sequences (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.sequence_name,
              {
                icon: "fas node-all fa-sort-numeric-down node-sequence",
                type: "sequence",
                contextMenu: "cm_sequence",
                schema: node.data.schema,
                database: this.selectedDatabase,
                oid: el.oid,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getViewsPostgresql(node) {
      this.api
        .post("/get_views_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Views (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-eye node-view",
              type: "view",
              contextMenu: "cm_view",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getViewsColumnsPostgresql(node) {
      this.api
        .post("/get_views_columns_postgresql/", {
          table: node.title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.insertNode(node, "Triggers", {
            icon: "fas node-all fa-bolt node-trigger",
            type: "trigger_list",
            contextMenu: "cm_view_triggers",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Rules", {
            icon: "fas node-all fa-lightbulb node-rule",
            type: "rule_list",
            contextMenu: "cm_rules",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, `Columns (${resp.data.length})`, {
            icon: "fas node-all fa-columns node-column",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          const columns_node = this.getFirstChildNode(node);

          resp.data.reduceRight((_, el) => {
            this.insertNode(columns_node, el.column_name, {
              icon: "fas node-all fa-columns node-column",
              type: "table_field",
              schema: node.data.schema,
              database: this.selectedDatabase,
            });
            const table_field = this.getFirstChildNode(columns_node);

            this.insertNode(
              table_field,
              `Type: ${el.data_type}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getViewDefinitionPostgresql(node) {
      this.api
        .post("/get_view_definition_postgresql/", {
          view: node.title,
          schema: node.data.schema,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getMaterializedViewsPostgresql(node) {
      this.api
        .post("/get_mviews_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Materialized Views (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-eye node-mview",
              type: "mview",
              contextMenu: "cm_mview",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getMaterializedViewsColumnsPostgresql(node) {
      this.api
        .post("/get_mviews_columns_postgresql/", {
          table: node.title,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.insertNode(node, "Statistics", {
            icon: "fas node-all fa-chart-bar node-statistics",
            type: "statistics_list",
            contextMenu: "cm_statistics",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Indexes", {
            icon: "fas node-all fa-thumbtack node-index",
            type: "indexes",
            contextMenu: "cm_indexes",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          this.insertNode(node, `Columns (${resp.data.length})`, {
            icon: "fas node-all fa-columns node-column",
            schema: node.data.schema,
            database: this.selectedDatabase,
          });

          const columns_node = this.getFirstChildNode(node);

          resp.data.reduceRight((_, el) => {
            this.insertNode(columns_node, el.column_name, {
              icon: "fas node-all fa-columns node-column",
              type: "table_field",
              schema: node.data.schema,
              database: this.selectedDatabase,
            });
            const table_field = this.getFirstChildNode(columns_node);

            this.insertNode(
              table_field,
              `Type: ${el.data_type}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                schema: node.data.schema,
                database: this.selectedDatabase,
              },
              true
            );
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getMaterializedViewDefinitionPostgresql(node) {
      this.api
        .post("/get_mview_definition_postgresql/", {
          view: node.title,
          schema: node.data.schema,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFunctionsPostgresql(node) {
      this.api
        .post("/get_functions_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Functions (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-cog node-function",
              type: "function",
              contextMenu: "cm_function",
              schema: node.data.schema,
              database: this.selectedDatabase,
              function_oid: el.function_oid,
              id: el.id,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFunctionFieldsPostgresql(node) {
      this.api
        .post("/get_function_fields_postgresql/", {
          function: node.data.id,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          resp.data.reduceRight((_, el) => {
            if (el.type === "O") {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-arrow-right node-function-field",
                  schema: node.data.schema,
                  database: this.selectedDatabase,
                },
                true
              );
            } else if (el.type === "I") {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-arrow-left node-function-field",
                  schema: node.data.schema,
                  database: this.selectedDatabase,
                },
                true
              );
            } else {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-exchange-alt node-function-field",
                  schema: node.data.schema,
                  database: this.selectedDatabase,
                },
                true
              );
            }
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFunctionDefinitionPostgresql(node) {
      this.api
        .post("/get_function_definition_postgresql/", {
          function: node.data.id,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getTriggerFunctionsPostgresql(node) {
      this.api
        .post("/get_triggerfunctions_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Trigger Functions (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-cog node-tfunction",
                type: "trigger_function",
                contextMenu: "cm_trigger_function",
                schema: node.data.schema,
                database: this.selectedDatabase,
                function_oid: el.function_oid,
                id: el.id,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getTriggerFunctionDefinitionPostgresql(node) {
      this.api
        .post("/get_triggerfunction_definition_postgresql/", {
          function: node.data.id,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getEventTriggerFunctionsPostgresql(node) {
      this.api
        .post("/get_eventtriggerfunctions_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Event Trigger Functions (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-cog node-etfunction",
                type: "event_trigger_function",
                contextMenu: "cm_event_trigger_function",
                schema: node.data.schema,
                database: this.selectedDatabase,
                id: el.id,
                function_oid: el.function_oid,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getEventTriggerFunctionDefinitionPostgresql(node) {
      this.api
        .post("/get_eventtriggerfunction_definition_postgresql/", {
          function: node.data.id,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getProceduresPostgresql(node) {
      this.api
        .post("/get_procedures_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Procedures (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-cog node-procedure",
              type: "procedure",
              contextMenu: "cm_procedure",
              schema: node.data.schema,
              database: this.selectedDatabase,
              id: el.id,
              function_oid: el.function_oid,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getProcedureFieldsPostgresql(node) {
      this.api
        .post("/get_procedure_fields_postgresql/", {
          procedure: node.data.id,
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          resp.data.reduceRight((_, el) => {
            if (el.type === "O") {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-arrow-right node-function-field",
                  schema: node.data.schema,
                  database: this.selectedDatabase,
                },
                true
              );
            } else if (el.type === "I") {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-arrow-left node-function-field",
                  schema: node.data.schema,
                  database: this.selectedDatabase,
                },
                true
              );
            } else {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-exchange-alt node-function-field",
                  schema: node.data.schema,
                  database: this.selectedDatabase,
                },
                true
              );
            }
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getProcedureDefinitionPostgresql(node) {
      this.api
        .post("/get_procedure_definition_postgresql/", {
          procedure: node.data.id,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getAggregatesPostgresql(node) {
      this.api
        .post("/get_aggregates_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Aggregates (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-cog node-aggregate",
              type: "aggregate",
              contextMenu: "cm_aggregate",
              schema: node.data.schema,
              database: this.selectedDatabase,
              id: el.id,
              oid: el.oid,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getTypesPostgresql(node) {
      this.api
        .post("/get_types_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Types (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.type_name,
              {
                icon: "fas node-all fa-square node-type",
                type: "type",
                contextMenu: "cm_type",
                schema: node.data.schema,
                database: this.selectedDatabase,
                oid: el.oid,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getDomainsPostgresql(node) {
      this.api
        .post("/get_domains_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Domains (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.domain_name,
              {
                icon: "fas node-all fa-square node-domain",
                type: "domain",
                contextMenu: "cm_domain",
                schema: node.data.schema,
                database: this.selectedDatabase,
                oid: el.oid,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getExtensionsPostgresql(node) {
      this.api
        .post("/get_extensions_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Extensions (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-cubes node-extension",
                type: "extension",
                contextMenu: "cm_extension",
                database: this.selectedDatabase,
                oid: el.oid,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getForeignDataWrappersPostgresql(node) {
      this.api
        .post("/get_foreign_data_wrappers_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Foreign Data Wrappers (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-cube node-fdw",
              type: "foreign_data_wrapper",
              contextMenu: "cm_foreign_data_wrapper",
              database: this.selectedDatabase,
              oid: el.oid,
            });

            const fdw_node = this.getFirstChildNode(node);

            this.insertNode(fdw_node, "Foreign Servers", {
              icon: "fas node-all fa-server node-server",
              type: "foreign_server_list",
              contextMenu: "cm_foreign_servers",
              database: this.selectedDatabase,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getForeignServersPostgresql(node) {
      this.api
        .post("/get_foreign_servers_postgresql/", {
          fdw: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Foreign Servers (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-server node-server",
              type: "foreign_server",
              contextMenu: "cm_foreign_server",
              database: this.selectedDatabase,
              oid: el.oid,
            });
            const foreign_server_node = this.getFirstChildNode(node);

            this.insertNode(foreign_server_node, "User Mappings", {
              icon: "fas node-all fa-user-friends node-user",
              type: "user_mapping_list",
              contextMenu: "cm_user_mappings",
              database: this.selectedDatabase,
            });

            let options = el.options.split(",");
            options.forEach((option_el) => {
              this.insertNode(
                foreign_server_node,
                option_el,
                {
                  icon: "fas node-all fa-ellipsis-h node-bullet",
                  database: this.selectedDatabase,
                },
                true
              );
            });

            this.insertNode(
              foreign_server_node,
              `Version: ${el.version}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );

            this.insertNode(
              foreign_server_node,
              `Type: ${el.type}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getUserMappingsPostgresql(node) {
      this.api
        .post("/get_user_mappings_postgresql/", {
          foreign_server: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `User Mappings (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-user-friends node-user",
              type: "user_mapping",
              contextMenu: "cm_user_mapping",
              database: this.selectedDatabase,
              foreign_server: el.foreign_server,
            });

            const user_mapping_node = this.getFirstChildNode(node);

            let options = el.options.split(",");

            options.forEach((option_el) => {
              this.insertNode(
                user_mapping_node,
                option_el,
                {
                  icon: "fas node-all fa-ellipsis-h node-bullet",
                  database: this.selectedDatabase,
                },
                true
              );
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getEventTriggersPostgresql(node) {
      this.api
        .post("/get_eventtriggers_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Event Triggers (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-bolt node-eventtrigger",
              type: "event_trigger",
              contextMenu: "cm_event_trigger",
              database: this.selectedDatabase,
              oid: el.oid,
            });
            const trigger_node = this.getFirstChildNode(node);

            this.insertNode(
              trigger_node,
              el.function,
              {
                icon: "fas node-all fa-cog node-etfunction",
                type: "direct_event_trigger_function",
                contextMenu: "cm_direct_event_trigger_function",
                id: el.id,
                function_oid: el.function_oid,
              },
              true
            );

            this.insertNode(
              trigger_node,
              `Event: ${el.event}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );

            this.insertNode(
              trigger_node,
              `Enabled: ${el.enabled}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getPublicationsPostgresql(node) {
      this.api
        .post("/get_publications_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Publications (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-arrow-alt-circle-down node-publication",
              type: "publication",
              contextMenu: "cm_publication",
              database: this.selectedDatabase,
              oid: el.oid,
            });
            const publication_node = this.getFirstChildNode(node);

            if (el.alltables === "False") {
              this.insertNode(publication_node, "Tables", {
                icon: "fas node-all fa-th node-table-list",
                type: "publication_table_list",
                contextMenu: "cm_pubtables",
                database: this.selectedDatabase,
              });
            }

            this.insertNode(
              publication_node,
              `Truncate: ${el.truncate}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );

            this.insertNode(
              publication_node,
              `Delete: ${el.delete}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );

            this.insertNode(
              publication_node,
              `Update: ${el.update}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );

            this.insertNode(
              publication_node,
              `Insert: ${el.insert}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );

            this.insertNode(
              publication_node,
              `All Tables: ${el.alltables}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getPublicationTablesPostgresql(node) {
      this.api
        .post("/get_publication_tables_postgresql/", {
          pub: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Tables (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-table node-table",
                type: "pubtable",
                contextMenu: "cm_pubtable",
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getSubscriptionsPostgresql(node) {
      this.api
        .post("/get_subscriptions_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Subscriptions (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-arrow-alt-circle-up node-subscription",
              type: "subscription",
              contextMenu: "cm_subscription",
              database: this.selectedDatabase,
              oid: el.oid,
            });
            const subscription_node = this.getFirstChildNode(node);

            this.insertNode(subscription_node, "Tables", {
              icon: "fas node-all fa-th node-table-list",
              type: "subscription_table_list",
              database: this.selectedDatabase,
            });

            this.insertNode(subscription_node, "Referenced Publications", {
              icon: "fas node-all fa-arrow-alt-circle-down node-publication",
              type: "subpubs",
              database: this.selectedDatabase,
            });
            const referenced_publications =
              this.getFirstChildNode(subscription_node);

            const publications = el.publications.split(",");
            publications.forEach((pub_el) => {
              this.insertNode(
                referenced_publications,
                pub_el,
                {
                  icon: "fas node-all fa-arrow-alt-circle-down node-publication",
                  type: "subpub",
                  database: this.selectedDatabase,
                },
                true
              );
            });

            this.insertNode(
              subscription_node,
              `ConnInfo: ${el.conn_info}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );

            this.insertNode(
              subscription_node,
              `Enabled: ${el.enabled}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getSubscriptionTablesPostgresql(node) {
      this.api
        .post("/get_subscription_tables_postgresql/", {
          sub: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Tables (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-table node-table",
                type: "subtable",
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getTablespacesPostgresql(node) {
      this.api
        .post("/get_tablespaces_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Tablespaces (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-folder node-tablespace",
                type: "tablespace",
                contextMenu: "cm_tablespace",
                oid: el.oid,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getRolesPostgresql(node) {
      this.api.post("/get_roles_postgresql/").then((resp) => {
        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Roles (${resp.data.data.length})`,
        });

        resp.data.data.reduceRight((_, el) => {
          this.insertNode(
            node,
            el.name,
            {
              icon: "fas node-all fa-user node-user",
              type: "role",
              contextMenu: "cm_role",
              oid: el.oid,
            },
            true
          );
        }, null);
      });
    },
    getPhysicalReplicationSlotsPostgresql(node) {
      this.api
        .post("/get_physicalreplicationslots_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Physical Replication Slots (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-sitemap node-repslot",
                type: "physical_replication_slot",
                contextMenu: "cm_physical_replication_slot",
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getLogicalReplicationSlotsPostgresql(node) {
      this.api
        .post("/get_logicalreplicationslots_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Logical Replication Slots (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-sitemap node-repslot",
                type: "logical_replication_slot",
                contextMenu: "cm_logical_replication_slot",
                database: this.selectedDatabase,
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    openWebSite(site) {
      window.open(site, "_blank");
    },
    getMajorVersion(version) {
      // FIXME
      let v_version = version.split(" (")[0];
      let tmp = v_version
        .replace("PostgreSQL ", "")
        .replace("beta", ".")
        .replace("rc", ".")
        .split(".");
      tmp.pop();
      return tmp.join(".");
    },
  },
};
</script>
