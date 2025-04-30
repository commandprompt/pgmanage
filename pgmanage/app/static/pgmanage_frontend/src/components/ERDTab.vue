<template>
  <div>
    <div class="d-flex justify-content-between gap-2 p-2 border-bottom bg-light">
      <button class="btn btn-outline-secondary btn-sm" @click="resetToDefault" title="Reset to default">
        Reset
      </button>
      
      <div class="float-right">
        <!-- Zoom In -->
        <button class="btn btn-outline-secondary btn-sm me-2" @click="zoomIn" title="Zoom In">
          <i class="fas fa-search-plus"></i>
        </button>
  
        <!-- Zoom Out -->
        <button class="btn btn-outline-secondary btn-sm" @click="zoomOut" title="Zoom Out">
          <i class="fas fa-search-minus"></i>
        </button>

      </div>
    </div>
  <div class="pt-3" style="width: 100%; height: calc(100vh - 70px); visibility: hidden" ref="cyContainer"></div>

  </div>
</template>

<script>
import axios from 'axios'
import ShortUniqueId from 'short-unique-id'
import cytoscape from 'cytoscape';
import nodeHtmlLabel from 'cytoscape-node-html-label'
import { tabsStore } from '../stores/stores_initializer';
import { handleError } from '../logging/utils';
import isEmpty from 'lodash/isEmpty';


export default {
  name: "ERDTab",
  props: {
    schema: String,
    workspaceId: String,
    tabId: String,
    databaseIndex: Number,
    databaseName: String,
  },
  setup(props) {
    if (typeof cytoscape("core", "nodeHtmlLabel") === "undefined")
      nodeHtmlLabel(cytoscape);
  },
  data() {
    return {
      nodes: [],
      edges: [],
      cy: {},
      layout: {},
      instance_uid: '',
      options: {
        boxSelectionEnabled: false,
        wheelSensitivity: 0.4,
        style: [
          {
            selector: 'node',
            style: {
              "shape": "round-rectangle",
              "background-color": "#F8FAFC",
              "background-opacity": 1,
              "height": 40,
              "width": 140,
              shape: "round-rectangle",
            }
          },
          {
            selector: 'edge',
            style: {
              'curve-style': 'straight',
              'target-arrow-shape': 'triangle',
              'width': 2,
              'line-style': 'solid'
            }
          },
          {
            selector: 'edge:selected',
            style: {
              'width': 4,
              'line-color': '#F76707',
              'target-arrow-color': '#F76707',
              'source-arrow-color': '#F76707',
            }
          },
        ],
      }
    };
  },
  mounted() {
    this.loadSchemaGraph()
    this.instance_uid = new ShortUniqueId({dictionary: 'alpha_upper', length: 4}).randomUUID()
  },
  updated() {
    this.$refs.cyContainer.style.visibility = 'visible';
  },
  methods: {
    resetToDefault() {
      this.layout = this.cy.layout({
          name: "grid",
          padding: 50,
          spacingFactor: 0.85,
        })

        setTimeout(() => {
        this.adjustSizes()
        this.saveGraphState();
      }, 100)
    },
    loadSchemaGraph() {
      axios.post('/draw_graph/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        schema: this.schema,
      })
      .then((response) => {
        this.isDefault = !response.data.nodes[0]?.position
        this.nodes = response.data.nodes.map((node) => (
          {
            data: {
              id: node.id,
              html_id: node.id.replace(/[^a-zA-Z_.-:]+/, '_'),
              label: node.label,
              columns: node.columns.map((column) => (
                {
                  name: column.name,
                  type: this.shortDataType(column.type),
                  cgid: column.cgid,
                  is_pk: column.is_pk,
                  is_fk: column.is_fk,
                  is_highlighted: false
                }
              )),
              type: 'table'
            },
            position: node?.position ?? {},
            classes: 'group' + node.group
          }
        ))

        this.edges = response.data.edges.map((edge) => (
          {
            data: {
              source: edge.from,
              target: edge.to,
              source_col: edge.from_col,
              target_col: edge.to_col,
              label: edge.label,
              cgid: edge.cgid
            }
          }
        ))
      })
      .then(() => { this.initGraph() })
      .catch((error) => {
        handleError(error);
      })
    },
    shortDataType(typename) {
      const TYPEMAP = {
        'character varying': 'varchar',
        'timestamp with time zone': 'timestamptz',
        'timestamp without time zone': 'timestamp',
        'time without time zone': 'time',
        'time with time zone': 'timetz',
        'character': 'char',
        'boolean': 'bool'
      }
      return TYPEMAP[typename] || typename
    },
    columnClass(column) {
      let classes = []
      if(column.is_pk)
        classes.push('pk-column')
      if(column.is_fk)
        classes.push('fk-column')
      if(column.is_highlighted)
        classes.push('highlighted')
      return classes.join(' ')
    },
    initGraph() {
      if (this.isDefault) {
        this.cy = cytoscape({
          container: this.$refs.cyContainer,
          ...this.options,
          elements: {
            selectable: true,
            grabbable: false,
            nodes: [...this.nodes],
            edges: [...this.edges]
          }
        })
        this.layout = this.cy.layout({
          name: "grid",
          padding: 50,
          spacingFactor: 0.85,
        })

      } else {
        this.cy = cytoscape({
          container: this.$refs.cyContainer,
          ...this.options,
          elements: {
            selectable: true,
            grabbable: false,
            nodes: [...this.nodes],
            edges: [...this.edges]
          },
          layout: {
            name: 'preset'
          }
        })
      }
      this.setupEvents();

      this.cy.nodeHtmlLabel(
        [{
          query: 'node',
          cssClass: 'erd-card',
          tpl: (function(data) {
            let coldivs = ''
            if (data.columns)
              coldivs = data.columns.map((c) => {
                let dataAttr = c.cgid ? `data-cgid="${c.cgid}"` : ''
                let colName = c.is_fk ?
                `<a ${dataAttr} href="#" class="erd-card__column_name">${c.name}</a>` :
                `<span class="erd-card__column_name">${c.name}</span>`
                return `<div ${dataAttr} class="erd-card__column ${this.columnClass(c)}">
                      ${colName}
                  <span class="erd-card__column_type">${c.type}</span>
                </div>`
              }).join('')

            return `<div class="erd-card__wrap"><div id="${this.instance_uid}-${data.html_id}">
                <h3 class="erd-card__title clipped-text" title="${data.label}">${data.label}</h3>
                ${coldivs}
            </div></div>`;
          }).bind(this)
        }],
      )

      setTimeout(() => {
        this.adjustSizes()
      }, 100)
    },
    adjustSizes() {
      const padding = 2;
        this.cy.nodes().forEach((node) => {
          let el = document.querySelector(`#${this.instance_uid}-${node.data().html_id}`)
          if (el) {
            node.style('width', el.parentElement.clientWidth + padding)
            node.style('height', el.parentElement.clientHeight + padding)
          }
        })
        if (!isEmpty(this.layout)) this.layout.run()
        this.cy.fit()
        this.$refs.cyContainer.style.visibility = 'visible'
    },
    saveGraphState() {
      const state = this.cy.nodes().map(node => ({
        id: node.id(), // table name
        position: node.position(),
      }));

      axios.post('/save_graph_state/', {
        workspace_id: this.workspaceId,
        schema: this.schema,
        database_name: this.databaseName,
        database_index: this.databaseIndex,
        node_positions: state,
      }).catch((error) => {
        handleError(error);
      });
    },
    setupEvents() {
      this.cy.on('select unselect', 'edge', function(evt) {
        let should_highlight = evt.type == 'select'
        let {source_col, target_col} = evt.target.data()
        let edge = evt.target
        let srccols = edge.source().data('columns')
        srccols.find((c) => c.name === source_col).is_highlighted = should_highlight
        edge.source().data('columns', srccols)
        let dstcols = edge.target().data('columns')
        dstcols.find((c) => c.name === target_col).is_highlighted = should_highlight
        edge.target().data('columns', dstcols)
      });

      this.cy.on('click', 'node', function (evt) {
        if (evt.originalEvent) {
          const element = document.elementFromPoint(evt.originalEvent.clientX, evt.originalEvent.clientY);
          if(element.dataset.cgid) {
            let edge = this.cy().edges().filter(( ele ) => ele.data('cgid') === element.dataset.cgid)
            setTimeout(() => {edge.select()}, 1)
          }
        }
      });

      this.cy.on("resize", () => {
        if(!(tabsStore.selectedPrimaryTab.metaData.selectedTab.id === this.tabId)) return;
        this.cy.fit()
      });

      this.cy.on("dragfree", () => {
        this.saveGraphState();
      });
    },
    zoomIn() {
      if (this.cy) {
        const currentZoom = this.cy.zoom();
        this.cy.zoom({
          level: currentZoom * 1.2,
          renderedPosition: { x: this.cy.width() / 2, y: this.cy.height() / 2 }
        });
      }
    },
    zoomOut() {
      if (this.cy) {
        const currentZoom = this.cy.zoom();
        this.cy.zoom({
          level: currentZoom * 0.8,
          renderedPosition: { x: this.cy.width() / 2, y: this.cy.height() / 2 }
        });
      }
    },
  },
};
</script>

<style scoped>

</style>