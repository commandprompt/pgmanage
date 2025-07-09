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
import axios from "axios";
import ShortUniqueId from "short-unique-id";
import cytoscape from "cytoscape";
import { handleError } from "../logging/utils";
import debounce from "lodash/debounce";

export default {
  name: "ERDTab",
  props: {
    schema: String,
    workspaceId: String,
    tabId: String,
    databaseIndex: Number,
    databaseName: String,
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
            selector: "node",
            style: {
              shape: "round-rectangle",
              "background-color": "#F8FAFC",
              "background-opacity": 1,
            },
          },
          {
            selector: "node.table",
            style: {
              label: "data(label)",
              "text-margin-y": 20,
              padding: 25,
              width: 40,
            },
          },
          {
            selector: "edge",
            style: {
              "curve-style": "straight",
              "target-arrow-shape": "triangle",
              width: 2,
              "line-style": "solid",
            },
          },
          {
            selector: "edge:selected",
            style: {
              width: 4,
              "line-color": "#F76707",
              "target-arrow-color": "#F76707",
              "source-arrow-color": "#F76707",
            },
          },
          {
            selector: "node.column-node",
            style: {
              label: (ele) => {
                const icon =
                  ele.data("is_pk") || ele.data("is_fk") ? "\u{1F511}" : "";
                return (
                  icon + ele.data("name") + "               " + ele.data("type")
                );
              },
              "text-valign": "center",
              "text-halign": "center",
              "background-color": "#F8FAFC",
              height: 10,
              shape: "rectangle",
              "font-size": 10,
              padding: 2,
              width: 140,
            },
          },
          {
            selector: "node.pk-column",
            style: {
              "background-color": "#D1FAE5",
              "border-color": "#10B981",
              "border-width": 2,
            },
          },
          {
            selector: "node.fk-column",
            style: {
              "background-color": "#FEF3C7",
              "border-color": "#F59E0B",
              "border-width": 2,
            },
          },
        ],
      },
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
      }, 100);
    },
    loadSchemaGraph() {
      axios
        .post("/draw_graph/", {
          database_index: this.databaseIndex,
          workspace_id: this.workspaceId,
          schema: this.schema,
        })
        .then((response) => {
          if (response.data.layout) {
            this.jsonLayout = response.data.layout;
            this.new_nodes = response.data.new_nodes;
            this.new_edges = response.data.new_edges;
          } else {
            response.data.nodes.forEach((table) => {
              this.nodes.push({
                data: {
                  id: table.id,
                  label: table.label,
                  type: "table",
                },
                position: table?.position ?? {},
                classes: "table",
              });

              table.columns.forEach((col, idx) => {
                const colId = `${table.id}_${col.name}`;
                this.nodes.push({
                  data: {
                    id: colId,
                    name: col.name,
                    parent: table.id,
                    is_pk: col.is_pk,
                    is_fk: col.is_fk,
                    cgid: col.cgid,
                    type: this.shortDataType(col.type),
                  },
                  classes: `column-node${col.is_pk ? " pk-column" : ""}${
                    col.is_fk ? " fk-column" : ""
                  }`,
                  position: {
                    x: 0,
                    y: 22 * idx,
                  },
                });
              });
            });

            this.edges = response.data.edges.map((edge) => {
              let source = !!edge.from_col
                ? `${edge.from}_${edge.from_col}`
                : edge.from;
              let target = !!edge.to_col
                ? `${edge.to}_${edge.to_col}`
                : edge.to;
              return {
                data: {
                  id: edge.cgid,
                  source: source,
                  target: target,
                  label: edge.label,
                  cgid: edge.cgid,
                },
              };
            });
          }
        })
        .then(() => {
          this.initGraph();
        })
        .catch((error) => {
          handleError(error);
        });
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
      let classes = [];
      if (column.is_pk) classes.push("pk-column");
      if (column.is_fk) classes.push("fk-column");
      if (column.is_highlighted) classes.push("highlighted");
      return classes.join(" ");
    },
    initGraph() {
      if (this.jsonLayout) {
        this.cy = cytoscape({
          container: this.$refs.cyContainer,
          ...this.options,
          elements: [],
        })
        this.cy.json(this.jsonLayout)

        if (this.new_nodes && this.new_nodes.length > 0) {
          const formattedNodes = this.new_nodes.map(node => ({
            group: 'nodes',
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
            classes: 'group' + node.group
          }));
          this.cy.add(formattedNodes);
        }

        if (this.new_edges && this.new_edges.length > 0) {
          const formattedEdges = this.new_edges.map(edge => ({
            group: 'edges',
            data: {
              source: edge.from,
                target: edge.to,
                source_col: edge.from_col,
                target_col: edge.to_col,
                label: edge.label,
                cgid: edge.cgid
            }
          }));
          this.cy.add(formattedEdges);
        }
        
      } else {
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
        
        setTimeout(() => {
          this.adjustSizes()
        }, 100)
      }

      this.setupEvents();

      this.$refs.cyContainer.style.visibility = "visible";
    },
    adjustSizes() {
      const columnHeight = 16;
      const columnSpacing = 6;
      const minTableHeight = 40;
      const tablePadding = 20;

      const tables = this.cy.nodes().filter((n) => n.isParent());

      tables.forEach((table) => {
        const columns = table.children();
        const totalHeight = Math.max(
          columns.length * (columnHeight + columnSpacing) + tablePadding,
          minTableHeight
        );

        table.style({
          width: 100,
          height: totalHeight,
        });

        columns.forEach((colNode, idx) => {
          const yOffset = (columnHeight + columnSpacing) * idx;
          colNode.position({
            x: table.position("x"),
            y: table.position("y") + tablePadding + yOffset + 20,
          });
        });
      });
      this.cy.fit();
    },
    saveGraphState() {
      const layoutData = this.cy.json(); 

      axios.post('/save_graph_state/', {
        workspace_id: this.workspaceId,
        schema: this.schema,
        database_name: this.databaseName,
        database_index: this.databaseIndex,
        layout: layoutData,
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

      // this.cy.on("dragfree", () => {
      //   this.saveGraphState();
      // });

      // this.cy.on('viewport', debounce(() => {
      //   this.saveGraphState();
      // }, 500));
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
