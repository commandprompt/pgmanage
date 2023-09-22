<template>
  <div style="width: 100%; height: calc(100vh - 70px); visibility: hidden" ref="cyContainer"></div>
</template>

<script>
import axios from 'axios'
import ShortUniqueId from 'short-unique-id'
import cytoscape from 'cytoscape';
import nodeHtmlLabel from 'cytoscape-node-html-label'

// const uid = new ShortUniqueId({dictionary: 'alpha_upper', length: 4})

// TODO: add fit/pan/zoom buttons
// TODO: handlecontainer resize

export default {
  name: "ERDTab",
  props: {
    schema: String,
    tab_id: String,
    database_index: Number,
    database_name: String,
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
      instance_uid: ''
    };
  },
  mounted() {
    this.loadSchemaGraph()
    this.instance_uid = new ShortUniqueId({dictionary: 'alpha_upper', length: 4}).randomUUID()
  },
  methods: {
    loadSchemaGraph() {
      axios.post('/draw_graph/', {
        database_index: this.database_index,
        tab_id: this.tab_id,
        schema: this.schema,
      })
      .then((response) => {
        this.nodes = response.data.v_data.nodes.map((node) => (
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
                  is_highlighed: false
                }
              )),
              type: 'table'
            },
            position: {},
            classes: 'group' + node.group
          }
        ))

        this.edges = response.data.v_data.edges.map((edge) => (
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
        console.log(error)
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
      if(column.is_highlighed)
        classes.push('highlighted')
      return classes.join(' ')
    },
    initGraph() {
      this.cy = cytoscape({
        container: this.$refs.cyContainer,
        boxSelectionEnabled: false,
        wheelSensitivity: 0.4,
        style: [
          {
            selector: 'node',
            style: {
              "shape": "round-rectangle",
              "background-color": "#F8FAFC",
              "background-opacity": 0,
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
        elements: {
          selectable: true,
          grabbable: false,
          nodes: this.nodes,
          edges: this.edges
        }
      })

      this.cy.on('select unselect', 'edge', function(evt) {
        let should_highlight = evt.type == 'select'
        let {source_col, target_col} = evt.target.data()
        let edge = evt.target
        let srccols = edge.source().data('columns')
        srccols.find((c) => c.name === source_col).is_highlighed = should_highlight
        edge.source().data('columns', srccols)
        let dstcols = edge.target().data('columns')
        dstcols.find((c) => c.name === target_col).is_highlighed = should_highlight
        edge.target().data('columns', dstcols)
      })

      this.layout = this.cy.layout({
        name: 'grid',
        padding: 50,
        spacingFactor: 0.85,
      })

      this.cy.on('click', 'node', function (evt) {
        if (evt.originalEvent) {
          const element = document.elementFromPoint(evt.originalEvent.clientX, evt.originalEvent.clientY);
          if(element.dataset.cgid) {
            let edge = this.cy().edges().filter(( ele ) => ele.data('cgid') === element.dataset.cgid)
            setTimeout(() => {edge.select()}, 1)
          }
        }
      })

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

      function adjustSizes() {
        const padding = 2;
        this.cy.nodes().forEach((node) => {
          let el = document.querySelector(`#${this.instance_uid}-${node.data().html_id}`)
          if (el) {
            node.style('width', el.parentElement.clientWidth + padding)
            node.style('height', el.parentElement.clientHeight + padding)
          }
        })
        this.layout.run()
        this.cy.fit()
        this.$refs.cyContainer.style.visibility = 'visible'
      }


      // FIXME: do this on render complete instead of just pausing
      setTimeout(adjustSizes.bind(this), 100)
      // not sure if we need it at all
      // new ResizeObserver((function(){
      //   this.cy.fit()
      //   this.cy.center()
      //   }).bind(this)).observe(this.$refs.cyContainer)
    },
  },
};
</script>

<style scoped>

</style>