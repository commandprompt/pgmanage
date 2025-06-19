export default {
  methods: {
    humanizeDuration(value) {
        let fullMin = Math.floor(value / 60)
        let rem = value % 60
        if(fullMin && rem) {
          return `${fullMin}m${rem}s`
        } 
        if(fullMin) {
          return `${fullMin}m`
        }
        
        return `${value}s`
    }
  },
};
