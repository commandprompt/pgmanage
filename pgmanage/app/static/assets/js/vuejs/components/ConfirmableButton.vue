<template>
  <button v-if="!clicked" type="button" @click="defaultClick">{{ defaulttext }}</button>
  <button v-else type="button" @click="confirmClick">{{ confirmText }}</button>
</template>

<script>

export default {
  props: {
    callbackFunc: {
      type: Function,
      required: true
    },
    defaulttext: {
      type: String,
      default: "Delete"
    },
    confirmText: {
      type: String,
      default: "Confirm Delete"
    }
  },
  data() {
    return {
      clicked: false
    }
  },
  methods: {
    defaultClick() {
      this.clicked = true
      setTimeout(() => {
        this.clicked = false
      }, 3000)
    },
    confirmClick() {
      this.callbackFunc()
    }
  }
}

</script>