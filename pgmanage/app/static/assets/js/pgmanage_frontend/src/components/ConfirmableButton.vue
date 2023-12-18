<template>
  <button v-if="!clicked" type="button" @click="defaultClick">
    <slot>
      Delete
    </slot>
  </button>
  <button v-else type="button" @click="confirmClick">{{ confirmText }}</button>
</template>

<script>

export default {
  props: {
    callbackFunc: {
      type: Function,
      required: true
    },
    confirmText: {
      type: String,
      default: "Confirm Delete?"
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