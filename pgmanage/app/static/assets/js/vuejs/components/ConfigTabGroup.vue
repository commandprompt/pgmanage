<template>
  <div class="card mb-2">
    <div class="card-header font-weight-bold">
      {{ settingGroup.category }}
    </div>
    <div class="card-body p-0">
      <form role="form">
        <ul class="list-group list-group-flush form-group rounded-0">
          <li v-for="(setting, index) in settingGroup.rows" :key="setting.name" class="list-group-item d-flex pl-0">
            <div :class="[{ 'border-top-0': !index }, 'col-7']">
              <span class="font-weight-semibold">
                {{ setting.name }}
              </span>
              <p class="text-muted mb-0 small">
                {{ setting.desc }}
              </p>
            </div>
            <div :class="[{ 'border-top-0': !index }, 'col-5']">
              <div class="row no-gutters">
                <InputItem :initial-setting="setting" :index="index" @setting-change="changeSetting" />
              </div>
            </div>
          </li>
        </ul>
      </form>
    </div>
  </div>
</template>

<script>
import ConfigTabGroupItemInput from "./ConfigTabGroupItemInput.vue";

export default {
  name: "ConfigGroup",
  components: {
    InputItem: ConfigTabGroupItemInput,
  },
  props: {
    initialGroup: {
      type: Object,
      required: true,
    },
  },
  emits: ["groupChange"],
  computed: {
    settingGroup() {
      return Object.assign({}, this.initialGroup);
    },
  },
  methods: {
    changeSetting(event) {
      this.settingGroup.rows[event.index] = event.changedSetting;
      this.$emit("groupChange", {
        changedGroup: this.settingGroup,
        changedSetting: event.changedSetting,
      });
    },
  },
};
</script>
