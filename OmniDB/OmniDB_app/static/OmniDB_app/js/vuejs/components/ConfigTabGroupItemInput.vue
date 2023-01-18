<template>
  <div ref="settingInput">
    <div class="custom-control custom-switch" v-if="setting.vartype === 'bool'">
      <input class="custom-control-input" type="checkbox" :id="`switch-${inputId}`" v-model="setting.setting" true-value="on" false-value="off"
        @change="changeSetting" :disabled="isReadOnly" />
      <label class="custom-control-label" :for="`switch-${inputId}`"></label>
    </div>

    <select v-else-if="setting.vartype === 'enum'" class="form-control form-control-sm" :name="setting.name"
      v-model="setting.setting" @input="changeSetting" :disabled="isReadOnly">
      <option v-for="v in setting.enumvals" :value="v">{{ v }}</option>
    </select>
    <input v-else-if="setting.vartype === 'string'" data-html="true" type="text" :name="setting.name"
      :placeholder="setting.name" v-model="v$.setting.setting.$model" :id="inputId" :disabled="isReadOnly" :class="[
        'form-control',
        { 'is-invalid': v$.setting.setting.$invalid }
      ]" @input="changeSetting" />
    <input v-else data-html="true" :placeholder="setting.name" v-model="v$.setting.setting.$model" :id="inputId"
      :disabled="isReadOnly" @input="changeSetting" :class="[
        'form-control',
        { 'is-invalid': v$.setting.setting.$invalid }
      ]" />
    <div class="invalid-feedback">
      <a v-for="error of v$.setting.setting.$errors" :key="error.$uid">
        {{ error.$message }}
        <br />
      </a>
    </div>
  </div>
  <button v-if="
    setting.setting != setting.boot_val &&
    setting.category != 'Preset Options'
  " type="button" class="btn btn-link btn-sm" :id="buttonId" :title="`Reset to: ${setting.boot_val}`"
    ref="resetButton"
    @click.prevent="setDefault">
    <span class="fa fa-undo" aria-hidden="true"></span>
    Reset to default
  </button>
</template>

<script>
const { required, requiredUnless, minValue, maxValue, helpers } = window.VuelidateValidators

export default {
  setup() {
    return {
      v$: Vuelidate.useVuelidate({ $lazy: true })
    }
  },
  props: {
    initialSetting: {
      type: Object,
      required: true,
    },
    index: {
      type: Number,
      required: true,
    },
  },
  emits: ["settingChange"],
  data() {
    return {
      tooltipTitle: `<table class="text-left">
              <tr>
                <td>Type:</td>
                <td><b>${this.initialSetting.vartype}</b></td>
              </tr>
              <tr>
                <td>Unit:</td>
                <td><b>${this.initialSetting.unit}</b></td>
              </tr>
              <tr>
                <td>Minimum:</td>
                <td><b>${this.initialSetting.min_val}</b></td>
              </tr>
              <tr>
                <td>Maximum:</td>
                <td><b>${this.initialSetting.max_val}</b></td>
              </tr>
              </table>`,
      inputId: `${this.initialSetting.name}_${v_connTabControl.selectedTab.tag.tabControl.selectedTab.id}`,
      buttonId: `buttonResetDefault_${this.initialSetting.name}`,
    };
  },
  computed: {
    setting() {
      return Object.assign({}, this.initialSetting);
    },
    isReadOnly() {
      return this.initialSetting.category === "Preset Options";
    }
  },
  validations() {
    const localRules = {
      setting: {
        setting: { required: required }
      }
    }
    if (this.setting.vartype === 'string') {
      localRules.setting.setting.required = requiredUnless(() => !this.setting.boot_val)
    } else if (!!this.setting.unit) {
      localRules.setting.setting.customValidator = helpers.withMessage(
        `This field must be less than ${this.setting.max_val} or more than ${this.setting.min_val}`,
        this.validNumericSetting
      )
    } else {
      let do_not_check_names = ["unix_socket_permissions", "log_file_mode"]
      if (do_not_check_names.includes(this.setting.name)) {
        return localRules
      }
      localRules.setting.setting.minValue = minValue(this.setting.min_val)
      localRules.setting.setting.maxValue = maxValue(this.setting.max_val)
    }
    return localRules
  },
  mounted() {
    this.$nextTick(() => {
      $(`#${this.inputId}`).tooltip({
        container: this.$refs.settingInput,
        sanitize: false,
        title: this.tooltipTitle,
        boundary: "window",
        html: true,
        delay: { show: 500, hide: 100 },
      });
      $(`#${this.buttonId}`).tooltip({
        container: this.$refs.resetButton,
        sanitize: false,
        boundary: "window",
        html: true,
        delay: { show: 500, hide: 100 },
      });
    });
  },
  methods: {
    changeSetting() {
      this.$emit("settingChange", {
        changedSetting: this.setting,
        index: this.index,
      });
    },
    setDefault() {
      this.setting.setting = this.setting.boot_val;
      this.changeSetting();
    },
    validNumericSetting(value) {
      let current_val = this.formatNumber(value, this.setting.unit)
      if (isNaN(current_val))
        return false
      if (current_val < this.setting.min_val || current_val > this.setting.max_val)
        return false
      return true
    },
    formatNumber(value, unit = null) {
      const units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "YB", "ZB"]
      const unit_regex_size = /(?<value>[0-9.]+)\s*(?<unit>[KMGBTPEYZ]?B)$/iy
      let match_value_size = value.match(unit_regex_size)
      let factor = 1
      // if unit contains number, like '8kb'
      if (!!unit) {
        let match_unit = unit.match(unit_regex_size)
        if (!!match_unit) {
          factor = match_unit.groups.value
          unit = match_unit.groups.unit
        }
      }

      if (!!match_value_size) {
        let size_num = match_value_size.groups.value
        let size_unit = match_value_size.groups.unit
        let m = 0
        for (u of units) {
          if (!!unit && unit.toLowerCase() === u.toLowerCase()) {
            m = 0
          }
          if (u.toLowerCase() === size_unit.toLowerCase()) {
            return (+size_num * (1024 ** m)) / factor
          } else {
            m += 1
          }
        }
      }

      // Valid time units are ms (milliseconds), s (seconds), min (minutes),
      // h (hours), and d (days
      const unit_regex_time = /(?<value>[0-9.]+)\s*(?<unit>us|ms|s|min|h|d)$/iy
      let match_value_time = value.match(unit_regex_time)
      let mult;
      switch (unit) {
        case "ms":
          mult = { "us": 0.001, "ms": 1, "s": 1000, "min": 60000, "h": 3600000, "d": 86400000 }
          break
        case "s":
          mult = { "ms": -1000, "s": 1, "min": 60, "h": 3600, "d": 86400 }
          break
        case "min":
          mult = { "ms": -60000, "s": -60, "min": 1, "h": 60, "d": 1440 }
          break
        case "h":
          mult = { "ms": -3600000, "s": -3600, "min": -60, "h": 1, "d": 24 }
          break
        case "d":
          mult = { "ms": -86400000, "s": -86400, "min": -1440, "h": -24, "d": 1 }
          break
        default:
          mult = { "ms": 1, "s": 1, "min": 1, "h": 1, "d": 1 }
      }

      if (!!match_value_time) {
        let time_num = match_value_time.groups.value
        let time_unit = match_value_time.groups.unit
        if (mult[time_unit] > 0)
          return time_num * mult[time_unit]
        else
          return time_num / Math.abs(mult[time_unit])
      }

      return +value
    },
  },
};
</script>