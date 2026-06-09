import DefaultTheme from 'vitepress/theme'
import CrashTest from './components/CrashTest.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('CrashTest', CrashTest)
  },
}
