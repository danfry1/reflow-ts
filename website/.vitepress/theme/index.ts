import { defineAsyncComponent } from 'vue'
import DefaultTheme from 'vitepress/theme'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    // Loaded only where it is used, the home page: it bundles the engine and
    // a schema library that no other page needs.
    app.component('Playground', defineAsyncComponent(() => import('./components/Playground.vue')))
  },
}
