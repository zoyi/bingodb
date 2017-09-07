/* eslint-disable prefer-rest-params,no-plusplus,no-unused-vars */
// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue';
import App from './App';
import router from './router';

Vue.config.productionTip = false;

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  template: '<App/>',
  components: { App },
  methods: {
    transform(data) {
      const transformed = {};
      transformed.data = [];

      /*
      for (let i = 0; i < data.length; i++) {
        transformed.data.push({
          name: 'hello',
          size: 10,
        });
      }
      */
      transformed.data.push({
        name: 'hello',
        size: 10,
      });

      transformed.data.push({
        name: 'hi',
        size: 10,
      });

      return transformed;
    },
  },
});
