<template>
  <div id="app">
    <img src="./assets/logo.png">
    <metric-table></metric-table>
  </div>
</template>

<script>
  import MetricTable from './components/MetricTable';

  export default {
    name: 'app',
    components: {
      MetricTable,
    },
  };
</script>

<style>
  #app {
    font-family: 'Avenir', Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-align: center;
    color: #2c3e50;
    margin-top: 60px;
  }
</style>
