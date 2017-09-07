<template>
  <div class="ui container">
    <vuetable ref="vuetable"
              api-url="http://0.0.0.0:9999/tables"
              :fields="fields"
              data-path="data"
              pagination-path=""
    ></vuetable>
    <vuetable-pagination ref="pagination"></vuetable-pagination>
  </div>
</template>

<script>
  import Vuetable from 'vuetable-2/src/components/Vuetable';
  import VuetablePagination from 'vuetable-2/src/components/VuetablePagination';

  export default {
    components: {
      Vuetable,
      VuetablePagination,
    },
    data() {
      return {
        fields: [
          'name', 'size',
        ],
      };
    },
  };
</script>
