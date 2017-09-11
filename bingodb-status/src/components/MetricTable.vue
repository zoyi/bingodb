<!--suppress HtmlUnknownAttribute -->
<template>
  <div class="ui container">
    <div class="vuetable-pagination ui basic segment grid">
      <vuetable-pagination-info ref="paginationInfoTop"
      ></vuetable-pagination-info>
      <!--suppress XmlUnboundNsPrefix -->
      <vuetable-pagination ref="paginationTop"
                           @vuetable-pagination:change-page="onChangePage"
      ></vuetable-pagination>
    </div>

    <!--suppress XmlUnboundNsPrefix -->
    <vuetable ref="vuetable"
              api-url="http://localhost:9999/tables"
              :fields="fields"
              data-path="data"
              pagination-path="pagination"
              detail-row-component="metric-details"
              track-by="name"
              @vuetable:cell-clicked="onCellClicked"
    >
      <template slot="actions" scope="props">
        <div class="metric-actions">
          <button class="ui basic button"
                  @click="onAction('view-item', props.rowData, props.rowIndex)">
            <i class="zoom icon"></i>
          </button>
          <button class="ui basic button"
                  @click="onAction('edit-item', props.rowData, props.rowIndex)">
            <i class="edit icon"></i>
          </button>
          <button class="ui basic button"
                  @click="onAction('delete-item', props.rowData, props.rowIndex)">
            <i class="delete icon"></i>
          </button>
        </div>
      </template>
    </vuetable>

    <div class="vuetable-pagination ui basic segment grid">
      <vuetable-pagination-info ref="paginationInfo"
      ></vuetable-pagination-info>
      <!--suppress XmlUnboundNsPrefix -->
      <vuetable-pagination ref="pagination"
                           @vuetable-pagination:change-page="onChangePage"
      ></vuetable-pagination>
    </div>
  </div>
</template>

<script>
  import Vue from 'vue';
  import Vuetable from 'vuetable-2/src/components/Vuetable';
  import VuetablePagination from 'vuetable-2/src/components/VuetablePagination';
  import VuetablePaginationInfo from 'vuetable-2/src/components/VuetablePaginationInfo';
  import DetailRow from './DetailRow';

  Vue.component('metric-details', DetailRow);

  export default {
    components: {
      Vuetable,
      VuetablePagination,
      VuetablePaginationInfo,
    },
    data() {
      return {
        fields: [
          {
            name: '__sequence',
            title: '',
            titleClass: 'center aligned',
            dataClass: 'right aligned',
          },
          {
            name: 'name',
            sortField: 'name',
            callback: 'allcap',
          },
          {
            name: 'size',
            sortField: 'size',
            dataClass: 'center aligned',
          },
          {
            name: '__slot:actions',
            title: 'Actions',
            titleClass: 'center aligned',
            dataClass: 'center aligned',
          },
        ],
        sortOrder: [
          {
            field: 'size',
            sortField: 'size',
            direction: 'desc',
          },
        ],
      };
    },
    methods: {
      transform(data) {
        const transformed = {};
        transformed.pagination = {
          total: data.length,
          per_page: data.length,
          current_page: 1,
          last_page: 1,
          next_page_url: null,
          prev_page_url: null,
          from: 1,
          to: data.length,
        };

        transformed.data = [];

        data.forEach((value) => {
          const newValue = {};
          // eslint-disable-next-line
          newValue.name = value.name;
          newValue.size = value.size;
          if (Object.prototype.hasOwnProperty.call(value, 'subIndices')) {
            newValue.subIndices = value.subIndices;
          }
          transformed.data.push(newValue);
        });

        return transformed;
      },
      onPaginationData(paginationData) {
        this.$refs.paginationTop.setPaginationData(paginationData);
        this.$refs.paginationInfoTop.setPaginationData(paginationData);
        this.$refs.pagination.setPaginationData(paginationData);
        this.$refs.paginationInfo.setPaginationData(paginationData);
      },
      onChangePage(page) {
        this.$refs.vuetable.changePage(page);
      },
      onCellClicked(data, field, event) {
        console.log('cellClicked: ', field.name, data.name, event);
        this.$refs.vuetable.toggleDetailRow(data.name);
      },
      onAction(action, data, index) {
        console.log(`slot action: ${action}`, data.name, index);
      },
      allcap(value) {
        return value.toUpperCase();
      },
    },
  };
</script>

<style>
  .metric-actions button.ui.button {
    padding: 8px 8px;
  }
  .metric-actions button.ui.button > i.icon {
    margin: auto !important;
  }
</style>
