<template>
  <div class="detail-row" @click="onClick">
    <div class="inline field">
      <label>Name: </label>
      <span>{{rowData.name}}</span>
    </div>
    <div class="inline field">
      <label>Size: </label>
      <span>{{rowData.size}}</span>
    </div>
    <div class="inline field">
      <label>Sub Indices: </label>
      <span>{{rowData.subIndices}}</span>
    </div>
  </div>
</template>

<script>
  export default {
    props: {
      rowData: {
        type: Object,
        required: true,
      },
      rowIndex: {
        type: Number,
      },
    },
    methods: {
      onClick(event) {
        console.log('my-detail-row: on-click', event.target);
      },
    },
  };
</script>

<style>
  .detail-row {
    background-color: #eee;
  }
</style>
