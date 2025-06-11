<script setup>
import ParquetTable from './components/ParquetTable.vue'
</script>

<template>
  <div class="app">
    <h1>데이터셋 뷰어</h1>
    <ParquetTable />
  </div>
</template>

<style>
.app {
  max-width: 1200px;
  margin: 0 auto;
  padding: 20px;
}

h1 {
  color: #2c3e50;
  text-align: center;
  margin-bottom: 30px;
}
</style>
