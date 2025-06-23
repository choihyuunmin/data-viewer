<script setup>
import { onMounted } from 'vue'
import DataViewer from './components/DataViewer.js'
import './components/DataViewer.css'

onMounted(() => {
  new DataViewer('data-viewer-container')
})
</script>

<template>
  <div id="data-viewer-container"></div>
</template>

<style>
html, body {
  margin: 0;
  padding: 0;
  width: 100%;
  height: 100%;
  overflow: hidden;
}

#app {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  width: 100%;
  height: 100%;
  margin: 0;
  padding: 0;
  background-color: #f8f9fa;
}
</style>
