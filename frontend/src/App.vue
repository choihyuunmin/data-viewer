<script>
export default {
  name: 'App'
}
</script>

<template>
  <div id="app">
    <iframe
      src="/src/components/DataViewer.html"
      frameborder="0"
      style="width: 100%; height: 100%; border: none;"
    ></iframe>
  </div>
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
