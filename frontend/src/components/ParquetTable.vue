<template>
  <div class="parquet-table">
    <div class="query-section">
      <textarea
        :disabled="tableData.length === 0"
        v-model="query"
        class="query-input"
        placeholder="쿼리를 입력하세요"
        @keydown.enter.ctrl="executeQuery"
      ></textarea>
      <div class="query-controls">
        <button 
          class="execute-button" 
          @click="executeQuery"
          :disabled="tableData.length === 0">
          {{ loading ? '실행 중...' : '쿼리 실행' }}
        </button>
        <div class="query-hint">Ctrl + Enter로 실행</div>
      </div>
    </div>

    <div class="file-upload">
      <input type="file" @change="handleFileUpload" accept=".parquet" />
    </div>
    
    <div v-if="loading" class="loading">
      파일을 로딩중입니다...
    </div>
    
    <div class="total-rows">
      총 {{ totalRows.toLocaleString() }}개
    </div>
    <div v-if="tableData.length > 0" class="table-container">
      <table>
        <thead>
          <tr>
            <th v-for="column in columns" 
                :key="column" 
                class="column-header"
                :style="{ width: columnWidths[column] + 'px' }"
                @click="sortBy(column)"
                :class="{ 
                  'sortable': true, 
                  'sorted-asc': sortColumn === column && sortDirection === 'asc',
                  'sorted-desc': sortColumn === column && sortDirection === 'desc'
                }">
              <div class="column-title">
                {{ column }}
                <span v-if="sortColumn === column" class="sort-icon">
                  {{ sortDirection === 'asc' ? '↑' : '↓' }}
                </span>
              </div>
              <!-- 수치형 데이터의 경우 차트 표시 -->
              <div v-if="distributions[column] && distributions[column].type === 'numeric'" class="column-chart">
                <div class="mini-chart">
                  <div class="mini-histogram">
                    <div v-for="(count, index) in distributions[column].counts" 
                         :key="index" 
                         class="mini-histogram-bar"
                         :style="{ height: (count / Math.max(...distributions[column].counts) * 30) + 'px' }"
                         @mouseover="updateAxisLabel(column, index)"
                         @mouseleave="resetAxisLabel(column)">
                      <div v-if="currentAxisLabel[column] && currentAxisLabel[column].index === index" 
                           class="tooltip">
                        {{ currentAxisLabel[column].text }}
                      </div>
                    </div>
                  </div>
                </div>
                <div class="axis-labels">
                  <div class="numeric-axis">
                    <div class="x-axis-label min-value">{{ currentAxisLabel[column]?.text || distributions[column].labels[0] }}</div>
                    <div class="x-axis-label max-value">{{ distributions[column].labels[distributions[column].labels.length - 1] }}</div>
                  </div>
                </div>
              </div>
              <div v-else-if="distributions[column] && distributions[column].type === 'categorical'" class="categorical-labels">
                <div v-for="(count, index) in distributions[column].counts.slice(0, 3)" 
                     :key="index" 
                     class="top-value"
                     :data-tooltip="`${distributions[column].labels[index]}: ${count.toLocaleString()}건`">
                  {{ distributions[column].labels[index] }}: {{ Math.round((count / totalRows) * 100) }}%
                </div>
              </div>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in sortedData" :key="index">
            <td v-for="column in columns" 
                :key="column" 
                class="table-cell"
                :style="{ width: columnWidths[column] + 'px' }">
              {{ row[column] }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <!-- 페이징 컨트롤 -->
    <div class="pagination">
      <button 
        :disabled="currentPage === 1"
        @click="currentPage--"
        class="page-button"
      >
        이전
      </button>
      <span class="page-info">
        {{ currentPage.toLocaleString() }} / {{ totalPages.toLocaleString() }} 페이지
      </span>
      <button 
        :disabled="currentPage === totalPages" 
        @click="currentPage++"
        class="page-button"
      >
        다음
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import axios from 'axios'

const tableData = ref([])
const columns = ref([])
const loading = ref(false)
const error = ref(null)
const distributions = ref({})
const currentPage = ref(1)
const pageSize = 10
const totalRows = ref(0)
const sortColumn = ref(null)
const sortDirection = ref(null)
const currentAxisLabel = ref({})
const query = ref('SELECT * FROM data')
const queryError = ref(null)
const sessionId = ref(null)
const paginatedData = ref([])

const totalPages = computed(() => Math.ceil(totalRows.value / pageSize))

// 정렬된 데이터 계산
const sortedData = computed(() => {
  if (!tableData.value) return []
  
  let sorted = [...tableData.value]
  
  if (sortColumn.value && sortDirection.value) {
    sorted.sort((a, b) => {
      let aVal = a[sortColumn.value]
      let bVal = b[sortColumn.value]
      
      // 숫자형 데이터 처리
      if (!isNaN(aVal) && !isNaN(bVal)) {
        aVal = Number(aVal)
        bVal = Number(bVal)
      }
      
      // null 값 처리
      if (aVal === null) return sortDirection.value === 'asc' ? -1 : 1
      if (bVal === null) return sortDirection.value === 'asc' ? 1 : -1
      
      // 문자열 비교
      if (aVal < bVal) return sortDirection.value === 'asc' ? -1 : 1
      if (aVal > bVal) return sortDirection.value === 'asc' ? 1 : -1
      return 0
    })
  }
  
  return sorted
})

// 페이지 변경 감시
watch(currentPage, async () => {
  if (tableData.value.length > 0) {
    await fetchPageData()
  }
})

// 페이지 데이터 가져오기
const fetchPageData = async () => {
  try {
    loading.value = true
    const response = await axios.post('http://localhost:8000/page', {
      query: query.value,
      session_id: sessionId.value,
      page: currentPage.value,
      page_size: pageSize
    })

    if (response.data.columns && response.data.data) {
      columns.value = response.data.columns
      tableData.value = response.data.data
    }
  } catch (err) {
    error.value = '페이지 데이터 로딩 중 오류가 발생했습니다: ' + err.message
  } finally {
    loading.value = false
  }
}

// 컬럼명에 특수문자가 포함된 경우 큰따옴표로 감싸기
const wrapColumnName = (column) => {
  if (column.includes('%') || column.includes(' ') || column.includes('-')) {
    return `"${column}"`
  }
  return column
}

// 쿼리 실행 함수
const executeQuery = async () => {
  try {
    loading.value = true
    queryError.value = null

    // 쿼리에서 컬럼명을 큰따옴표로 감싸기
    let processedQuery = query.value
    columns.value.forEach(col => {
      const wrappedCol = wrapColumnName(col)
      if (wrappedCol !== col) {
        processedQuery = processedQuery.replace(new RegExp(col, 'g'), wrappedCol)
      }
    })

    const response = await axios.post('http://localhost:8000/query', {
      query: processedQuery,
      session_id: sessionId.value,
      page: currentPage.value,
      page_size: pageSize
    })

    if (response.data.columns && response.data.data) {
      columns.value = response.data.columns
      tableData.value = response.data.data
      distributions.value = response.data.distributions
      totalRows.value = response.data.total
    }
  } catch (err) {
    queryError.value = err.response?.data?.detail || '쿼리 실행 중 오류가 발생했습니다.'
  } finally {
    loading.value = false
  }
}

const handleFileUpload = async (event) => {
  const file = event.target.files[0]
  if (!file) return

  loading.value = true
  error.value = null
  currentPage.value = 1
  sortColumn.value = null
  sortDirection.value = null

  try {
    const formData = new FormData()
    formData.append('file', file)

    const response = await axios.post('http://localhost:8000/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data'
      }
    })

    if (response.data.columns && response.data.preview) {
      sessionId.value = response.data.session_id
      columns.value = response.data.columns
      tableData.value = response.data.preview
      distributions.value = response.data.distributions
      totalRows.value = response.data.total
      paginatedData.value = tableData.value.slice(0, pageSize)
    }
  } catch (err) {
    error.value = '파일 처리 중 오류가 발생했습니다: ' + err.message
  } finally {
    loading.value = false
  }
}

const sortBy = (column) => {
  if (sortColumn.value === column) {
    // 같은 컬럼을 다시 클릭한 경우
    if (sortDirection.value === 'asc') {
      sortDirection.value = 'desc'
    } else if (sortDirection.value === 'desc') {
      sortColumn.value = null
      sortDirection.value = null
    }
  } else {
    // 다른 컬럼을 클릭한 경우
    sortColumn.value = column
    sortDirection.value = 'asc'
  }
}

const calculateColumnWidth = (column) => {
  const headerWidth = column.length * 10; // 헤더 텍스트 길이에 따른 기본 너비
  const maxDataWidth = Math.max(
    ...tableData.value.map(row => {
      const value = row[column];
      return value ? String(value).length * 8 : 0; // 데이터 텍스트 길이에 따른 너비
    })
  );
  return Math.max(160, Math.max(headerWidth, maxDataWidth)); // 최소 160px 보장
}

// 컬럼 너비 계산 및 적용
const columnWidths = computed(() => {
  const widths = {};
  columns.value.forEach(column => {
    widths[column] = calculateColumnWidth(column);
  });
  return widths;
});

const updateAxisLabel = (column, index) => {
  const distribution = distributions[column];
  const label = distribution.labels[index];
  const count = distribution.counts[index];
  currentAxisLabel.value[column] = {
    index,
    text: `${label}: ${count.toLocaleString()}건`
  };
}

const resetAxisLabel = (column) => {
  currentAxisLabel.value[column] = null;
}
</script>

<style scoped>
.parquet-table {
  width: 100%;
  background-color: white;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  padding: 20px;
}

.file-upload {
  margin-bottom: 20px;
}

.file-upload input[type="file"] {
  padding: 10px;
  border: 2px dashed #3498db;
  border-radius: 6px;
  width: 100%;
  cursor: pointer;
  transition: all 0.3s ease;
}

.file-upload input[type="file"]:hover {
  border-color: #2980b9;
  background-color: #f7f9fc;
}

.table-container {
  overflow-x: auto;
  border-radius: 8px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  position: relative;
  max-height: calc(100vh - 200px);
  overflow-y: auto;
  scroll-behavior: smooth;
}

/* 테이블 스타일 */
table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
  border: 1px solid #e0e0e0;
}

thead {
  position: sticky;
  top: 0;
  z-index: 10;
  background-color: #f8f9fa;
}

th {
  background-color: #f8f9fa;
  font-weight: 700;
  color: #2c3e50;
  position: relative;
  padding-bottom: 70px;
  text-align: center;
  white-space: nowrap;
  cursor: pointer;
  transition: background-color 0.2s ease;
}

th::after {
  content: '';
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
  height: 1px;
  background-color: #e0e0e0;
  z-index: 11;
}

.column-header {
  position: relative;
  background-color: #f8f9fa;
  font-weight: 700;
  padding-bottom: 100px;
  border-bottom: 2px solid #e0e0e0;
  border-right: 1px solid #e0e0e0;
  text-align: center;
  white-space: nowrap;
  cursor: pointer;
  transition: all 0.2s ease;
  min-width: 160px;
  padding-left: 12px;
  padding-right: 12px;
  width: auto;
}

.column-header:last-child {
  border-right: none;
}

.column-header:hover {
  background-color: #e9ecef;
}

.column-header.sortable {
  position: relative;
}

.column-header.sorted-asc {
  background-color: #e3f2fd;
}

.column-header.sorted-desc {
  background-color: #e3f2fd;
}

.sort-icon {
  margin-left: 4px;
  font-size: 0.9em;
  color: #3498db;
}

.table-cell {
  padding: 8px 12px;
  border-bottom: 1px solid #e0e0e0;
  border-right: 1px solid #e0e0e0;
  text-align: left;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 160px;
  min-width: 160px;
  width: auto;
}

.table-cell:last-child {
  border-right: none;
}

td {
  color: #34495e;
  text-align: left;
  padding: 12px 16px;
}

tr {
  transition: all 0.2s ease;
}

tr:hover {
  background-color: #f5f9ff;
}

tr:hover td {
  border-color: #b3d4fc;
}

.column-title {
  margin-bottom: 15px;
  color: #2c3e50;
  font-size: 1.1em;
  font-weight: 700;
  text-align: center;
  padding: 0 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 4px;
}

.column-chart {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  height: 90px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  background-color: #f8f9fa;
  width: 100%;
}

.mini-chart {
  height: 30px;
  width: 100%;
  display: flex;
  align-items: flex-end;
  justify-content: center;
  gap: 1px;
  margin-bottom: 4px;
}

.mini-histogram {
  display: flex;
  align-items: flex-end;
  height: 100%;
  width: 100%;
  gap: 1px;
  padding: 0 4px;
}

.mini-histogram-bar {
  flex: 1;
  background-color: #3498db;
  min-width: 2px;
  border-radius: 1px 1px 0 0;
  transition: background-color 0.2s ease;
  cursor: pointer;
  position: relative;
}

.mini-histogram-bar:hover {
  background-color: #2980b9;
}

.axis-labels {
  height: 15px;
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  font-size: 11px;
  color: #666;
  margin-top: 2px;
}

.numeric-axis {
  display: flex;
  justify-content: space-between;
  width: 100%;
  padding: 0 4px;
}

.x-axis-label {
  font-size: 11px;
  color: #666;
  transition: all 0.2s ease;
}

.categorical-labels {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 4px;
  padding: 0;
  margin: 10px;
  height: 90px;
  justify-content: center;
  background-color: #f8f9fa;
  width: calc(100% - 20px);
}

.top-value {
  font-size: 13px;
  color: #2c3e50;
  white-space: nowrap;
  font-weight: 600;
  background-color: rgba(255, 255, 255, 0.9);
  padding: 4px 12px;
  border-radius: 4px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  cursor: help;
  transition: all 0.2s ease;
  position: relative;
  min-width: 120px;
  text-align: center;
}

.top-value:hover {
  background-color: #e3f2fd;
  transform: scale(1.05);
}

.top-value:hover::after {
  content: attr(data-tooltip);
  position: absolute;
  bottom: 100%;
  left: 50%;
  transform: translateX(-50%);
  background-color: rgba(0, 0, 0, 0.8);
  color: white;
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 12px;
  white-space: nowrap;
  z-index: 1000;
  margin-bottom: 5px;
}

/* 페이징 스타일 */
.pagination {
  display: flex;
  justify-content: center;
  align-items: center;
  margin-top: 20px;
  gap: 15px;
  padding: 15px 0;
}

.page-button {
  padding: 8px 16px;
  border: none;
  background-color: #3498db;
  color: white;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s ease;
  font-weight: 500;
}

.page-button:hover:not(:disabled) {
  background-color: #2980b9;
  transform: translateY(-1px);
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.page-button:disabled {
  background-color: #bdc3c7;
  cursor: not-allowed;
  transform: none;
  box-shadow: none;
}

.page-info {
  color: #666;
  font-size: 0.9em;
}

.loading, .error {
  text-align: center;
  padding: 20px;
  color: #666;
  background-color: #f8f9fa;
  border-radius: 6px;
  margin: 20px 0;
}

.error {
  color: #e74c3c;
  background-color: #fdf3f2;
  border: 1px solid #fadbd8;
}

.tooltip {
  position: absolute;
  bottom: 100%;
  left: 50%;
  transform: translateX(-50%);
  background-color: rgba(0, 0, 0, 0.8);
  color: white;
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 12px;
  white-space: nowrap;
  z-index: 1000;
  margin-bottom: 5px;
  pointer-events: none;
  opacity: 1;
  visibility: visible;
}

.tooltip::after {
  content: '';
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  border-width: 5px;
  border-style: solid;
  border-color: rgba(0, 0, 0, 0.8) transparent transparent transparent;
}

.query-section {
  margin-bottom: 20px;
  background: white;
  border-radius: 8px;
  padding: 15px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.query-input {
  width: 100%;
  min-height: 100px;
  padding: 12px;
  border: 1px solid #e0e0e0;
  border-radius: 4px;
  font-family: monospace;
  font-size: 14px;
  line-height: 1.5;
  resize: vertical;
  margin-bottom: 10px;
}

.query-input:focus {
  outline: none;
  border-color: #2196F3;
  box-shadow: 0 0 0 2px rgba(33, 150, 243, 0.1);
}

.query-controls {
  display: flex;
  align-items: center;
  gap: 10px;
}

.execute-button {
  padding: 8px 16px;
  background-color: #2196F3;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-weight: 600;
  transition: background-color 0.2s ease;
}

.execute-button:hover {
  background-color: #1976D2;
}

.execute-button:disabled {
  background-color: #BDBDBD;
  cursor: not-allowed;
}

.query-hint {
  color: #666;
  font-size: 12px;
}

.query-error {
  margin-top: 10px;
  padding: 10px;
  background-color: #ffebee;
  color: #c62828;
  border-radius: 4px;
  font-size: 14px;
}
</style> 