import config from '../config/webConfig'
import Chart from 'chart.js/auto'

const getCssVar = (varName) => {
    return getComputedStyle(document.documentElement).getPropertyValue(varName).trim()
}

const colorPonit = getCssVar('--color-point-bar');
const colorPointHover = getCssVar('--color-hover-point');

class DataViewer {
    constructor(containerId) {
        this.containerId = containerId
        this.container = document.getElementById(containerId)
        if (!this.container) {
            console.error(`Container with id ${containerId} not found`)
            return
        }
        
        this.query = 'select * from data'
        this.columns = []
        this.distributions = []
        this.tableData = []
        this.file_path = null
        this.charts = {}
        this.pageSize = 10
        this.currentPage = 1
        this.totalRows = 0
        this.totalPages = 0

        this.initializeElements()
        this.attachEventListeners()
    }

    initializeElements() {
        this.container.innerHTML = `
            <div class="parquet-table">          
                <h3>Dataset Viewer</h3>
                <div class="file-upload-area" id="fileDropZone">
                    <p class="file-placeholder">파일을 이곳에 드롭하거나 클릭하여 파일 선택</p>
                    <input type="file" id="fileInput" accept=".parquet" hidden />
                    <p id="selectedFileName" class="file-name-text">선택된 파일 없음</p>
                </div>

                <div class="query-section">
                    <h5>쿼리를 입력하여 데이터를 확인해보세요. <span>(Ctrl + Enter로 실행가능)</span></h5>
                    <div class="query-section-inner">
                        <textarea
                            id="queryInput"
                            class="query-input"
                            placeholder="쿼리를 입력하세요"
                        ></textarea>
                        <button 
                            id="executeButton"
                            class="execute-button">
                            실행
                        </button>
                        <div class="query-section-inner-info">
                            <p>from 절은 data 테이블로 고정되어 있습니다.</p>
                        </div>
                    </div>
                </div>
                
                <div id="totalRows" class="total-rows">
                    총 0건
                </div>
                <div id="tableContainer" class="table-container">
                    <table>
                        <thead>
                            <tr id="tableHeader"></tr>
                        </thead>
                        <tbody id="tableBody"></tbody>
                    </table>
                </div>
                <div class="pagination">
                    <button 
                        id="prevButton"
                        class="page-button">
                        이전
                    </button>
                    <span id="pageInfo" class="page-info">
                        1 / 1
                    </span>
                    <button 
                        id="nextButton"
                        class="page-button">
                        다음
                    </button>
                </div>
            </div>
        `

        this.queryInput = this.container.querySelector('#queryInput')
        this.executeButton = this.container.querySelector('#executeButton')
        this.fileInput = this.container.querySelector('#fileInput')
        this.totalRowsElement = this.container.querySelector('#totalRows')
        this.tableHeader = this.container.querySelector('#tableHeader')
        this.tableBody = this.container.querySelector('#tableBody')
        this.prevButton = this.container.querySelector('#prevButton')
        this.nextButton = this.container.querySelector('#nextButton')
        this.pageInfo = this.container.querySelector('#pageInfo')

        this.queryInput.value = this.query

        const fileDropZone = this.container.querySelector('#fileDropZone')
        const fileNameText = this.container.querySelector('#selectedFileName')

        fileDropZone.addEventListener('click', () => this.fileInput.click())

        this.fileInput.addEventListener('change', (e) => {
            const file = e.target.files[0]
            fileNameText.textContent = file ? `파일명 ${file.name}` : '선택된 파일 없음'
            this.handleFileUpload(e)
        })

        fileDropZone.addEventListener('dragover', (e) => {
            e.preventDefault()
            fileDropZone.classList.add('dragover')
        })

        fileDropZone.addEventListener('dragleave', () => {
            fileDropZone.classList.remove('dragover')
        })

        fileDropZone.addEventListener('drop', (e) => {
            e.preventDefault()
            fileDropZone.classList.remove('dragover')
            const file = e.dataTransfer.files[0]
            if (file) {
                this.fileInput.files = e.dataTransfer.files
                fileNameText.textContent = `파일명 ${file.name}`
                this.handleFileUpload({ target: { files: [file] } })
            }
        })
    }
    

    attachEventListeners() {
        this.queryInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.currentPage = 1
                this.executeQuery()
            }
        })

        this.executeButton.addEventListener('click', () => this.executeQuery())
        this.fileInput.addEventListener('change', (e) => this.handleFileUpload(e))

        this.prevButton.addEventListener('click', () => {
            if (this.currentPage > 1) {
                this.currentPage--
                this.changePage(this.currentPage)
            }
        })
        this.nextButton.addEventListener('click', () => {
            if (this.currentPage < this.totalPages) {
                this.currentPage++
                this.changePage(this.currentPage)
            }
        })
    }

    async executeQuery() {
        if (!this.file_path) {
            alert('파일을 먼저 업로드해주세요.')
            return
        }

        // 쿼리 유효성 검증
        const query = this.queryInput.value.trim()
        if (!query) {
            alert('쿼리를 입력해주세요.')
            return
        }

        if (query.length > 1000) {
            alert('쿼리가 너무 깁니다. 1000자 이내로 입력해주세요.')
            return
        }

        this.loading = true
        this.error = null

        try {
            const response = await fetch(`${config.api.baseUrl}/query`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    query: query,
                    page: this.currentPage,
                    page_size: this.pageSize,
                    file_path: this.file_path
                })
            })

            if (!response.ok) {
                const errorData = await response.json()
                throw new Error(errorData.detail || '쿼리 실행 중 오류가 발생했습니다.')
            }

            const data = await response.json()
            console.log('Query Response:', data)
            
            if (data.tableData) {
                this.tableData = data.tableData
                this.totalRows = data.total
                this.columns = data.columns
                this.distributions = data.distributions
                this.totalPages = Math.ceil(this.totalRows / this.pageSize)
                
                console.log('Updated State:', {
                    totalRows: this.totalRows,
                    totalPages: this.totalPages,
                    currentPage: this.currentPage,
                    pageSize: this.pageSize
                })
                
                this.updateTable()
                this.updatePagination()
            }
        } catch (error) {
            console.error('Query execution error:', error)
            this.error = error.message
            alert(this.error)
        } finally {
            this.loading = false
        }
    }

    async changePage(page) {
        if (!this.file_path) {
            alert('파일을 먼저 업로드해주세요.')
            return
        }

        this.loading = true
        this.error = null

        try {
            const response = await fetch(`${config.api.baseUrl}/page`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    query: this.queryInput.value,
                    page: page,
                    page_size: this.pageSize,
                    file_path: this.file_path
                })
            })

            const data = await response.json()
            
            this.tableData = data.tableData
            this.updateTableBody()
            this.updatePagination()
        } catch (error) {
            console.error('Page change error:', error)
            this.error = error.message
            alert(this.error)
        } finally {
            this.loading = false
        }
    }

    async handleFileUpload(event) {
        const file = event.target.files[0]
        if (!file) return

        this.loading = true
        this.error = null
        this.currentPage = 1
        this.sortColumn = null
        this.sortDirection = null

        try {
            const formData = new FormData()
            formData.append('file', file)

            const response = await fetch(`${config.api.baseUrl}/init`, {
                method: 'POST',
                body: formData
            })

            if (!response.ok) {
                throw new Error('파일 업로드 중 오류가 발생했습니다.')
            }

            const data = await response.json()
            if (data.columns && data.tableData) {
                this.columns = data.columns
                this.tableData = data.tableData
                this.distributions = data.distributions
                this.totalRows = data.total
                this.totalPages = Math.ceil(this.totalRows / this.pageSize)
                this.file_path = data.file_path
                this.updateTable()
                this.updatePagination()
            }
        } catch (err) {
            this.error = err.message
            console.error(this.error)
        } finally {
            this.loading = false
        }
    }

    async updateTable() {
        await this.updateTableHeader()
        await this.updateCharts()
        this.updateTableBody()
    }

    async updateTableHeader() {
        this.tableHeader.innerHTML = ''
        this.columns.forEach(column => {
            const th = document.createElement('th')
            th.className = 'column-header'
            th.onclick = () => this.sortBy(column)

            const titleDiv = document.createElement('div')
            titleDiv.className = 'column-title'
            titleDiv.textContent = column

            if (this.sortColumn === column) {
                titleDiv.innerHTML += `<span class="sort-icon ${this.sortDirection === 'asc' ? 'sorted-asc' : 'sorted-desc'}">${this.sortDirection === 'asc' ? '↑' : '↓'}</span>`
            }

            th.appendChild(titleDiv)

            const distributionContainer = document.createElement('div')
            distributionContainer.className = 'distribution-container'
            distributionContainer.id = `distribution-${this.columns.indexOf(column)}`
            th.appendChild(distributionContainer)

            this.tableHeader.appendChild(th)
        })
    }

    updateTableBody() {
        this.tableBody.innerHTML = ''
        const sortedData = this.getSortedData()
        
        sortedData.forEach(row => {
            const tr = document.createElement('tr')
            this.columns.forEach(column => {
                const td = document.createElement('td')
                td.className = 'table-cell'
                let cellValue = row[column]

                const isYearOrDateLike = (col) =>
                col.includes('년도') || col.includes('년월') || col.includes('년') || col.includes('월') || col.toLowerCase().includes('year') || col.toLowerCase().includes('date')

                // 숫자면 천단위 쉼표 포맷
                if (
                this.isNumericColumn(column) && !isNaN(cellValue) && !isYearOrDateLike(column)
                ) {
                td.textContent = Number(cellValue).toLocaleString()
                td.classList.add('numeric-cell')
                } else {
                // XSS 방지를 위해 텍스트 이스케이프
                td.textContent = this.escapeHtml(String(cellValue))
                }

                if (this.isNumericColumn(column)) {
                    td.dataset.value = row[column]
                    td.dataset.column = column
                }
                tr.appendChild(td)
            })
            this.tableBody.appendChild(tr)
        })

        // 테이블 셀에 이벤트 리스너 추가
        this.tableBody.querySelectorAll('.table-cell').forEach(cell => {
            if (cell.dataset.value && cell.dataset.column) {
                const column = cell.dataset.column

                cell.addEventListener('mouseover', () => {
                    const value = parseFloat(cell.dataset.value)
                    const data = this.distributions[column]
                    const chart = this.charts[column]

                    if (!chart?.data?.datasets?.[0] || !data?.labels) return

                    const binIndex = this.getBinIndex(value, data.labels)
                    if (binIndex !== -1) {
                        const newColors = chart.data.datasets[0].data.map((_, idx) =>
                            idx === binIndex ? colorPointHover : colorPonit
                        )
                        chart.data.datasets[0].backgroundColor = newColors
                        try {
                            chart.update('none')
                        } catch (e) {
                            console.warn('Chart update 실패:', e)
                        }
                    }
                })

                cell.addEventListener('mouseout', () => {
                    const chart = this.charts[column]
                    if (!chart?.originalColors) return
                    try {
                        chart.data.datasets[0].backgroundColor = chart.originalColors
                        chart.update('none')
                    } catch (e) {
                        console.warn('Chart reset 실패:', e)
                    }
                })
            }
        })
    }

    getSortedData() {
        if (!this.sortColumn || !this.sortDirection) return this.tableData

        return [...this.tableData].sort((a, b) => {
            let aVal = a[this.sortColumn]
            let bVal = b[this.sortColumn]

            if (!isNaN(aVal) && !isNaN(bVal)) {
                aVal = Number(aVal)
                bVal = Number(bVal)
            }

            if (aVal === null) return this.sortDirection === 'asc' ? -1 : 1
            if (bVal === null) return this.sortDirection === 'asc' ? 1 : -1

            if (aVal < bVal) return this.sortDirection === 'asc' ? -1 : 1
            if (aVal > bVal) return this.sortDirection === 'asc' ? 1 : -1
            return 0
        })
    }

    sortBy(column) {
        if (this.sortColumn === column) {
            if (this.sortDirection === 'asc') {
                this.sortDirection = 'desc'
            } else if (this.sortDirection === 'desc') {
                this.sortColumn = null
                this.sortDirection = null
            }
        } else {
            this.sortColumn = column
            this.sortDirection = 'asc'
        }
        this.updateTable()
    }

    async updateCharts() {
        this.destroyAllCharts()

        console.log(this.distributions)
        for (const column of this.columns) {
            const data = this.distributions[column]
            if (!this.isValidDistributionData(data)) continue

            const distributionContainer = this.getDistributionContainer(column)
            if (!distributionContainer) continue

            await this.createChartForColumn(column, data, distributionContainer)
        }
    }

    destroyAllCharts() {
        Object.values(this.charts).forEach(chart => {
            if (chart) {
                chart.destroy()
            }
        })
        this.charts = {}
    }

    isValidDistributionData(data) {
        return data && data.labels && data.labels.length > 0
    }

    getDistributionContainer(column) {
        return document.querySelector(`#distribution-${this.columns.indexOf(column)}`)
    }

async createChartForColumn(column, data, container) {
    container.innerHTML = '' 

    if (data.type === 'numeric') {
        const chartDiv = document.createElement('div')
        chartDiv.className = 'column-chart'
        container.appendChild(chartDiv)

        const canvas = document.createElement('canvas')
        canvas.width = 400
        canvas.height = 200
        canvas.style.width = '180px'
        canvas.style.height = '80px'
        chartDiv.appendChild(canvas)

        await new Promise(resolve => {
            requestAnimationFrame(() => {
                requestAnimationFrame(resolve)
            })
        })

        const ctx = canvas.getContext('2d')
        if (!ctx) return

        try {
            await this.createNumericChart(column, data, ctx)
        } catch (error) {
            console.error(`Error creating chart for column ${column}:`, error)
        }
    } else {
        this.createCategoricalDisplay(column, data, container)
    }
}


    async createNumericChart(column, data, ctx) {
        const originalColors = data.counts.map(() => colorPonit)
        
        this.charts[column] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    data: data.counts,
                    backgroundColor: originalColors,
                    borderWidth: 0,
                    hoverBackgroundColor: colorPointHover, 
                    categoryPercentage: 1.0,
                    barThickness: 'flex',
                    barPercentage: 1.0,
                    categoryPercentage: 0.8,    
                    borderRadius: 2,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        enabled: true,
                        mode: 'index',
                        intersect: false,
                        position: 'nearest',
                        callbacks: {
                            label: (context) => {
                                const value = context.raw
                                const total = data.counts.reduce((a, b) => a + b, 0)
                                const percent = ((value / total) * 100).toFixed(1)
                                return `${value.toLocaleString()}건 (${percent}%)`
                            }
                        },
                    }
                },
                scales: {
                    x: {
                        display: true,
                        ticks: {
                            padding : 0,
                            callback: function(value, index, ticks) {
                                const formatKMB = (val) => {
                                    const num = Number(val)
                                    if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M'
                                    if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K'
                                    return num.toString() 
                                }

                                if (data.labels.length === 1) {
                                    return formatKMB(data.labels[0])
                                }

                                if (index === 0) {
                                    return formatKMB(data.labels[0])
                                }

                                if (index === data.labels.length - 1) {
                                    return formatKMB(data.labels[data.labels.length - 1])
                                }

                                return null
                            },
                            align: function(context) {
                                if (data.labels.length === 1) return 'center'
                                if (context.index === 0) return 'start'
                                if (context.index === context.chart.data.labels.length - 1) return 'end'
                                return 'center'
                            }
                        },
                        grid: {
                            display: false
                        }
                    },
                    y: {
                        display: false,
                        grid: {
                            display: false
                        }
                    }
                }
            }
        })

        this.charts[column].originalColors = originalColors
    }

    createCategoricalDisplay(column, data, container) {
        const labelsDiv = document.createElement('div')
        labelsDiv.className = 'categorical-labels'
        
        const topValues = this.getTopValues(data, 3)
        const total = data.counts.reduce((a, b) => a + b, 0)

        topValues.forEach(({ label, count }) => {
            const valueDiv = document.createElement('div')
            valueDiv.className = 'top-value'
            const percent = ((count / total) * 100).toFixed(1)
            valueDiv.textContent = `${label}: ${percent}%`
            valueDiv.setAttribute('data-count', `${label}: ${count.toLocaleString()}건`)
            labelsDiv.appendChild(valueDiv)
        })

        container.appendChild(labelsDiv)
    }

    getTopValues(data, limit) {
        return data.labels
            .map((label, index) => ({ label, count: data.counts[index] }))
            .sort((a, b) => b.count - a.count)
            .slice(0, limit)
    }

    isNumericColumn(column) {
        return this.distributions[column]?.type === 'numeric'
    }

    getBinIndex(value, bins) {
        for (let i = 0; i < bins.length - 1; i++) {
            const currentBin = parseFloat(bins[i])
            const nextBin = parseFloat(bins[i + 1])
            if (value >= currentBin && value < nextBin) {
                return i
            }
        }
        if (value >= parseFloat(bins[bins.length - 1])) {
            return bins.length - 1
        }
        return -1
    }

    updatePagination() {
        this.prevButton.disabled = this.currentPage === 1
        this.nextButton.disabled = this.currentPage >= this.totalPages
        
        this.totalRowsElement.textContent = `총 ${this.totalRows.toLocaleString()}건`
        this.pageInfo.textContent = `${this.currentPage} / ${this.totalPages}`
    }

    // XSS 방지를 위한 텍스트 이스케이프 함수
    escapeHtml(text) {
        const div = document.createElement('div')
        div.textContent = text
        return div.innerHTML
    }
}

export default DataViewer