import config from '../config/webConfig'
import Chart from 'chart.js/auto'

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
                <div class="query-section">
                    <textarea
                        id="queryInput"
                        class="query-input"
                        placeholder="쿼리를 입력하세요"
                    ></textarea>
                    <div class="query-controls">
                        <button 
                            id="executeButton"
                            class="execute-button">
                            쿼리 실행
                        </button>
                        <div class="query-hint">Ctrl + Enter로 실행</div>
                    </div>
                </div>

                <div class="file-upload">
                    <input type="file" id="fileInput" accept=".parquet" />
                </div>
                
                <div id="totalRows" class="total-rows">
                    총 0개
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
                        1 / 1 페이지
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

        this.loading = true
        this.error = null

        try {
            const response = await fetch(`${config.api.baseUrl}/query`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    query: this.queryInput.value,
                    page: this.currentPage,
                    page_size: this.pageSize,
                    file_path: this.file_path
                })
            })

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
        this.updateTableBody()
        this.updateCharts()
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
                titleDiv.innerHTML += `<span class="sort-icon">${this.sortDirection === 'asc' ? '↑' : '↓'}</span>`
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
                td.textContent = row[column]
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
                const chart = this.charts[column]
                if (chart) {
                    cell.addEventListener('mouseover', () => {
                        const value = parseFloat(cell.dataset.value)
                        const data = this.distributions[column]
                        if (data && data.labels) {
                            const binIndex = this.getBinIndex(value, data.labels)
                            if (binIndex !== -1) {
                                const newColors = chart.data.datasets[0].data.map((_, idx) =>
                                    idx === binIndex ? 'rgba(255, 99, 132, 0.8)' : 'rgba(54, 162, 235, 0.5)'
                                )
                                chart.data.datasets[0].backgroundColor = newColors
                                chart.update('none')
                            }
                        }
                    })

                    cell.addEventListener('mouseout', () => {
                        if (chart.originalColors) {
                            chart.data.datasets[0].backgroundColor = chart.originalColors
                            chart.update('none')
                        }
                    })
                }
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
        // 기존 차트 제거
        this.destroyAllCharts()

        // 각 컬럼에 대해 차트 생성
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
        const chartDiv = document.createElement('div')
        chartDiv.className = 'column-chart'
        container.innerHTML = ''
        container.appendChild(chartDiv)

        const canvas = document.createElement('canvas')
        chartDiv.appendChild(canvas)

        await new Promise(resolve => requestAnimationFrame(resolve))

        const ctx = canvas.getContext('2d')
        if (!ctx) return

        try {
            if (data.type === 'numeric') {
                await this.createNumericChart(column, data, ctx)
            } else {
                this.createCategoricalDisplay(column, data, container)
            }
        } catch (error) {
            console.error(`Error creating chart for column ${column}:`, error)
        }
    }

    async createNumericChart(column, data, ctx) {
        const originalColors = data.counts.map(() => 'rgba(54, 162, 235, 0.5)')
        
        this.charts[column] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    data: data.counts,
                    backgroundColor: originalColors,
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 0,
                    barThickness: 15,
                    barPercentage: 0.6,
                    categoryPercentage: 1.0
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
                        callbacks: {
                            label: (context) => {
                                const value = context.raw
                                const total = data.counts.reduce((a, b) => a + b, 0)
                                const percent = ((value / total) * 100).toFixed(1)
                                return `${value.toLocaleString()}건 (${percent}%)`
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        display: true,
                        ticks: {
                            callback: function(value, index, ticks) {
                                if (data.labels.length === 1) {
                                    return data.labels[0]
                                }
                                if (index === 0) {
                                    return data.labels[0]
                                }
                                if (index === data.labels.length - 1) {
                                    return data.labels[data.labels.length - 1]
                                }
                                return null
                            },
                            align: function(context) {
                                if (data.labels.length === 1) {
                                    return 'center'
                                }
                                if (context.index === 0) {
                                    return 'start'
                                }
                                if (context.index === context.chart.data.labels.length - 1) {
                                    return 'end'
                                }
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

        // 차트 객체에 원본 색상 저장
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
        // 마지막 bin 체크
        if (value >= parseFloat(bins[bins.length - 1])) {
            return bins.length - 1
        }
        return -1
    }

    updatePagination() {
        this.prevButton.disabled = this.currentPage === 1
        this.nextButton.disabled = this.currentPage >= this.totalPages
        
        this.totalRowsElement.textContent = `${this.totalRows.toLocaleString()}건`
        this.pageInfo.textContent = `${this.currentPage} / ${this.totalPages}`
    }
}

export default DataViewer