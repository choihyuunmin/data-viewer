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

        this.query = ''
        this.file_path = null
        this.charts = {}

        this.initializeElements()
        this.attachEventListeners()
    }

    getOrCreateTooltip(chart) {
        let tooltipEl = chart.canvas.parentNode.querySelector('div')

        if (!tooltipEl) {
            tooltipEl.style.opacity = 1;
            tooltipEl.style.left = positionX + tooltip.caretX + 'px';
            tooltipEl.style.top = positionY + tooltip.caretY + 'px';
            tooltipEl.style.font = tooltip.options.bodyFont.string;
            tooltipEl.style.padding = tooltip.options.padding + 'px ' + tooltip.options.padding + 'px';
          
            const table = document.createElement('table')
            table.style.margin = '0px'
            table.style.borderSpacing = '0'
            table.style.borderCollapse = 'collapse'

            tooltipEl.appendChild(table)
            chart.canvas.parentNode.appendChild(tooltipEl)
        }

        return tooltipEl
    }

    externalTooltipHandler(context) {
        const { chart, tooltip } = context
        const tooltipEl = this.getOrCreateTooltip(chart)

        if (tooltip.opacity === 0) {
            tooltipEl.style.opacity = 0
            return
        }

        const position = chart.canvas.getBoundingClientRect()
        const bodyFont = tooltip.options.bodyFont

        tooltipEl.style.opacity = 1
        tooltipEl.style.position = 'fixed'
        tooltipEl.style.left = position.left + tooltip.caretX + 'px'
        tooltipEl.style.top = position.top + tooltip.caretY + 'px'
        tooltipEl.style.font = bodyFont.string
        tooltipEl.style.padding = tooltip.options.padding + 'px'
        tooltipEl.style.background = 'rgba(0, 0, 0, 0.8)'
        tooltipEl.style.borderRadius = '4px'
        tooltipEl.style.color = 'white'
        tooltipEl.style.pointerEvents = 'none'
        tooltipEl.style.transform = 'translate(-50%, -100%)'
        tooltipEl.style.transition = 'all .1s ease'
        tooltipEl.style.zIndex = '1000'
        tooltipEl.style.boxShadow = '0 2px 4px rgba(0, 0, 0, 0.2)'

        const table = tooltipEl.querySelector('table')
        table.style.margin = '0px'
        table.style.borderSpacing = '0'
        table.style.borderCollapse = 'collapse'

        if (tooltip.body) {
            const titleLines = tooltip.title || []
            const bodyLines = tooltip.body.map(bodyItem => bodyItem.lines)

            const tableHead = document.createElement('thead')
            titleLines.forEach(title => {
                const tr = document.createElement('tr')
                tr.style.borderWidth = '0'
                const th = document.createElement('th')
                th.style.borderWidth = '0'
                const text = document.createTextNode(title)
                th.appendChild(text)
                tr.appendChild(th)
                tableHead.appendChild(tr)
            })
            table.appendChild(tableHead)

            const tableBody = document.createElement('tbody')
            bodyLines.forEach((body, i) => {
                const tr = document.createElement('tr')
                tr.style.backgroundColor = 'inherit'
                tr.style.borderWidth = '0'

                const td = document.createElement('td')
                td.style.borderWidth = '0'
                const text = document.createTextNode(body)
                td.appendChild(text)
                tr.appendChild(td)
                tableBody.appendChild(tr)
            })
            table.appendChild(tableBody)
        }
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
                this.executeQuery()
            }
        })
        this.nextButton.addEventListener('click', () => {
            const totalPages = Math.ceil(this.totalRows / this.pageSize)
            if (this.currentPage < totalPages) {
                this.currentPage++
                this.executeQuery()
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
            const response = await fetch(`${config.apiBaseUrl}${config.endpoints.query}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    query: this.query,
                    page: this.currentPage,
                    page_size: this.pageSize,
                    file_path: this.file_path
                })
            })

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`)
            }

            const data = await response.json()
            this.updateTable(data)
            this.updatePagination(data.total_rows)
        } catch (error) {
            console.error('Query execution error:', error)
            this.error = error.message
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

            const response = await fetch(`${config.api.baseUrl}${config.api.endpoints.init}`, {
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
                tr.appendChild(td)
            })
            this.tableBody.appendChild(tr)
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
        this.charts[column] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    data: data.counts,
                    backgroundColor: 'rgba(54, 162, 235, 0.5)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1,
                    barPercentage: 1.0,
                    categoryPercentage: 1.0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        enabled: false,
                        external: this.externalTooltipHandler.bind(this),
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
                        display: false
                    }
                }
            }
        })
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
            valueDiv.title = `${label}: ${count.toLocaleString()}건`
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

    updatePagination(totalRows) {
        this.totalRows = totalRows
        const totalPages = Math.ceil(totalRows / this.pageSize)
        
        this.paginationInfo.textContent = `총 ${totalRows.toLocaleString()}건 중 ${((this.currentPage - 1) * this.pageSize + 1).toLocaleString()}~${Math.min(this.currentPage * this.pageSize, totalRows).toLocaleString()}건 표시`
        
        this.prevButton.disabled = this.currentPage === 1
        this.nextButton.disabled = this.currentPage >= totalPages
        
        this.pageInfo.textContent = `${this.currentPage} / ${totalPages}`
    }
}

export default DataViewer