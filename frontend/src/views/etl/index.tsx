import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/etl'
const columns = [
  '任务编号',
  '任务名称',
  '依赖任务',
  '调度表达式',
  '重试次数',
  '超时分钟',
  '告警渠道',
  '任务状态',
  '已完成水位',
  '异常区间',
  '未完成依赖',
  '待处理原因',
]
const actions = ['启动任务', '暂停任务', '完成依赖', '记录失败', '记录中断', '归档任务']
const statuses = ['草稿', '运行中', '已暂停', '已完成']
const stats = [
  { key: 'total', label: '调度任务总数' },
  { key: 'running', label: '运行中任务' },
  { key: 'paused', label: '已暂停任务' },
  { key: 'blocked', label: '依赖未完成' },
]

type Row = Record<string, string | number | null>

type Todo = {
  '批次编号': string
  '关联调度任务': string
  '重跑状态': string
  '已完成水位': string
  '异常区间': string
  '未完成依赖': string
  '待处理原因': string
  '调度待办状态': string
}

export default function EtlView() {
  const [rows, setRows] = useState<Row[]>([])
  const [todos, setTodos] = useState<Todo[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [watermarks, setWatermarks] = useState<Record<string, string>>({})
  const [reasons, setReasons] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const [listResponse, todoResponse] = await Promise.all([
        request(`${ENDPOINT}?${query}`),
        request('/api/backfill/todos'),
      ])
      if (!listResponse.ok) {
        throw new Error('调度任务列表读取失败')
      }
      const payload = await listResponse.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
      if (todoResponse.ok) {
        const todoPayload = await todoResponse.json()
        setTodos(todoPayload.items ?? [])
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : 'ETL任务调度列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    const body: Record<string, unknown> = { action }
    if (action === '完成依赖') {
      const watermark = watermarks[String(row.id)]
      if (watermark) {
        body['已完成水位'] = watermark
      }
    }
    if (reasons[String(row.id)]) {
      body['原因'] = reasons[String(row.id)]
    }
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify(body),
      })
      const payload = await response.json().catch(() => null)
      if (!response.ok) {
        throw new Error(payload?.detail ?? 'ETL任务调度动作未生效，请稍后重试')
      }
      if (payload?.ok === false) {
        setErrorMessage(String(payload.message))
      } else {
        setNotice(String(payload?.message ?? ''))
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : 'ETL任务调度操作失败')
    }
  }

  const statValues: Record<string, number> = {
    total,
    running: rows.filter((row) => row['任务状态'] === '运行中').length,
    paused: rows.filter((row) => row['任务状态'] === '已暂停').length,
    blocked: rows.filter((row) => String(row['未完成依赖'] ?? '') !== '').length,
  }

  return (
    <section className="page" data-module="etl">
      <header className="page-head">
        <div>
          <h2>ETL任务调度管理</h2>
          <p className="page-desc">
            维护ETL任务的调度配置、依赖关系与告警策略；未完成依赖与已完成水位直接读取调度待办，
            与历史重跑页保持同一个口径。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('调度任务登记入口尚未接入审批流')}>
            登记调度任务
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出ETL任务调度清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{statValues[item.key] ?? 0}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <label className="filter-item">
          <span>状态</span>
          <select
            value={filters.status ?? ''}
            onChange={(event) => setFilters((prev) => ({ ...prev, status: event.target.value }))}
          >
            <option value="">全部</option>
            {statuses.map((status) => <option key={status} value={status}>{status}</option>)}
          </select>
        </label>
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>水位/原因回填</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => (
                <td key={column}>{row[column] ?? '—'}</td>
              ))}
              <td className="outcome-cell">
                <input
                  value={watermarks[String(row.id)] ?? ''}
                  placeholder="推进到水位"
                  onChange={(event) => setWatermarks((prev) => ({ ...prev, [String(row.id)]: event.target.value }))}
                />
                <input
                  value={reasons[String(row.id)] ?? ''}
                  placeholder="失败/中断原因"
                  onChange={(event) => setReasons((prev) => ({ ...prev, [String(row.id)]: event.target.value }))}
                />
              </td>
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无ETL任务调度数据，可先登记调度任务</td>
            </tr>
          )}
        </tbody>
      </table>

      <h3 className="section-title">关联调度待办（与重跑页同源）</h3>
      <table className="data-table">
        <thead>
          <tr>
            <th>批次编号</th>
            <th>关联调度任务</th>
            <th>重跑状态</th>
            <th>已完成水位</th>
            <th>异常区间</th>
            <th>未完成依赖</th>
            <th>调度待办状态</th>
            <th>待处理原因</th>
          </tr>
        </thead>
        <tbody>
          {todos.map((todo) => (
            <tr key={todo['批次编号']}>
              <td>{todo['批次编号']}</td>
              <td>{todo['关联调度任务'] || '—'}</td>
              <td>{todo['重跑状态']}</td>
              <td>{todo['已完成水位'] || '—'}</td>
              <td>{todo['异常区间'] || '—'}</td>
              <td>{todo['未完成依赖'] || '—'}</td>
              <td>{todo['调度待办状态'] || '—'}</td>
              <td>{todo['待处理原因'] || '—'}</td>
            </tr>
          ))}
          {todos.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={8}>暂无调度待办</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条ETL任务调度记录</span>
        {notice && <span className="notice-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
