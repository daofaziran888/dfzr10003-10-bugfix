import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/backfill'
const columns = [
  '批次编号',
  '目标表名',
  '开始日期',
  '结束日期',
  '并行数量',
  '覆盖模式',
  '重跑状态',
  '已完成水位',
  '异常区间',
  '未完成依赖',
  '待处理原因',
]
const actions = ['启动重跑', '暂停重跑', '恢复重跑', '归档批次']
const statuses = ['草稿', '运行中', '已暂停', '已完成']
const stats = [
  { key: 'total', label: '重跑批次总数' },
  { key: 'running', label: '运行中批次' },
  { key: 'paused', label: '已暂停批次' },
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

type Outcome = {
  执行结果: string
  中断日期: string
  原因: string
}

const blankOutcome: Outcome = { 执行结果: '成功', 中断日期: '', 原因: '' }

export default function BackfillView() {
  const [rows, setRows] = useState<Row[]>([])
  const [todos, setTodos] = useState<Todo[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [outcomes, setOutcomes] = useState<Record<string, Outcome>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const [listResponse, todoResponse] = await Promise.all([
        request(`${ENDPOINT}?${query}`),
        request(`${ENDPOINT}/todos`),
      ])
      if (!listResponse.ok) {
        throw new Error('重跑批次列表读取失败')
      }
      const payload = await listResponse.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
      if (todoResponse.ok) {
        const todoPayload = await todoResponse.json()
        setTodos(todoPayload.items ?? [])
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '历史重跑列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    const outcome = outcomes[String(row.id)]
    const body: Record<string, unknown> = { action }
    if (action.startsWith('恢复重跑') || action.startsWith('启动重跑')) {
      if (outcome?.执行结果 && outcome.执行结果 !== '成功') {
        body['执行结果'] = outcome.执行结果
      }
      if (outcome?.中断日期) {
        body['中断日期'] = outcome.中断日期
      }
      if (outcome?.原因) {
        body['原因'] = outcome.原因
      }
    }
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify(body),
      })
      const payload = await response.json().catch(() => null)
      if (!response.ok) {
        throw new Error(payload?.detail ?? '历史重跑动作未生效，请稍后重试')
      }
      // 动作接口返回 ok=false 时说明依赖未完成或状态不合法，必须把原因摆在页面上。
      if (payload?.ok === false) {
        setErrorMessage(String(payload.message))
      } else {
        setNotice(String(payload?.message ?? ''))
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '历史重跑操作失败')
    }
  }

  function updateOutcome(rowId: string, patch: Partial<Outcome>) {
    setOutcomes((prev) => ({
      ...prev,
      [rowId]: { ...blankOutcome, ...prev[rowId], ...patch },
    }))
  }

  const statValues: Record<string, number> = {
    total,
    running: rows.filter((row) => row['重跑状态'] === '运行中').length,
    paused: rows.filter((row) => row['重跑状态'] === '已暂停').length,
    blocked: rows.filter((row) => String(row['未完成依赖'] ?? '') !== '').length,
  }

  return (
    <section className="page" data-module="backfill">
      <header className="page-head">
        <div>
          <h2>历史重跑管理</h2>
          <p className="page-desc">
            维护历史重跑批次的日期范围、覆盖模式与并行参数；重跑状态同步进调度待办，
            ETL 任务页读取同一份未完成依赖与已完成水位。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('重跑批次登记入口尚未接入审批流')}>
            登记重跑批次
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出历史重跑清单
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
            <th>执行结果回填</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const outcome = outcomes[String(row.id)] ?? blankOutcome
            return (
              <tr key={String(row.id)}>
                {columns.map((column) => (
                  <td key={column}>{row[column] ?? '—'}</td>
                ))}
                <td className="outcome-cell">
                  <select
                    value={outcome.执行结果}
                    onChange={(event) => updateOutcome(String(row.id), { 执行结果: event.target.value })}
                  >
                    {['成功', '部分成功', '中断', '超时', '失败'].map((value) => (
                      <option key={value} value={value}>{value}</option>
                    ))}
                  </select>
                  <input
                    value={outcome.中断日期}
                    placeholder="中断/超时日期"
                    onChange={(event) => updateOutcome(String(row.id), { 中断日期: event.target.value })}
                  />
                  <input
                    value={outcome.原因}
                    placeholder="失败原因"
                    onChange={(event) => updateOutcome(String(row.id), { 原因: event.target.value })}
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
            )
          })}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 2}>暂无历史重跑数据，可先登记重跑批次</td>
            </tr>
          )}
        </tbody>
      </table>

      <h3 className="section-title">调度待办（ETL 任务页同源）</h3>
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
        <span>共 {total} 条历史重跑记录</span>
        {notice && <span className="notice-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
