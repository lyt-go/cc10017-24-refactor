<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <section v-if="lineageSnapshot" class="snapshot-panel" data-testid="workbench-lineage-snapshot">
      <header class="snapshot-head">
        <div>
          <h3>血缘图版本快照</h3>
          <p class="page-desc">工作台同步流式血缘冻结的版本快照；关联记录刷新后仍保持原版本与历史口径。</p>
        </div>
        <button class="btn" type="button" data-testid="workbench-refresh-snapshot" @click="loadLineageSnapshot">刷新快照</button>
      </header>
      <div class="snapshot-meta">
        <span>血缘版本：{{ lineageSnapshot.version }}</span>
        <span>修订号：{{ lineageSnapshot.revision }}</span>
        <span>节点：{{ lineageSnapshot.node_count }}</span>
        <span>边：{{ lineageSnapshot.edge_count }}</span>
        <span>边顺序口径：{{ lineageSnapshot.edge_policy === 'legacy_order' ? '保留旧边顺序' : lineageSnapshot.edge_policy }}</span>
        <span>冻结时间：{{ lineageSnapshot.frozen_at }}</span>
      </div>
      <p v-if="deadLetterContext" class="snapshot-message" data-testid="workbench-dead-letter-context">
        已交付死信处理：版本 {{ deadLetterContext.snapshot_version }}（修订 {{ deadLetterContext.snapshot_revision }}）
      </p>
    </section>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

type LineageSnapshot = {
  version: string
  revision: number
  node_count: number
  edge_count: number
  edge_policy: string
  frozen_at: string
}

type DeadLetterContext = {
  snapshot_version: string
  snapshot_revision: number
  delivered_at?: string | null
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])
const lineageSnapshot = ref<LineageSnapshot | null>(null)
const deadLetterContext = ref<DeadLetterContext | null>(null)

async function loadLineageSnapshot() {
  try {
    const payload = await fetchJson<{ snapshot: LineageSnapshot | null; dead_letter_context: DeadLetterContext | null }>(
      '/api/pipeline/lineage-snapshot/current',
    )
    lineageSnapshot.value = payload.snapshot
    deadLetterContext.value = payload.dead_letter_context
  } catch {
    lineageSnapshot.value = null
    deadLetterContext.value = null
  }
}

onMounted(async () => {
  await loadLineageSnapshot()
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "主题注册", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流式计算", "created": 0, "pending": 0, "abnormal": 0}, {"name": "消息接入", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Schema演进", "created": 0, "pending": 0, "abnormal": 0}, {"name": "增量合并", "created": 0, "pending": 0, "abnormal": 0}, {"name": "湖上分区", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流式质量", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Iceberg管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "水位追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "死信处理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "物化查询", "created": 0, "pending": 0, "abnormal": 0}, {"name": "CDC捕获", "created": 0, "pending": 0, "abnormal": 0}, {"name": "合并清理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流式血缘", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流式补数", "created": 0, "pending": 0, "abnormal": 0}, {"name": "下沉分发", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据生命周期", "created": 0, "pending": 0, "abnormal": 0}, {"name": "实时指标", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
