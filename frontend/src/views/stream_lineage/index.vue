<template>
  <section class="page" data-module="stream_lineage">
    <header class="page-head">
      <div><h2>流式血缘</h2><p class="page-desc">维护流式数据从消息到湖表的全链路血缘与延迟追踪，为流式血缘提供可验证的数据工程流程。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记血缘记录</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>
    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>
    <section class="snapshot-panel" data-testid="snapshot-panel">
      <header class="snapshot-head">
        <div>
          <h3>血缘图版本快照</h3>
          <p class="page-desc">收拢节点过滤、连边与同名节点查找后冻结历史口径；同一血缘版本重复构建只生成一个快照。</p>
        </div>
        <div class="page-actions">
          <button class="btn" type="button" data-testid="freeze-snapshot" @click="freezeSnapshot">冻结当前版本快照</button>
          <button class="btn" type="button" data-testid="compare-snapshot" @click="compareSnapshot">比较重构前后图</button>
          <button class="btn" type="button" data-testid="deliver-snapshot" @click="deliverSnapshot">交付死信处理</button>
        </div>
      </header>
      <div v-if="snapshot" class="snapshot-meta">
        <span data-testid="snapshot-version">血缘版本：{{ snapshot.version }}</span>
        <span>修订号：{{ snapshot.revision }}</span>
        <span>节点：{{ snapshot.node_count }}</span>
        <span>边：{{ snapshot.edge_count }}</span>
        <span>边顺序口径：{{ snapshot.edge_policy === 'legacy_order' ? '保留旧边顺序' : snapshot.edge_policy }}</span>
        <span v-if="snapshot.duplicate_names?.length">同名节点：{{ snapshot.duplicate_names.join('、') }}</span>
      </div>
      <div v-else class="empty-state">尚未冻结快照，点击「冻结当前版本快照」生成可回查的版本。</div>
      <table v-if="snapshots.length" class="data-table">
        <thead><tr><th>版本</th><th>修订号</th><th>节点数</th><th>边数</th><th>边顺序口径</th><th>冻结时间</th></tr></thead>
        <tbody>
          <tr v-for="item in snapshots" :key="item.id" :data-testid="`snapshot-row-${item.id}`" :data-version="item.version">
            <td>{{ item.version }}</td><td>{{ item.revision }}</td><td>{{ item.node_count }}</td>
            <td>{{ item.edge_count }}</td><td>{{ item.edge_policy === 'legacy_order' ? '保留旧边顺序' : item.edge_policy }}</td>
            <td>{{ item.frozen_at }}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="compareResult" class="snapshot-compare" data-testid="snapshot-compare">
        <dl><dt>节点集合一致</dt><dd data-testid="compare-nodes-equal">{{ compareResult.nodes_equal ? '是' : '否' }}</dd></dl>
        <dl><dt>边集合一致</dt><dd>{{ compareResult.edges_equal ? '是' : '否' }}</dd></dl>
        <dl><dt>旧边顺序保留</dt><dd>{{ compareResult.edge_order_preserved ? '是' : '否' }}</dd></dl>
        <dl><dt>快照/实时节点数</dt><dd>{{ compareResult.node_count_before }} / {{ compareResult.node_count_after }}</dd></dl>
        <dl><dt>快照/实时边数</dt><dd>{{ compareResult.edge_count_before }} / {{ compareResult.edge_count_after }}</dd></dl>
      </div>
      <p v-if="snapshotMessage" class="snapshot-message" data-testid="snapshot-message">{{ snapshotMessage }}</p>
    </section>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记血缘记录</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot"><span>{{ graph.nodes.length }} 个节点</span></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/stream_lineage'
const columns = ["记录编号", "上游主题", "下游表名", "转换节点", "字段映射", "采集时间", "延迟偏移", "链路版本"]
const actions = ["提交校验", "标记生效", "停用记录"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','待校验':'#fef3c7','已启用':'#d1fae5','已停用':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<number,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)
function layout(nodes: any[]) {
  const w = 800; const h = Math.max(400, nodes.length * 80)
  svgW.value = w; svgH.value = h
  const pos: Record<number,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function startDrag(e: MouseEvent, node: any) { dragNode = node; const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }; const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }; document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp) }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); graph.nodes = data.nodes || []; graph.edges = data.edges || []; layout(graph.nodes) } catch(e) { errorMessage.value = '加载失败' }
}
async function runAction(action: string, node: any) {
  try { await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) }); await reload() } catch(e) { errorMessage.value = '操作失败' }
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }

type Snapshot = {
  id: number
  version: string
  revision: number
  node_count: number
  edge_count: number
  edge_policy: string
  duplicate_names?: string[]
  frozen_at: string
}
type CompareResult = {
  version: string
  nodes_equal: boolean
  edges_equal: boolean
  edge_order_preserved: boolean
  node_count_before: number
  node_count_after: number
  edge_count_before: number
  edge_count_after: number
}
const snapshot = ref<Snapshot | null>(null)
const snapshots = ref<Snapshot[]>([])
const compareResult = ref<CompareResult | null>(null)
const snapshotMessage = ref('')

async function loadSnapshots() {
  try {
    const res = await request(`${ENDPOINT}/snapshots`)
    if (!res.ok) return
    const data = await res.json()
    snapshots.value = data.items ?? []
    snapshot.value = data.current ?? snapshots.value[0] ?? null
  } catch {
    snapshotMessage.value = '快照加载失败'
  }
}

async function freezeSnapshot() {
  snapshotMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/snapshots/freeze?version=v1`, { method: 'POST' })
    const data = await res.json()
    if (!res.ok || !data.ok) throw new Error(data.message || '冻结失败')
    snapshotMessage.value = data.message
    await loadSnapshots()
  } catch (error) {
    snapshotMessage.value = error instanceof Error ? error.message : '快照冻结失败'
  }
}

async function compareSnapshot() {
  snapshotMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/snapshots/v1/compare`)
    if (!res.ok) throw new Error('对比失败')
    const result: CompareResult = await res.json()
    compareResult.value = result
    snapshotMessage.value = result.nodes_equal && result.edges_equal
      ? '重构前后节点与边集合一致，历史口径保持不变'
      : '检测到节点或边集合差异，请核对历史口径'
  } catch (error) {
    snapshotMessage.value = error instanceof Error ? error.message : '快照对比失败'
  }
}

async function deliverSnapshot() {
  snapshotMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/snapshots/v1/deliver`, { method: 'POST' })
    const data = await res.json()
    if (!res.ok || !data.ok) throw new Error(data.message || '交付失败')
    snapshotMessage.value = `已交付死信处理，保留血缘版本 ${data.entry?.snapshot_version ?? 'v1'}`
  } catch (error) {
    snapshotMessage.value = error instanceof Error ? error.message : '快照交付失败'
  }
}

onMounted(() => { void reload(); void loadSnapshots() })
</script>
