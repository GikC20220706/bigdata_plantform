// 工作流状态映射
export const WorkflowStatusMapping = {
  'DRAFT': 'UN_PUBLISHED',
  'ACTIVE': 'PUBLISHED', 
  'PAUSED': 'STOP',
  'ERROR': 'FAIL',
  'DISABLED': 'DISABLE',
  'ENABLED': 'ENABLE'
}

// 执行状态映射
export const ExecutionStatusMapping = {
  'running': 'RUNNING',
  'success': 'SUCCESS',
  'failed': 'FAIL', 
  'cancelled': 'ABORT',
  'queued': 'PENDING',
  'up_for_retry': 'RUNNING',
  'upstream_failed': 'FAIL',
  'skipped': 'SUCCESS'
}

// 数据源状态映射
export const DataSourceStatusMapping = {
  'active': 'ACTIVE',
  'inactive': 'NO_ACTIVE',
  'error': 'CHECK_ERROR',
  'testing': 'CHECKING'
}

// API状态映射
export const ApiStatusMapping = {
  true: 'PUBLISHED',
  false: 'UN_PUBLISHED'
}

// 统一状态映射函数
export function mapStatus(status: any, type: 'workflow' | 'execution' | 'datasource' | 'api'): string {
  const mappings = {
    workflow: WorkflowStatusMapping,
    execution: ExecutionStatusMapping, 
    datasource: DataSourceStatusMapping,
    api: ApiStatusMapping
  }
  
  const mapping = mappings[type]
  return mapping[status] || status
}