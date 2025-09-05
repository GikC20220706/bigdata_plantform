import { http } from '@/utils/http'
import type { ColonyInfo } from '../components/component'

export type QueryComputeInstancesParam = {
  page: number
  pageSize: number
  searchKeyWord?: string | null
}

export type ComputeInstance = {
  workflowInstanceId: string
  workflowName: string
  duration: number
  startDateTime: string
  endDateTime: string
  status: string
  lastModifiedBy: string
}

export interface ResponseWarp<T> {
  code: number
  data: T
  msg: string
}

export type QueryComputeInstancesResponse = {
  totalElements: number
  content: ComputeInstance[]
}

export function queryComputeInstances(data: QueryComputeInstancesParam) {
  // 获取最近的任务实例列表
  return http.request<ResponseWarp<QueryComputeInstancesResponse>>({
    method: 'get',
    url: '/api/v1/scheduler/recent-task-instances',
    params: {
      page: data.page,
      page_size: data.pageSize,
      search: data.searchKeyWord,
      date: new Date().toISOString().split('T')[0]
    }
  })
}

export type SystemBaseInfoItem = {
  total: number
  activeNum: number
}

export type SystemBaseInfo = {
  clusterMonitor: SystemBaseInfoItem
  datasourceMonitor: SystemBaseInfoItem
  workflowMonitor: SystemBaseInfoItem
  apiMonitor: SystemBaseInfoItem
}

// 1. 首先定义新的调度器响应类型
export type SchedulerOverviewResponse = {
  airflow_status: {
    healthy: boolean
    scheduler_status: string
    version: string
  }
  dag_statistics: {
    total_dags: number
    active_dags: number
    paused_dags: number
    platform_generated_dags: number
  }
  task_statistics: {
    total_tasks: number
    success_tasks: number
    failed_tasks: number
    running_tasks: number
    success_rate: number
  }
  system_info: any
}

export function querySystemBaseInfo() {
  return http.request<ResponseWarp<SchedulerOverviewResponse>>({
    method: 'get',
    url: '/api/v1/scheduler/overview'
  }).then(response => {
    // 数据格式适配
    if (response && response.data) {
      const overview = response.data
      const dagStats = overview.dag_statistics || {}
      const taskStats = overview.task_statistics || {}
      
      // 转换为原有的数据格式
      const adaptedData: SystemBaseInfo = {
        clusterMonitor: {
          activeNum: dagStats.active_dags || 0,
          total: dagStats.total_dags || 1
        },
        datasourceMonitor: {
          activeNum: taskStats.total_tasks || 0,
          total: taskStats.total_tasks || 1
        },
        workflowMonitor: {
          activeNum: dagStats.platform_generated_dags || 0,
          total: dagStats.total_dags || 1
        },
        apiMonitor: {
          activeNum: taskStats.success_tasks || 0,
          total: taskStats.total_tasks || 1
        }
      }
      
      return {
        code: response.code || 200,
        data: adaptedData,
        msg: response.msg || 'success'
      } as ResponseWarp<SystemBaseInfo>
    }
    
    // 返回默认数据
    return {
      code: 500,
      data: {
        clusterMonitor: { activeNum: 0, total: 1 },
        datasourceMonitor: { activeNum: 0, total: 1 },
        workflowMonitor: { activeNum: 0, total: 1 },
        apiMonitor: { activeNum: 0, total: 1 }
      },
      msg: 'error'
    } as ResponseWarp<SystemBaseInfo>
  }).catch(error => {
    console.error('获取系统基础信息失败:', error)
    return {
      code: 500,
      data: {
        clusterMonitor: { activeNum: 0, total: 1 },
        datasourceMonitor: { activeNum: 0, total: 1 },
        workflowMonitor: { activeNum: 0, total: 1 },
        apiMonitor: { activeNum: 0, total: 1 }
      },
      msg: 'error'
    } as ResponseWarp<SystemBaseInfo>
  })
}

export type QueryVmChartInfoParams = {
  localDate: string
}

export type queryVmChartInfoResponse = {
  instanceNumLine: Array<{
    runningNum: number
    successNum: number
    failNum: number
    localTime: string
  }>
}

export function queryVmChartInfo(data: QueryVmChartInfoParams) {
  // 获取当天所有DAG运行的任务实例，按小时统计
  return http.request<ResponseWarp<queryVmChartInfoResponse>>({
    method: 'get',
    url: '/api/v1/scheduler/task-statistics',
    params: {
      date: data.localDate,
      group_by: 'hour'
    }
  })
}

export type QueryClusterMonitorInfoParams = {
  clusterId: string
  timeType: 'THIRTY_MIN' | 'ONE_HOUR' | 'TWO_HOUR' | 'SIX_HOUR' | 'TWELVE_HOUR' | 'ONE_DAY' | 'SEVEN_DAY' | 'THIRTY_DAY'
}

export type ClusterMonitorInfo = {
  activeNodeSize: number
  tenantId: string
  dateTime: string
  cpuPercent: string
  diskIoReadSpeed: string
  networkIoReadSpeed: string
  diskIoWriteSpeed: string
  networkIoWriteSpeed: string
  usedMemorySize: string
  usedStorageSize: string
}

export type QueryClusterMonitorInfoResponse = {
  line: ClusterMonitorInfo[]
}

export function queryClusterMonitorInfo(data: QueryClusterMonitorInfoParams) {
  return http.request<ResponseWarp<QueryClusterMonitorInfoResponse>>({
    method: 'post',
    url: '/monitor/getClusterMonitor',
    data
  })
}


export function queryAllClusterInfo() {
  return http.request<ResponseWarp<ColonyInfo[]>>({
    method: 'post',
    url: '/cluster/queryAllCluster'
  })
}