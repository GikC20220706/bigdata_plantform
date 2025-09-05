<template>
  <div class="sys-info">
    <div class="sys-info__header">
      <span class="sys-info__title">系统监控</span>
      <div class="sys-info__ops">
        <el-icon class="sys-info__icon" @click="querySysInfoData"><RefreshRight /></el-icon>
        <!-- <el-icon class="sys-info__icon"><Setting /></el-icon> -->
      </div>
    </div>
    <div class="sys-info__body">
      <template v-for="sysInfo in sysInfoData" :key="sysInfo.type">
        <sys-chart :chart-data="sysInfo"></sys-chart>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import SysChart from './sys-chart.vue'
import { ChartInfo } from './component'
import { onMounted, ref } from 'vue'
import { querySystemBaseInfo } from '../services/computer-group';
import { http } from '@/utils/http'

const sysInfoData = ref<Array<ChartInfo>>([
  {
    type: 'clusterMonitor',
    title: '计算集群',
    mix: undefined,
    total: 100,
    color: '#ED7B09'
  },
  {
    type: 'datasourceMonitor',
    title: '数据源',
    mix: undefined,
    total: 100,
    color: '#1967BF'
  },
  {
    type: 'workflowMonitor',
    title: '发布作业',
    mix: undefined,
    total: 100,
    color: '#03A89D'
  },
  {
    type: 'apiMonitor',
    title: '发布接口',
    mix: undefined,
    total: 100,
    color: '#4B19BF'
  }
])

function updateSysInfoItem(type: string, activeNum: number, total: number) {
  const item = sysInfoData.value.find(info => info.type === type)
  if (item) {
    item.mix = activeNum
    item.total = Math.max(total, 1) // 避免除0错误
  }
}

async function querySysInfoData() {
  try {
    const response = await http.request({
      method: 'get',
      url: '/api/v1/scheduler/overview'
    })

    if (response.success && response.data) {
      const overview = response.data
      
      // 更新统计数据 - 使用真实的DAG和任务统计
      const dagStats = overview.dag_statistics || {}
      const taskStats = overview.task_statistics || {}
      
      updateSysInfoItem('clusterMonitor', dagStats.active_dags || 0, dagStats.total_dags || 1)
      updateSysInfoItem('datasourceMonitor', taskStats.total_tasks || 0, taskStats.total_tasks || 1)
      updateSysInfoItem('workflowMonitor', dagStats.platform_generated_dags || 0, dagStats.total_dags || 1)
      updateSysInfoItem('apiMonitor', taskStats.success_tasks || 0, taskStats.total_tasks || 1)
    }

  } catch (error) {
    console.error('获取系统统计信息失败:', error)
    sysInfoData.value.forEach(item => {
      if (item.mix === undefined) {
        item.mix = 0
        item.total = Math.max(item.total, 1)
      }
    })
  }
}

onMounted(() => {
  querySysInfoData()
})

</script>

<style lang="scss">
.sys-info {
  margin-bottom: 24px;
  .sys-info__header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    height: 40px;
  }

  .sys-info__body {
    margin-top: 24px;
    display: flex;
    justify-content: space-between;

    .sys-chart {
      margin-right: 24px;
      &:last-child {
        margin-right: 0;
      }
    }
  }

  .sys-info__title {
    font-size: getCssVar('font-size', 'medium');
    font-weight: bold;
  }

  .sys-info__icon {
    margin-right: 12px;
    cursor: pointer;

    &:hover {
      color: getCssVar('color', 'primary');
    }

    &:last-child {
      margin-right: 0;
    }
  }
}
</style>