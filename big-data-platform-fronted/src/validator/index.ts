/*
 * @Author: fanciNate
 * @Date: 2023-04-29 17:44:44
 * @LastEditTime: 2023-04-29 21:21:56
 * @LastEditors: fanciNate
 * @Description: In User Settings Edit
 * @FilePath: /zqy-web/src/validator/index.ts
 */
import { isFunction } from '@/utils/checkType'
import HostValidator from './host.validator'
import PortValidator from './port.validator'

export const Validator = {
  HostValidator,
  PortValidator
}

export function _validate(type: any, message: string, trigger: string[]) {
  // 生成验证器
  const validator = {
    trigger: trigger,
    validator: null
  }
  if (isFunction(type)) {
    validator.validator = type
  }

  return validator
}
// 数据源连接参数验证器
export function createDataSourceValidator(type: string) {
  const validators: Record<string, any> = {
    mysql: {
      host: { required: true, message: '请输入主机地址' },
      port: { required: true, type: 'number', min: 1, max: 65535, message: '请输入有效的端口号' },
      database: { required: true, message: '请输入数据库名' },
      username: { required: true, message: '请输入用户名' },
      password: { required: true, message: '请输入密码' }
    },
    hive: {
      host: { required: true, message: '请输入HiveServer2地址' },
      port: { required: true, type: 'number', min: 1, max: 65535, message: '请输入有效的端口号' },
      username: { required: true, message: '请输入用户名' }
    },
    doris: {
      host: { required: true, message: '请输入FE地址' },
      port: { required: true, type: 'number', min: 1, max: 65535, message: '请输入有效的端口号' },
      database: { required: true, message: '请输入数据库名' },
      username: { required: true, message: '请输入用户名' },
      password: { required: true, message: '请输入密码' }
    },
    kingbase: {
      host: { required: true, message: '请输入主机地址' },
      port: { required: true, type: 'number', min: 1, max: 65535, message: '请输入有效的端口号' },
      database: { required: true, message: '请输入数据库名' },
      username: { required: true, message: '请输入用户名' },
      password: { required: true, message: '请输入密码' }
    },
    dm: {
      host: { required: true, message: '请输入主机地址' },
      port: { required: true, type: 'number', min: 1, max: 65535, message: '请输入有效的端口号' },
      database: { required: true, message: '请输入数据库名' },
      username: { required: true, message: '请输入用户名' },
      password: { required: true, message: '请输入密码' }
    },
    tidb: {
      host: { required: true, message: '请输入主机地址' },
      port: { required: true, type: 'number', min: 1, max: 65535, message: '请输入有效的端口号' },
      database: { required: true, message: '请输入数据库名' },
      username: { required: true, message: '请输入用户名' },
      password: { required: true, message: '请输入密码' }
    }
  }
  
  return validators[type] || {}
}

// API参数验证器
export function createApiParamValidator() {
  return {
    apiName: { required: true, message: '请输入API名称' },
    apiPath: { 
      required: true, 
      message: '请输入API路径',
      pattern: /^\/[a-zA-Z0-9\/_-]*$/,
      trigger: 'blur'
    },
    dataSourceId: { required: true, message: '请选择数据源' },
    sqlTemplate: { required: true, message: '请输入SQL模板' }
  }
}

// 工作流验证器
export function createWorkflowValidator() {
  return {
    name: { required: true, message: '请输入工作流名称' },
    displayName: { required: true, message: '请输入显示名称' }
  }
}
5. 新建文件：src/utils/cache-manager.ts
typescriptinterface CacheItem<T> {
  data: T
  timestamp: number
  expireTime?: number
}

interface CacheOptions {
  expireTime?: number // 过期时间（毫秒）
  maxSize?: number    // 最大缓存条目数
}

class CacheManager {
  private cache = new Map<string, CacheItem<any>>()
  private readonly defaultExpireTime = 5 * 60 * 1000 // 5分钟
  private readonly defaultMaxSize = 100

  // 设置缓存
  set<T>(key: string, data: T, options: CacheOptions = {}): void {
    const expireTime = options.expireTime || this.defaultExpireTime
    const maxSize = options.maxSize || this.defaultMaxSize

    // 检查缓存大小限制
    if (this.cache.size >= maxSize && !this.cache.has(key)) {
      // 删除最旧的缓存项
      const oldestKey = this.cache.keys().next().value
      this.cache.delete(oldestKey)
    }

    const cacheItem: CacheItem<T> = {
      data,
      timestamp: Date.now(),
      expireTime: expireTime > 0 ? Date.now() + expireTime : undefined
    }

    this.cache.set(key, cacheItem)
  }

  // 获取缓存
  get<T>(key: string): T | null {
    const cacheItem = this.cache.get(key)
    
    if (!cacheItem) {
      return null
    }

    // 检查是否过期
    if (cacheItem.expireTime && Date.now() > cacheItem.expireTime) {
      this.cache.delete(key)
      return null
    }

    return cacheItem.data as T
  }

  // 检查缓存是否存在且未过期
  has(key: string): boolean {
    return this.get(key) !== null
  }

  // 删除缓存
  delete(key: string): boolean {
    return this.cache.delete(key)
  }

  // 清空所有缓存
  clear(): void {
    this.cache.clear()
  }

  // 清理过期缓存
  clearExpired(): void {
    const now = Date.now()
    for (const [key, item] of this.cache.entries()) {
      if (item.expireTime && now > item.expireTime) {
        this.cache.delete(key)
      }
    }
  }

  // 获取缓存统计信息
  getStats(): { size: number; items: Array<{ key: string; timestamp: number; expireTime?: number }> } {
    const items = Array.from(this.cache.entries()).map(([key, item]) => ({
      key,
      timestamp: item.timestamp,
      expireTime: item.expireTime
    }))

    return {
      size: this.cache.size,
      items
    }
  }

  // 创建带缓存的函数
  createCachedFunction<T extends (...args: any[]) => Promise<any>>(
    fn: T,
    keyGenerator: (...args: Parameters<T>) => string,
    options: CacheOptions = {}
  ): T {
    return ((...args: Parameters<T>) => {
      const cacheKey = keyGenerator(...args)
      const cachedResult = this.get(cacheKey)
      
      if (cachedResult !== null) {
        return Promise.resolve(cachedResult)
      }

      const result = fn(...args)
      
      if (result && typeof result.then === 'function') {
        return result.then((data: any) => {
          this.set(cacheKey, data, options)
          return data
        })
      }
      
      return result
    }) as T
  }
}

// 创建全局实例
export const cacheManager = new CacheManager()

// 定期清理过期缓存
setInterval(() => {
  cacheManager.clearExpired()
}, 60 * 1000) // 每分钟清理一次

// 导出类型
export type { CacheItem, CacheOptions }
export { CacheManager }