export {
  cloneDeep,
  clone,
  concat,
  map,
  debounce,
  findIndex,
  find,
  some,
  forEach,
  reduce,
  random,
  assign,
  omit,
  attempt,
  uniq,
  filter,
  includes,
  orderBy,
  isEmpty,
  isEqual,
  isNaN,
  // isNull,
  isString,
  isUndefined,
  merge
} from 'lodash-es'

const toString = Object.prototype.toString

// 判断值是否为某个类型
export function is(val: unknown, type: string) {
  return toString.call(val) === `[object ${type}]`
}

// 是否已定义
export const isDef = <T = unknown>(val?: T): val is T => {
  return typeof val !== 'undefined'
}

// 是否未定义
export const isUnDef = <T = unknown>(val?: T): val is T => {
  return !isDef(val)
}

// // 是否为null
export function isNull(val: unknown): val is null {
  return val === null
}

// 是否为null并且未定义
export function isNullAndUnDef(val: unknown): val is null | undefined {
  return isUnDef(val) && isNull(val)
}

// 是否为null或者未定义
export function isNullOrUnDef(val: unknown): val is null | undefined {
  return isUnDef(val) || isNull(val)
}

// 是否为函数
export function isFunction<T = Function>(val: unknown): val is T {
  return is(val, 'Function')
}

// 是否为对象
export const isObject = (val: any): val is Record<any, any> => {
  return val !== null && is(val, 'Object')
}

// 是否为日期
export function isDate(val: unknown): val is Date {
  return is(val, 'Date')
}

// 是否为数值
export function isNumber(val: unknown): val is number {
  return is(val, 'Number')
}

// 是否为字符串
// export function isString(val: unknown): val is string {
//   return is(val, 'String');
// }

// 是否为布尔
export function isBoolean(val: unknown): val is boolean {
  return is(val, 'Boolean')
}

// 是否为数组
export function isArray(val: any): val is Array<any> {
  return val && Array.isArray(val)
}

// 是否为AsyncFunction
export function isAsyncFunction<T = any>(val: unknown): val is Promise<T> {
  return is(val, 'AsyncFunction')
}

// 是否为promise
export function isPromise<T = any>(val: unknown): val is Promise<T> {
  return is(val, 'Promise') && isObject(val) && isFunction(val.then) && isFunction(val.catch)
}

// 是否为客户端
export const isClient = () => {
  return typeof window !== 'undefined'
}

// 是否为服务端
export const isServer = typeof window === 'undefined'

// 是否为window
export const isWindow = (val: any): val is Window => {
  return typeof window !== 'undefined' && is(val, 'Window')
}

// 是否为元素
export const isElement = (val: unknown): val is Element => {
  return isObject(val) && !!val.tagName
}

// 是否为图片节点
export function isImageDom(o: Element) {
  return o && [ 'IMAGE', 'IMG' ].includes(o.tagName)
}

// 是否为完整的url
export function isUrl(url: string) {
  return /(^http|https:\/\/)/g.test(url)
}

export function setObjToUrlParams(baseUrl: string, obj: any): string {
  let parameters = ''
  let url = ''
  for (const key in obj) {
    parameters += key + '=' + encodeURIComponent(obj[key]) + '&'
  }
  parameters = parameters.replace(/&$/, '')
  if (/\?$/.test(baseUrl)) {
    url = baseUrl + parameters
  } else {
    url = baseUrl.replace(/\/?$/, '?') + parameters
  }
  return url
}

// 返回URL中全部或者某个查询参数的值
export function getUrlParam(url: string, key?: string): string | null {
  const urlObj = new URL(url)
  return key ? urlObj.searchParams.get(key) : null
  // const arr: string[] = url.substring(url.indexOf('?') + 1).split('&');
  // const obj: any = {};
  // for (let i = 0; i < arr.length; i++) {
  //   if (arr[i].indexOf('=') < 0) {
  //     obj[arr[i]] = 'undefined';
  //   } else {
  //     const arr2 = arr[i].split('=');
  //     obj[arr2[0]] = decodeURIComponent(arr2[1]);
  //   }
  // }
  // return !key ? obj : obj[key];
}

export function deepMerge<T = any>(src: any = {
}, target: any = {
}): T {
  let key: string
  for (key in target) {
    src[key] = isObject(src[key]) ? deepMerge(src[key], target[key]) : target[key]
  }
  return src
}

// 状态映射相关函数
export function mapWorkflowStatus(status: string): string {
  const statusMap: Record<string, string> = {
    'DRAFT': 'UN_PUBLISHED',
    'ACTIVE': 'PUBLISHED', 
    'PAUSED': 'STOP',
    'ERROR': 'FAIL',
    'DISABLED': 'DISABLE',
    'ENABLED': 'ENABLE'
  }
  return statusMap[status] || status
}

export function mapExecutionStatus(status: string): string {
  const statusMap: Record<string, string> = {
    'running': 'RUNNING',
    'success': 'SUCCESS',
    'failed': 'FAIL', 
    'cancelled': 'ABORT',
    'queued': 'PENDING',
    'up_for_retry': 'RUNNING',
    'upstream_failed': 'FAIL',
    'skipped': 'SUCCESS'
  }
  return statusMap[status] || status
}

export function mapDataSourceStatus(status: string): string {
  const statusMap: Record<string, string> = {
    'active': 'ACTIVE',
    'inactive': 'NO_ACTIVE',
    'error': 'CHECK_ERROR',
    'testing': 'CHECKING'
  }
  return statusMap[status] || status
}

export function mapApiStatus(isActive: boolean): string {
  return isActive ? 'PUBLISHED' : 'UN_PUBLISHED'
}

// 数据格式转换函数
export function transformPaginationData(data: any) {
  if (data && data.items && Array.isArray(data.items)) {
    return {
      ...data,
      content: data.items,
      totalElements: data.total || 0,
      currentPage: data.page || 1,
      pageSize: data.page_size || 20,
      totalPages: Math.ceil((data.total || 0) / (data.page_size || 20))
    }
  }
  
  if (data && data.workflows && Array.isArray(data.workflows)) {
    return {
      ...data,
      content: data.workflows,
      totalElements: data.total || 0,
      currentPage: data.page || 1,
      pageSize: data.page_size || 20
    }
  }
  
  if (data && data.clusters && Array.isArray(data.clusters)) {
    return {
      ...data,
      content: data.clusters,
      totalElements: data.total || 0,
      currentPage: data.page || 1,
      pageSize: data.page_size || 20
    }
  }
  
  if (data && data.executions && Array.isArray(data.executions)) {
    return {
      ...data,
      content: data.executions,
      totalElements: data.total || 0,
      currentPage: data.page || 1,
      pageSize: data.page_size || 20
    }
  }
  
  return data
}

// 时间相关函数
export function calculateDuration(startTime: string, endTime?: string): number {
  if (!startTime) return 0
  const start = new Date(startTime).getTime()
  const end = endTime ? new Date(endTime).getTime() : Date.now()
  return Math.max(0, Math.floor((end - start) / 1000))
}

export function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}秒`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}分${seconds % 60}秒`
  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const secs = seconds % 60
  return `${hours}小时${minutes}分${secs}秒`
}

// 文件大小格式化
export function formatFileSize(bytes: number): string {
  if (!bytes || bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

// 数字格式化
export function formatNumber(num: number, digits: number = 2): string {
  if (!num) return '0'
  const si = [
    { value: 1E18, symbol: 'E' },
    { value: 1E15, symbol: 'P' },
    { value: 1E12, symbol: 'T' },
    { value: 1E9, symbol: 'G' },
    { value: 1E6, symbol: 'M' },
    { value: 1E3, symbol: 'K' }
  ]
  for (let i = 0; i < si.length; i++) {
    if (num >= si[i].value) {
      return (num / si[i].value).toFixed(digits).replace(/\.0+$|(\.[0-9]*[1-9])0+$/, '$1') + si[i].symbol
    }
  }
  return num.toString()
}

// 百分比格式化
export function formatPercentage(value: number, total: number, digits: number = 1): string {
  if (!total || total === 0) return '0%'
  return ((value / total) * 100).toFixed(digits) + '%'
}

// 安全获取对象属性值
export function safeGet(obj: any, path: string, defaultValue: any = null): any {
  try {
    return path.split('.').reduce((current, key) => current && current[key], obj) || defaultValue
  } catch {
    return defaultValue
  }
}

// 生成唯一ID
export function generateId(prefix: string = 'id'): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`
}

// 防抖函数增强版
export function debounceWithOptions<T extends (...args: any[]) => any>(
  func: T,
  wait: number,
  options: { leading?: boolean; trailing?: boolean; maxWait?: number } = {}
): (...args: Parameters<T>) => void {
  let lastArgs: Parameters<T> | undefined
  let lastThis: any
  let maxTimeoutId: NodeJS.Timeout | undefined
  let timeoutId: NodeJS.Timeout | undefined
  let lastCallTime: number | undefined
  let lastInvokeTime = 0
  let leading = false
  let maxing = false
  let trailing = true

  if (options.leading !== undefined) leading = !!options.leading
  if (options.trailing !== undefined) trailing = !!options.trailing
  if (options.maxWait !== undefined) maxing = true

  function invokeFunc(time: number) {
    const args = lastArgs
    const thisArg = lastThis
    lastArgs = lastThis = undefined
    lastInvokeTime = time
    return func.apply(thisArg, args as Parameters<T>)
  }

  function leadingEdge(time: number) {
    lastInvokeTime = time
    timeoutId = setTimeout(timerExpired, wait)
    return leading ? invokeFunc(time) : undefined
  }

  function remainingWait(time: number) {
    const timeSinceLastCall = time - (lastCallTime || 0)
    const timeSinceLastInvoke = time - lastInvokeTime
    const timeWaiting = wait - timeSinceLastCall
    return maxing ? Math.min(timeWaiting, (options.maxWait || 0) - timeSinceLastInvoke) : timeWaiting
  }

  function shouldInvoke(time: number) {
    const timeSinceLastCall = time - (lastCallTime || 0)
    const timeSinceLastInvoke = time - lastInvokeTime
    return (lastCallTime === undefined || timeSinceLastCall >= wait ||
            timeSinceLastCall < 0 || (maxing && timeSinceLastInvoke >= (options.maxWait || 0)))
  }

  function timerExpired() {
    const time = Date.now()
    if (shouldInvoke(time)) {
      return trailingEdge(time)
    }
    timeoutId = setTimeout(timerExpired, remainingWait(time))
  }

  function trailingEdge(time: number) {
    timeoutId = undefined
    if (trailing && lastArgs) {
      return invokeFunc(time)
    }
    lastArgs = lastThis = undefined
    return undefined
  }

  function cancel() {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId)
    }
    if (maxTimeoutId !== undefined) {
      clearTimeout(maxTimeoutId)
    }
    lastInvokeTime = 0
    lastArgs = lastCallTime = lastThis = timeoutId = maxTimeoutId = undefined
  }

  function flush() {
    return timeoutId === undefined ? undefined : trailingEdge(Date.now())
  }

  function debounced(...args: Parameters<T>) {
    const time = Date.now()
    const isInvoking = shouldInvoke(time)
    lastArgs = args
    lastThis = this
    lastCallTime = time

    if (isInvoking) {
      if (timeoutId === undefined) {
        return leadingEdge(lastCallTime)
      }
      if (maxing) {
        clearTimeout(timeoutId)
        timeoutId = setTimeout(timerExpired, wait)
        return invokeFunc(lastCallTime)
      }
    }
    if (timeoutId === undefined) {
      timeoutId = setTimeout(timerExpired, wait)
    }
  }
  
  debounced.cancel = cancel
  debounced.flush = flush
  return debounced
}