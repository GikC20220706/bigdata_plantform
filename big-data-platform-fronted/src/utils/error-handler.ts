interface ErrorInfo {
  code?: string | number
  message: string
  data?: any
  timestamp?: string
}

interface ErrorHandlerOptions {
  showMessage?: boolean
  logToConsole?: boolean
  reportToServer?: boolean
}

class ErrorHandler {
  private errorLog: ErrorInfo[] = []
  private readonly maxLogSize = 100

  // 处理错误
  handleError(error: any, options: ErrorHandlerOptions = {}): ErrorInfo {
    const errorInfo = this.parseError(error)
    
    // 记录错误日志
    this.logError(errorInfo)
    
    // 控制台输出
    if (options.logToConsole !== false) {
      console.error('Error caught:', errorInfo)
    }
    
    // 上报服务器 (可选)
    if (options.reportToServer) {
      this.reportError(errorInfo)
    }
    
    return errorInfo
  }

  // 解析错误对象
  private parseError(error: any): ErrorInfo {
    let errorInfo: ErrorInfo = {
      message: '未知错误',
      timestamp: new Date().toISOString()
    }

    if (error) {
      // 处理字符串错误
      if (typeof error === 'string') {
        errorInfo.message = error
      }
      // 处理Error对象
      else if (error instanceof Error) {
        errorInfo.message = error.message
        errorInfo.code = 'JS_ERROR'
      }
      // 处理HTTP错误响应
      else if (error.response) {
        errorInfo.code = error.response.status
        errorInfo.message = error.response.data?.message || error.response.data?.msg || error.message || '请求失败'
        errorInfo.data = error.response.data
      }
      // 处理自定义错误对象
      else if (typeof error === 'object') {
        errorInfo.code = error.code || error.status
        errorInfo.message = error.message || error.msg || '操作失败'
        errorInfo.data = error.data
      }
    }

    return errorInfo
  }

  // 记录错误日志
  private logError(errorInfo: ErrorInfo): void {
    this.errorLog.unshift(errorInfo)
    
    // 限制日志大小
    if (this.errorLog.length > this.maxLogSize) {
      this.errorLog = this.errorLog.slice(0, this.maxLogSize)
    }
  }

  // 上报错误到服务器
  private async reportError(errorInfo: ErrorInfo): Promise<void> {
    try {
      // 这里可以实现错误上报逻辑
      await fetch('/api/error-report', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(errorInfo)
      })
    } catch (reportError) {
      console.error('Error reporting failed:', reportError)
    }
  }

  // 获取错误日志
  getErrorLog(): ErrorInfo[] {
    return [...this.errorLog]
  }

  // 清空错误日志
  clearErrorLog(): void {
    this.errorLog = []
  }

  // 创建错误处理中间件
  createErrorMiddleware() {
    return (error: any, options?: ErrorHandlerOptions) => {
      return this.handleError(error, options)
    }
  }

  // 网络错误判断
  isNetworkError(error: any): boolean {
    return error && (
      error.code === 'NETWORK_ERROR' ||
      error.code === 'TIMEOUT' ||
      (error.message && error.message.includes('Network Error')) ||
      (error.message && error.message.includes('timeout'))
    )
  }

  // 权限错误判断
  isAuthError(error: any): boolean {
    return error && (
      error.code === 401 ||
      error.code === 403 ||
      (error.response && (error.response.status === 401 || error.response.status === 403))
    )
  }

  // 业务错误判断
  isBusinessError(error: any): boolean {
    return error && error.code && error.code >= 400 && error.code < 500 && error.code !== 401 && error.code !== 403
  }
}

// 创建全局实例
export const errorHandler = new ErrorHandler()

// 导出类型
export type { ErrorInfo, ErrorHandlerOptions }
export { ErrorHandler }