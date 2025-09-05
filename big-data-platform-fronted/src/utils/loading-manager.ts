interface LoadingState {
  [key: string]: boolean
}

interface LoadingCallbacks {
  [key: string]: ((loading: boolean) => void)[]
}

class LoadingManager {
  private loadingStates: LoadingState = {}
  private callbacks: LoadingCallbacks = {}

  // 设置loading状态
  setLoading(key: string, loading: boolean): void {
    this.loadingStates[key] = loading
    
    // 触发所有注册的回调函数
    if (this.callbacks[key]) {
      this.callbacks[key].forEach(callback => {
        try {
          callback(loading)
        } catch (error) {
          console.error(`Loading callback error for key ${key}:`, error)
        }
      })
    }
  }

  // 获取loading状态
  getLoading(key: string): boolean {
    return this.loadingStates[key] || false
  }

  // 注册loading状态变化回调
  onLoadingChange(key: string, callback: (loading: boolean) => void): () => void {
    if (!this.callbacks[key]) {
      this.callbacks[key] = []
    }
    this.callbacks[key].push(callback)
    
    // 返回取消订阅的函数
    return () => {
      if (this.callbacks[key]) {
        const index = this.callbacks[key].indexOf(callback)
        if (index > -1) {
          this.callbacks[key].splice(index, 1)
        }
      }
    }
  }

  // 批量设置loading状态
  setBatchLoading(states: LoadingState): void {
    Object.keys(states).forEach(key => {
      this.setLoading(key, states[key])
    })
  }

  // 清理loading状态
  clearLoading(key?: string): void {
    if (key) {
      delete this.loadingStates[key]
      delete this.callbacks[key]
    } else {
      this.loadingStates = {}
      this.callbacks = {}
    }
  }

  // 获取所有loading状态
  getAllLoadingStates(): LoadingState {
    return { ...this.loadingStates }
  }

  // 检查是否有任何loading状态为true
  hasAnyLoading(): boolean {
    return Object.values(this.loadingStates).some(loading => loading)
  }

  // 等待指定的loading完成
  waitForLoading(key: string, timeout: number = 30000): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.getLoading(key)) {
        resolve()
        return
      }

      const timer = setTimeout(() => {
        unsubscribe()
        reject(new Error(`Loading timeout for key: ${key}`))
      }, timeout)

      const unsubscribe = this.onLoadingChange(key, (loading) => {
        if (!loading) {
          clearTimeout(timer)
          unsubscribe()
          resolve()
        }
      })
    })
  }

  // 创建loading装饰器
  createLoadingDecorator(key: string) {
    return <T extends (...args: any[]) => Promise<any>>(fn: T): T => {
      return ((...args: any[]) => {
        this.setLoading(key, true)
        
        const promise = fn(...args)
        
        promise
          .finally(() => {
            this.setLoading(key, false)
          })
          .catch(() => {
            // 确保即使出错也会清除loading状态
          })

        return promise
      }) as T
    }
  }
}

// 创建全局实例
export const loadingManager = new LoadingManager()

// 导出类型
export type { LoadingState, LoadingCallbacks }
export { LoadingManager }