type Task = () => Promise<void>

export class SessionQueue {
  private queues = new Map<string, Task[]>()
  private running = new Set<string>()

  async enqueue(sessionId: string, task: Task) {
    if (!this.queues.has(sessionId)) this.queues.set(sessionId, [])
    this.queues.get(sessionId)!.push(task)
    if (!this.running.has(sessionId)) this.drain(sessionId)
  }

  private async drain(sessionId: string) {
    this.running.add(sessionId)
    const q = this.queues.get(sessionId)!
    while (q.length) {
      const t = q.shift()!
      try { await t() } catch { /* bridge 层已兜底 */ }
    }
    this.running.delete(sessionId)
    this.queues.delete(sessionId)   // 空闲即释放
  }
}