export class LRUCache<K, V> {
  private map = new Map<K, { value: V; expireAt: number }>()
  constructor(private capacity: number, private ttlMs: number) {}

  has(key: K): boolean {
    const entry = this.map.get(key)
    if (!entry) return false
    if (Date.now() > entry.expireAt) { this.map.delete(key); return false }
    // 命中移到末尾（Map 插入顺序即访问顺序）
    this.map.delete(key)
    this.map.set(key, entry)
    return true
  }

  set(key: K, value: V) {
    if (this.map.size >= this.capacity) {
      const firstKey = this.map.keys().next().value!
      this.map.delete(firstKey)   // 淘汰最久未访问
    }
    this.map.set(key, { value, expireAt: Date.now() + this.ttlMs })
  }
}