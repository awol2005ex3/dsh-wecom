import { createWriteStream, openAsBlob } from 'node:fs'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { randomUUID } from 'node:crypto'

export interface MediaConfig {
  /** 沙箱根目录（来自 sandboxPolicy.workspaceRoot）。 */
  sandboxRoot: string
}

export class MediaHandler {
  constructor(private cfg: MediaConfig) {}

  /** 下载企微媒体资源到沙箱，返回本地路径 */
  async download(mediaId: string, ext: string): Promise<string> {
    const dir = join(this.cfg.sandboxRoot, 'wecom-media')
    await mkdir(dir, { recursive: true })
    const localPath = join(dir, `${randomUUID()}${ext}`)

    // ⚠️ URL 与鉴权方式以官方文档「获取媒体资源」一节为准
    const url = `https://openws.work.weixin.qq.com/cgi-bin/media/get?media_id=${encodeURIComponent(mediaId)}`
    const res = await fetch(url)
    if (!res.ok || !res.body) throw new Error(`media download failed: ${res.status}`)
    await pipeline(Readable.fromWeb(res.body as unknown as import('node:stream/web').ReadableStream), createWriteStream(localPath))
    return localPath
  }

  /** 上传临时素材，返回 media_id */
  async upload(filePath: string, type: 'image' | 'file'): Promise<string> {
    // ⚠️ 接口地址与 form-data 字段名以官方文档「上传临时素材」一节为准；
    //   Node ≥19.8 使用全局 FormData + fs.openAsBlob，无需第三方 form-data 依赖
    const form = new FormData()
    form.append('media', await openAsBlob(filePath), filePath.split(/[\\/]/).pop())
    form.append('type', type)

    const res = await fetch('https://openws.work.weixin.qq.com/cgi-bin/media/upload', {
      method: 'POST',
      body: form,
    })
    const data = await res.json() as any
    if (data?.errcode) throw new Error(`upload failed: ${data.errmsg}`)
    return data.media_id
  }
}