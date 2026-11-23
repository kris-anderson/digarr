// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// This suite uses the REAL p-queue (no mock) plus fake timers to prove the
// rate-gate's timing contract: the backoff sleep of a retrying request lives
// OUTSIDE the single concurrency slot, so it cannot stall other MB traffic.

const mockFetch = vi.fn()
vi.stubGlobal('fetch', mockFetch)

function jsonOk(data: unknown): Response {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  })
}

beforeEach(() => {
  vi.stubEnv('DIGARR_MUSICBRAINZ_URL', undefined)
  vi.stubEnv('DIGARR_MUSICBRAINZ_INTERVAL_MS', undefined)
  vi.useFakeTimers()
  vi.setSystemTime(0) // anchor Date.now() so fetch timestamps are relative to t=0
  mockFetch.mockReset()
})

afterEach(() => {
  vi.unstubAllEnvs()
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('MusicBrainz shared queue timing (real p-queue)', () => {
  it.each([
    [undefined, undefined, 1100],
    ['http://mirror.example/ws/2', '25', 25],
  ])('shares the configured interval across clients (%s, %s)', async (url, interval, expected) => {
    vi.setSystemTime(1)
    vi.stubEnv('DIGARR_MUSICBRAINZ_URL', url)
    vi.stubEnv('DIGARR_MUSICBRAINZ_INTERVAL_MS', interval)
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')
    const times: number[] = []
    mockFetch.mockImplementation(async () => {
      times.push(Date.now())
      return jsonOk({ artists: [] })
    })
    const requests = [
      createMusicBrainzClient().searchArtist('A'),
      createMusicBrainzClient().searchArtist('B'),
    ]
    await vi.advanceTimersByTimeAsync(expected * 2)
    await Promise.all(requests)
    expect(times).toEqual([1, expected + 1])
  })

  it('never starts a call less than one interval after a late-starting one', async () => {
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')
    const times: number[] = []
    mockFetch.mockImplementation(async (url: string) => {
      times.push(Date.now())
      const query = new URL(url).searchParams.get('query')
      // A is slow, so B starts late in the second 1100 ms window.
      if (query === 'A') await new Promise((resolve) => setTimeout(resolve, 1900))
      return jsonOk({ artists: [] })
    })
    const client = createMusicBrainzClient()
    const requests = ['A', 'B', 'C'].map((query) => client.searchArtist(query))
    await vi.advanceTimersByTimeAsync(10_000)
    await Promise.all(requests)
    // A fixed window started C at 2200, 300 ms after B.
    expect(times).toEqual([0, 1900, 3000])
  })

  it('keeps one in-flight request across clients when the mirror interval is zero', async () => {
    vi.stubEnv('DIGARR_MUSICBRAINZ_URL', 'http://mirror.example/ws/2')
    vi.stubEnv('DIGARR_MUSICBRAINZ_INTERVAL_MS', '0')
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')
    let releaseFirst: (response: Response) => void = () => {
      throw new Error('First fetch did not start')
    }
    mockFetch.mockImplementationOnce(
      () =>
        new Promise<Response>((resolve) => {
          releaseFirst = resolve
        }),
    )
    mockFetch.mockResolvedValue(jsonOk({ artists: [] }))
    const requests = [
      createMusicBrainzClient().searchArtist('A'),
      createMusicBrainzClient().searchArtist('B'),
    ]
    await vi.advanceTimersByTimeAsync(0)
    expect(mockFetch).toHaveBeenCalledTimes(1)
    releaseFirst(jsonOk({ artists: [] }))
    await vi.advanceTimersByTimeAsync(0)
    await Promise.all(requests)
    expect(mockFetch).toHaveBeenCalledTimes(2)
    expect(Date.now()).toBe(0)
  })

  it('runs queued interactive requests before background ones, retries included', async () => {
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')
    const order: string[] = []
    let failedOnce = false
    mockFetch.mockImplementation(async (url: string) => {
      const query = new URL(url).searchParams.get('query') ?? ''
      order.push(query)
      if (query === 'scan-1' && !failedOnce) {
        failedOnce = true
        return new Response('', { status: 503 })
      }
      return jsonOk({ artists: [] })
    })
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const background = createMusicBrainzClient({ priority: 'background' })
    const scan = createMusicBrainzClient()
    const requests = [
      background.searchArtist('bg-1'),
      background.searchArtist('bg-2'),
      background.searchArtist('bg-3'),
      scan.searchArtist('scan-1'),
      scan.searchArtist('scan-2'),
    ]
    await vi.advanceTimersByTimeAsync(10_000)
    await Promise.all(requests)
    // The retry re-enters at interactive priority and goes ahead of every older
    // background request (p-queue 10 no longer lets bg-2 take the slot first).
    expect(order).toEqual(['bg-1', 'scan-1', 'scan-2', 'scan-1', 'bg-2', 'bg-3'])
  })

  it('a retrying request does not block other queued traffic for its full backoff', async () => {
    // Fresh module load so the shared queue is constructed under fake timers.
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')

    const fetches: Array<{ query: string; t: number }> = []
    mockFetch.mockImplementation(async (url: string) => {
      const query = new URL(url).searchParams.get('query') ?? ''
      const seenForQuery = fetches.filter((f) => f.query === query).length
      fetches.push({ query, t: Date.now() })
      // Caller A's first attempt fails transiently (forces a backoff + retry);
      // every other fetch (A's retry, all of B) succeeds. A 502 is retried
      // without throttling the queue, so B's timing shows only the slot.
      if (query === 'AAA' && seenForQuery === 0) {
        return new Response('', { status: 502 })
      }
      return jsonOk({ artists: [] })
    })

    const client = createMusicBrainzClient()
    const pA = client.searchArtist('AAA') // 502 then retry
    const pB = client.searchArtist('BBB') // succeeds

    await vi.advanceTimersByTimeAsync(5000)
    await Promise.all([pA, pB])

    const aFirst = fetches.find((f) => f.query === 'AAA')
    const aRetry = fetches.filter((f) => f.query === 'AAA')[1]
    const b = fetches.find((f) => f.query === 'BBB')

    expect(aFirst?.t).toBe(0)
    expect(b).toBeDefined()
    expect(aRetry).toBeDefined()
    // B runs one queue interval after A's first attempt (~1s), NOT after A's
    // whole retry/backoff cycle. Pre-fix it was stuck behind A at ~2s+.
    expect(b?.t).toBeLessThan(1500)
    // And B is served before A's retry fetch (A's ~1s backoff is spent outside
    // the slot). Pre-fix, A held the slot through its in-slot sleep, so B's
    // fetch could not run until A's whole retry cycle finished -> b.t >= aRetry.t.
    expect(b?.t).toBeLessThan(aRetry?.t ?? Number.POSITIVE_INFINITY)
  })
})

describe('MusicBrainz adaptive pacing (real p-queue)', () => {
  // Every query in `failing` answers its first fetch with `failure`; all other
  // fetches succeed. Returns the fetch log in start order.
  async function run(failing: Record<string, () => Response>, queries: string[]) {
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')
    const fetches: Array<{ query: string; t: number }> = []
    mockFetch.mockImplementation(async (url: string) => {
      const query = new URL(url).searchParams.get('query') ?? ''
      const seen = fetches.some((f) => f.query === query)
      fetches.push({ query, t: Date.now() })
      const failure = failing[query]
      return failure && !seen ? failure() : jsonOk({ artists: [] })
    })
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const client = createMusicBrainzClient()
    const requests = queries.map((query) => client.searchArtist(query))
    await vi.advanceTimersByTimeAsync(120_000)
    await Promise.all(requests)
    return fetches
  }

  it('a 503 without Retry-After widens the spacing for the next queued call', async () => {
    const fetches = await run({ A: () => new Response('', { status: 503 }) }, ['A', 'B'])
    expect(fetches.map((f) => f.query)).toEqual(['A', 'B', 'A'])
    // B is a different caller. Without adaptive pacing it ran one interval
    // (1100 ms) after A's 503; now the whole queue waits one extra step.
    expect(fetches[1]?.t).toBe(2200)
  })

  it('a 503 with Retry-After delays the next queued call, not just the retry', async () => {
    const throttled = () => new Response('', { status: 503, headers: { 'Retry-After': '5' } })
    const fetches = await run({ A: throttled }, ['A', 'B'])
    expect(fetches.map((f) => f.query)).toEqual(['A', 'B', 'A'])
    expect(fetches[1]?.t).toBe(5000)
  })

  it('does not widen the spacing for a non-throttling 502', async () => {
    const fetches = await run({ A: () => new Response('', { status: 502 }) }, ['A', 'B'])
    expect(fetches[1]).toEqual({ query: 'B', t: 1100 })
  })

  it('narrows the spacing back to the configured interval as calls succeed', async () => {
    const queries = Array.from({ length: 14 }, (_, i) => `q${i}`)
    const fetches = await run({ q0: () => new Response('', { status: 503 }) }, queries)
    const gaps = fetches.slice(1).map((f, i) => f.t - (fetches[i]?.t ?? 0))
    // One 503 adds one 1100 ms step; each success removes 110 ms of it.
    expect(gaps.slice(0, 11)).toEqual([
      2200, 2090, 1980, 1870, 1760, 1650, 1540, 1430, 1320, 1210, 1100,
    ])
    expect(new Set(gaps.slice(10))).toEqual(new Set([1100]))
  })

  it('doubles the extra spacing on back-to-back throttles, up to eight steps', async () => {
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')
    const times: number[] = []
    mockFetch.mockImplementation(async () => {
      times.push(Date.now())
      return new Response('', { status: 429 })
    })
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const client = createMusicBrainzClient()
    const requests = ['a', 'b', 'c', 'd', 'e'].map((query) =>
      expect(client.searchArtist(query)).rejects.toThrow(/MusicBrainz HTTP 429/),
    )
    await vi.advanceTimersByTimeAsync(60_000)
    const gaps = times.slice(1, 6).map((t, i) => t - (times[i] ?? 0))
    expect(gaps).toEqual([2200, 3300, 5500, 9900, 9900])
    await vi.runAllTimersAsync()
    await Promise.all(requests)
  })

  it('lets a queued caller abort while the queue is held', async () => {
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')
    const fetches: Array<{ query: string; t: number }> = []
    mockFetch.mockImplementation(async (url: string) => {
      const query = new URL(url).searchParams.get('query') ?? ''
      fetches.push({ query, t: Date.now() })
      return query === 'A' && fetches.length === 1
        ? new Response('', { status: 503 })
        : jsonOk({ artists: [] })
    })
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const client = createMusicBrainzClient()
    const controller = new AbortController()
    const a = client.searchArtist('A')
    const b = client.searchArtist('B', { signal: controller.signal })
    const rejected = expect(b).rejects.toMatchObject({ name: 'AbortError' })
    const c = client.searchArtist('C')
    // A's 503 holds the queue until 2200 ms; B is still queued at 1500 ms.
    await vi.advanceTimersByTimeAsync(1500)
    controller.abort()
    await rejected
    await vi.advanceTimersByTimeAsync(10_000)
    await Promise.all([a, c])
    expect(fetches.map((f) => f.query)).toEqual(['A', 'C', 'A'])
    expect(fetches[1]?.t).toBe(2200)
  })
})

describe('MusicBrainz caller cancellation', () => {
  it.each([0, 1])('drops an aborted catalog queue entry after %i pages', async (pages) => {
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')
    const client = createMusicBrainzClient()
    const fetches: Array<{ path: string; t: number }> = []
    mockFetch.mockImplementation(async (url: string) => {
      const path = new URL(url).pathname
      fetches.push({ path, t: Date.now() })
      return jsonOk(
        path.endsWith('/release-group')
          ? {
              'release-group-count': 2,
              'release-groups': [
                { id: 'first', title: 'First', 'primary-type': 'Album', 'secondary-types': [] },
              ],
            }
          : { artists: [] },
      )
    })
    await client.searchArtist('prime')
    const controller = new AbortController()
    const catalog = client.getReleaseGroups('cancelled', undefined, { signal: controller.signal })
    const rejected = expect(catalog).rejects.toMatchObject({ name: 'AbortError' })
    await vi.advanceTimersByTimeAsync(pages * 1100)
    controller.abort()
    await rejected
    const victim = client.searchArtist('victim')
    await vi.advanceTimersByTimeAsync(5000)
    await victim
    expect(fetches.filter(({ path }) => path.endsWith('/release-group'))).toHaveLength(pages)
    expect(fetches.at(-1)).toEqual({ path: '/ws/2/artist/', t: (pages + 1) * 1100 })
  })

  it('aborts an active fetch without retrying or fetching subsequent pages', async () => {
    vi.resetModules()
    const { createMusicBrainzClient } = await import('@/core/clients/musicbrainz')
    let fetchSignal: AbortSignal | undefined
    mockFetch.mockImplementation(
      (_url: string, init: RequestInit) =>
        new Promise((_resolve, reject) => {
          fetchSignal = init.signal ?? undefined
          fetchSignal?.addEventListener('abort', () => reject(fetchSignal?.reason), { once: true })
        }),
    )
    const controller = new AbortController()
    const catalog = createMusicBrainzClient().getReleaseGroups('active', undefined, {
      signal: controller.signal,
    })
    const rejected = expect(catalog).rejects.toMatchObject({ name: 'AbortError' })
    await vi.advanceTimersByTimeAsync(0)
    expect(mockFetch).toHaveBeenCalledTimes(1)
    controller.abort()
    await rejected
    expect(fetchSignal?.aborted).toBe(true)
    await vi.advanceTimersByTimeAsync(30_000)
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })
})
