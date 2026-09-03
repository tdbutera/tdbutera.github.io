/**
 * buteranet-matcher, Cloudflare Worker
 * Route: buteranet.com/api/match
 *
 * POST { jd: "<job description text>", tsToken?: "<turnstile token>" }
 * Streams SSE text deltas from Claude (claude-sonnet-4-6) back to the browser.
 *
 * Required Worker secrets (set via `wrangler secret put`):
 *   ANTHROPIC_API_KEY, your Anthropic API key
 *   TURNSTILE_SECRET, (stored as a Worker secret; never hardcode it here)
 */

const CORS = {
  'Access-Control-Allow-Origin':  'https://buteranet.com',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

const SYSTEM_PROMPT = `You are an expert career analyst helping a hiring manager or recruiter understand how Travis D. Butera's background aligns with a job description.

## Travis D. Butera, Background

**Current Role:** Cyber Assistant, ISSM, and Cyberspace Workforce Program Manager
Commander, Submarine Squadron ONE (COMSUBRON ONE), Pearl Harbor, HI
Responsible for cybersecurity posture across seven operational fast-attack submarines.

**Service:** U.S. Navy Senior Chief Petty Officer (E-8) | 18+ years DoD enterprise IT & cybersecurity leadership

**Clearance:** Active Top Secret / SCI Eligible (TS/SCI)

**Education:** B.S. Information Technology, Purdue Global, Summa Cum Laude, December 2025

**Planned Retirement / Transition:** February 2028

**Key Technical Skills & Experience:**
- Full RMF / ATO lifecycle authority, eMASS, ACAS/Tenable, VRAM, STIG compliance, eMASS package management
- ISIC Cyber Inspector qualified; executed 4 major inspections in a single year, all satisfactory or better
- Contributed to the highest-scoring Fleet Cyber Command cyber inspection in submarine history (Navy and Marine Corps Commendation Medal, Commodore award)
- Co-authored ITN rate occupational standards at the CNO level
- Managed cybersecurity posture across 7 operational fast-attack submarines concurrently
- $5.1M in managed IT assets; 100% operational uptime in forward-deployed environments
- Enterprise infrastructure management: multi-site, multi-platform, classified/unclassified network separation
- KMI (Key Management Infrastructure) certified; executed KMI inspection, satisfactory
- HBSS / McAfee ePolicy Orchestrator, IA vulnerability management, POA&M tracking
- Workforce development: developed 32+ technical personnel across 5 paygrades

**Leadership & Program Management:**
- Cyberspace Workforce Program Manager for COMSUBRON ONE
- Led turnarounds of failing cyber programs; built teams that perform under inspection pressure
- Joint operations experience; DoD enterprise at the highest operational security tier
- Mentored and trained junior ISSM-track personnel

**Certifications (in progress, targeted completion before retirement):**
- CompTIA Security+ (75% complete)
- CISM, Certified Information Security Manager (35% complete)
- PMP, Project Management Professional (pursuing)
- CISSP (pursuing, after CISM)

**Target Roles Post-Retirement:**
ISSM, ISSO, Cybersecurity Manager, IT Program Manager, Director of IT/Cyber, cleared defense contractor roles (Leidos, SAIC, Booz Allen, GDIT, Peraton, Raytheon, Northrop Grumman, etc.), Federal civilian (GS-13/14/15, DoD/IC), Senior Cybersecurity Analyst

## Your Task

Analyze the job description provided by the user and produce a structured match report with these sections:

1. **Overall Match Score**, X / 10 with a one-sentence rationale. Use this fixed scale: 9-10 = 90%+ of hard requirements met, clearance matches, strong interview candidate; 7-8 = most hard requirements met, minor closeable gaps; 5-6 = meets core requirements but notable gaps in cert, civilian experience, or geography; 3-4 = some alignment but significant gaps; 1-2 = fundamental misalignment. Be consistent, the same JD should always yield the same score.
2. **Hard Requirements Met**, bullet list of specific JD requirements Travis clearly meets
3. **Gaps / Partial Matches**, honest gaps (certs not yet held, civilian experience limited, etc.) with mitigation notes
4. **Top Talking Points**, 3–5 specific, concrete things Travis should lead with in an interview for this role
5. **Suggested Resume Emphasis**, which of his resume versions or experiences to highlight for this specific role

Keep the tone professional and direct. Be honest about gaps, this is for Travis's own use to prepare. Format with clear markdown headers and bullets. Do not pad the response.`;

export default {
  async fetch(request, env) {

    // ── CORS preflight ──────────────────────────────────────────────
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    if (request.method !== 'POST') {
      return json({ error: 'Method not allowed' }, 405);
    }

    // ── Parse body ──────────────────────────────────────────────────
    let body;
    try {
      body = await request.json();
    } catch {
      return json({ error: 'Invalid JSON body' }, 400);
    }

    const { jd, tsToken } = body;

    if (!jd || jd.trim().length < 30) {
      return json({ error: 'Job description too short, please paste the full text.' }, 400);
    }

    // ── Turnstile verification (if token provided) ──────────────────
    if (tsToken && env.TURNSTILE_SECRET) {
      const tsRes = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ secret: env.TURNSTILE_SECRET, response: tsToken }),
      });
      const tsData = await tsRes.json();
      if (!tsData.success) {
        return json({ error: 'Bot verification failed. Please refresh and try again.' }, 403);
      }
    }

    // ── Call Claude API (streaming) ─────────────────────────────────
    const claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type':      'application/json',
        'x-api-key':         env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model:       'claude-sonnet-4-6',
        max_tokens:  2048,
        temperature: 0.2,
        stream:      true,
        system:      SYSTEM_PROMPT,
        messages:   [{
          role:    'user',
          content: `Analyze this job description and produce the match report:\n\n${jd.trim().slice(0, 8000)}`,
        }],
      }),
    });

    if (!claudeRes.ok) {
      const err = await claudeRes.text();
      return json({ error: 'Claude API error: ' + err }, 502);
    }

    // ── Transform Claude SSE → plain text SSE ──────────────────────
    // Claude emits: data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"..."}}
    // We emit:      data: <raw text chunk>\n\n
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();

    (async () => {
      const reader = claudeRes.body.getReader();
      const dec = new TextDecoder();
      let buf = '';

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buf += dec.decode(value, { stream: true });
          const lines = buf.split('\n');
          buf = lines.pop(); // keep incomplete line in buffer

          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            const raw = line.slice(6).trim();
            if (raw === '[DONE]') continue;
            try {
              const evt = JSON.parse(raw);
              if (evt.type === 'content_block_delta' && evt.delta?.type === 'text_delta') {
                const chunk = evt.delta.text;
                await writer.write(encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`));
              }
            } catch { /* skip malformed lines */ }
          }
        }
        // Signal completion
        await writer.write(encoder.encode('data: [DONE]\n\n'));
      } finally {
        writer.close();
      }
    })();

    return new Response(readable, {
      headers: {
        ...CORS,
        'Content-Type':  'text/event-stream',
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
      },
    });
  },
};

// ── helpers ──────────────────────────────────────────────────────────
function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
}
