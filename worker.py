#!/usr/bin/env python3
"""Vivify venue-hirers discovery WORKER (Python replacement for the n8n flow).
Outcome: find groups that demonstrably book/hire a SPECIFIC venue, evidence required.

Pipeline: cache check -> discover (web search + read pages + 1-level crawl to "where we meet" pages,
charity register by postcode, exact-postcode DB, Facebook posts) -> LLM gate (real org + venue tie)
-> write via process_venue_hirer_results RPC -> status complete.

Run:  python3 worker.py <search_id>
"""
import os, re, json, base64, sys, html, time, concurrent.futures as cf, urllib.request, urllib.parse

ENV = dict(os.environ)  # Render provides secrets as env vars
_envfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(_envfile):  # local dev overlay
    for line in open(_envfile):
        if '=' in line and not line.startswith('#'):
            k, v = line.strip().split('=', 1); ENV[k] = v.strip().strip('"')
DFS_AUTH = base64.b64encode(f"{ENV['DATAFORSEO_LOGIN']}:{ENV['DATAFORSEO_PASSWORD']}".encode()).decode()
SUPA, SKEY = ENV['SUPABASE_URL'], ENV['SUPABASE_KEY']
APIFY = ENV.get('APIFY_TOKEN', '')
OPENAI_KEY = ENV.get('OPENAI_API_KEY', '') or os.environ.get('OPENAI_API_KEY', '')

# ---------------- helpers ----------------
def collapse(s): return re.sub(r'[^a-z0-9]', '', (s or '').lower())
STOPV = {'school','academy','college','high','primary','grammar','the','community','centre','center',
         'sports','leisure','club','and','of','junior','infant','park','saturday'}
def vtokens(v): return [t for t in re.split(r'[^a-z0-9]+', v.lower()) if len(t) > 3 and t not in STOPV]
def outcode(pc):
    c = collapse(pc); return c[:-3] if len(c) >= 5 else c

NOISE = ['wikipedia.org','indeed.','reed.co.uk','totaljobs','glassdoor','tes.com','eteach','rightmove','zoopla',
    'onthemarket','linkedin.com','twitter.com','x.com','reddit.com','youtube.com','amazon.','tripadvisor','yell.com',
    'companieshouse','find-and-update','company-information','schoolsweek','ofsted','goodschoolsguide','locrating',
    'schoolparrot','schoolguide.co.uk','crystalroof','foxtons','mumsnet','get-information-schools','findmyschool',
    'schooluniguide','theschoolsguide','schoolopinion','grokipedia','studocu','edval','ambition.org','pathwayctm',
    'studysmarter','mylondon','hamhigh','instagram.com','tiktok.com','snobe','tutorhunt','savemyexams','alchetron',
    'rome2rio','bustimes','londonbusroutes','tfl.gov.uk','gettyimages','wikimapia','upmystreet','edarabia','applicaaone',
    'cylex','daynurseries','moovit','mapcarta','localeiq','propertistics','streetlist','streetcheck','doogal',
    'housepriceinflation','rentaroof','sharetobuy','bellway','data.parliament','wikimedia','rocketreach','flower-shops',
    'nhs.uk','heyschools','heygolf','schoolratings','schoolsfootball','schoolsnetball','schoolsbasketball',
    'allschools','schoolstogether','schoolowl','goodschools','locatethis','cleanair','primarytimes']
AGG = ['charitycommission','findachurch','classforkids','pitchfinder','clubspark','playfootball','happity',
       'footyaddicts','hoop.co.uk','eventbrite','meetup']
def noisy(dom):
    if 'charitycommission' in dom or 'findachurch' in dom: return False
    if dom.endswith('gov.uk') and 'charitycommission' not in dom: return True
    return any(n in dom for n in NOISE)
def is_agg(dom): return any(a in dom for a in AGG)

JUNK_RE = [re.compile(p, re.I) for p in [
    r'^venue hire', r'venue hire$', r'^facilit', r'^hall hire', r'^room hire', r"^what'?s on", r'^home$',
    r'^contact', r'^about us', r'^term dates?', r'^admission', r'^newsletter', r'^vacanc', r'^career',
    r'^welcome', r'^gallery$', r'^our (classes|clubs|facilities)', r'mathsconf', r'business studies',
    r'exams? assistant', r'football pitch', r'auditorium|drama studio|gymnasium|sports hall', r'pitches? - ',
    r'match overview', r'\bvs\.? ', r'booking system', r'events calendar', r'girls pe ', r'^event:',
    r'^results?$', r'^news$', r'leggings', r'^\d', r'^map of', r'^area information', r'postcode s']]
def is_junk(name):
    n = (name or '').strip()
    if len(n) < 3 or not re.search(r'[a-z]', n, re.I): return True
    if any(r.search(n) for r in JUNK_RE): return True
    w = n.split(); caps = sum(1 for x in w if x[:1].isupper() or x[:1].isdigit())
    return len(w) >= 6 and caps <= 1

def synth(prefix, key):
    h = 0
    for ch in (prefix + '|' + key): h = (h * 31 + ord(ch)) & 0x7fffffff
    return f"{prefix}_{h}"

# ---------------- http ----------------
def dfs(kw, depth=30, retries=2):
    """Returns (items, task_cost_usd). Cost is PER QUERY (once), not per result item."""
    b = json.dumps([{"keyword": kw, "location_name": "United Kingdom", "language_code": "en", "depth": depth}]).encode()
    for attempt in range(retries + 1):
        r = urllib.request.Request("https://api.dataforseo.com/v3/serp/google/organic/live/advanced", data=b,
            headers={"Authorization": "Basic " + DFS_AUTH, "Content-Type": "application/json"})
        try:
            d = json.load(urllib.request.urlopen(r, timeout=60))
            task = d['tasks'][0]
            items = [{"url": i.get('url'), "title": i.get('title') or '', "snippet": i.get('description') or ''}
                     for i in (task['result'][0].get('items') or []) if i.get('type') == 'organic' and i.get('url')]
            return items, float(task.get('cost', 0) or 0)
        except Exception as e:
            if attempt == retries: sys.stderr.write(f"dfs err [{kw[:30]}]: {e}\n"); return [], 0.0
            time.sleep(1.5)

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
        return url, urllib.request.urlopen(req, timeout=12).read()[:700000].decode('utf-8', 'ignore')
    except Exception:
        return url, ''

def page_blob(raw, snippet, title):
    raw = re.sub(r'(?is)<(script|style|noscript|svg)[^>]*>.*?</\1>', ' ', raw)
    return collapse(title + ' ' + snippet + ' ' + html.unescape(re.sub(r'(?is)<[^>]+>', ' ', raw)))

def page_evidence(raw, venue, pc):
    """Return a short human evidence sentence around the venue/postcode mention."""
    text = re.sub(r'\s+', ' ', html.unescape(re.sub(r'(?is)<[^>]+>', ' ', re.sub(r'(?is)<(script|style)[^>]*>.*?</\1>', ' ', raw))))
    low = text.lower()
    for needle in [pc.lower(), venue.lower().split(',')[0][:18]]:
        i = low.find(needle)
        if i >= 0: return text[max(0, i-70):i+90].strip()
    return ''

def site_name(raw):
    """The site's own brand name from og:site_name / <title> tail — far cleaner than a SERP page title."""
    if not raw: return ''
    m = re.search(r'(?is)<meta[^>]+property=["\']og:site_name["\'][^>]+content=["\'](.*?)["\']', raw)
    if m:
        s = html.unescape(m.group(1)).strip()
        if s and not is_junk(s): return s
    m = re.search(r'(?is)<title[^>]*>(.*?)</title>', raw)
    if m:
        t = html.unescape(re.sub(r'\s+', ' ', m.group(1))).strip()
        # the brand usually sits after the last separator ("Page name | Brand")
        for sep in [' | ', ' – ', ' — ', ' :: ', ' • ']:
            if sep in t:
                tail = t.split(sep)[-1].strip()
                if tail and not is_junk(tail) and len(tail) <= 45: return tail
    return ''

def brand(titles, dom):
    counts = {}
    for t in titles:
        for seg in re.split(r'\s+[|–—:•\-]\s+|\s+@\s+', t or ''):
            s = seg.strip()
            if len(s) < 3 or not re.search(r'[a-z]', s, re.I) or is_junk(s): continue
            counts.setdefault(s.lower(), [0, s]); counts[s.lower()][0] += 1
    if counts: return sorted(counts.values(), key=lambda c: (-c[0], len(c[1])))[0][1]
    core = dom.split('.')[0].replace('-', ' ').replace('_', ' ')
    return core.title() if core else dom

def sublinks(rawhtml, base, dom, extra):
    out, seen = [], set()
    kws = ['location','where','venue','timetable','contact','class','madrasah','session','find-us','find us',
           'meet','term','school'] + extra
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\'#]+)["\'][^>]*>(.*?)</a>', rawhtml or ''):
        href = m.group(1); anchor = re.sub(r'<[^>]+>', ' ', m.group(2)).lower()
        full = urllib.parse.urljoin(base, href)
        if urllib.parse.urlparse(full).netloc.lower().replace('www.', '') != dom: continue
        if any(k in (href + ' ' + anchor).lower() for k in kws):
            if full not in seen: seen.add(full); out.append(full)
    return out[:3]

# ---------------- discovery ----------------
def queries_for(venue, pc):
    acts = ['football','netball','basketball','cricket','gymnastics','dance','ballet','karate','martial arts','tuition',
            'language school','saturday school','supplementary school','madrasah','quran','persian school','tamil school',
            'german saturday school','korean church','church','scouts','toddler group','holiday camp','music lessons',
            'drama','classes','club','academy','timetable']
    q = [venue, f'{venue} {pc}'] + [f'{a} {venue}' for a in acts]
    q += [f'{a} "{venue}"' for a in ['madrasah','tamil school','persian school','german saturday school','church','football','dance']]
    q += [f'"{pc}" charity', f'"{pc}" church', f'"{pc}" club', f'"{pc}" academy',
          f'site:register-of-charities.charitycommission.gov.uk "{pc}"', f'site:findachurch.co.uk "{pc}"']
    q += [f'site:{d} "{venue}"' for d in ['classforkids.io','happity.co.uk','playfootball.net','clubspark.lta.org.uk','pitchfinder.org.uk']]
    return q

def tie_kind(blob, vcol, pcol, vtoks, oc):
    if pcol and pcol in blob: return 'postcode'
    if vcol and len(vcol) > 6 and vcol in blob: return 'venue'
    if len(vtoks) >= 2 and oc and oc in blob and all(t in blob for t in vtoks): return 'venue'
    return None

def discover_web(venue, pc):
    vcol, pcol, vtoks, oc = collapse(venue), collapse(pc), vtokens(venue), outcode(pc)
    raw, seen, cost = [], set(), 0.0
    for q in queries_for(venue, pc):
        items, qcost = dfs(q)
        cost += qcost
        for r in items:
            if r['url'] in seen: continue
            seen.add(r['url']); r['domain'] = urllib.parse.urlparse(r['url']).netloc.lower().replace('www.', '')
            raw.append(r)
    cands = [r for r in raw if not noisy(r['domain']) and 'facebook.com' not in r['domain']]
    cands.sort(key=lambda c: ('charitycommission' in c['domain']) or (pcol in collapse(c['title']+c['snippet'])) or (vcol in collapse(c['title']+c['snippet'])), reverse=True)
    cands = cands[:350]
    pages = {}
    with cf.ThreadPoolExecutor(max_workers=20) as ex:
        for u, rh in ex.map(fetch, [c['url'] for c in cands]): pages[u] = rh
    byd, agg = {}, []
    for c in cands:
        blob = page_blob(pages.get(c['url'], ''), c['snippet'], c['title'])
        tie = tie_kind(blob, vcol, pcol, vtoks, oc)
        if not tie: continue
        if is_agg(c['domain']):
            agg.append({'name': brand([c['title']], c['domain']), 'domain': c['domain'], 'tie': tie, 'url': c['url'],
                        'snippet': c['snippet'], 'evidence': page_evidence(pages.get(c['url'], ''), venue, pc)})
        else:
            d = byd.setdefault(c['domain'], {'titles': [], 'tie': tie, 'url': c['url'], 'snippet': c['snippet'], 'raw': pages.get(c['url'], ''), 'sitename': site_name(pages.get(c['url'], ''))})
            d['titles'].append(c['title'])
            if not d.get('sitename'): d['sitename'] = site_name(pages.get(c['url'], ''))
            if tie == 'postcode': d['tie'] = 'postcode'
    # 1-level crawl for org pages that didn't tie on the main page
    targets = []
    for c in cands:
        if c['domain'] in byd or is_agg(c['domain']): continue
        for su in sublinks(pages.get(c['url'], ''), c['url'], c['domain'], vtoks + [oc]):
            targets.append((c['domain'], c['title'], su))
    targets = targets[:150]
    if targets:
        sub = {}
        with cf.ThreadPoolExecutor(max_workers=20) as ex:
            for u, rh in ex.map(fetch, [t[2] for t in targets]): sub[u] = rh
        for dom, title, su in targets:
            if dom in byd: continue
            blob = page_blob(sub.get(su, ''), '', title)
            tie = tie_kind(blob, vcol, pcol, vtoks, oc)
            if tie: byd[dom] = {'titles': [title], 'tie': tie, 'url': su, 'snippet': '', 'raw': sub.get(su, '')}
    out, seen_n = [], set()
    for dom, d in byd.items():
        nm = d.get('sitename') or brand(d['titles'], dom)
        if is_junk(nm) or collapse(nm) in vcol or vcol in collapse(nm): continue
        k = collapse(nm)[:20]
        if not k or k in seen_n: continue
        seen_n.add(k)
        out.append({'name': nm, 'domain': dom, 'tie': d['tie'], 'url': d['url'],
                    'snippet': d.get('snippet', ''), 'evidence': page_evidence(d.get('raw', ''), venue, pc), 'src': 'dataforseo'})
    for r in agg:
        if is_junk(r['name']) or collapse(r['name']) in vcol or vcol in collapse(r['name']): continue
        k = collapse(r['name'])[:20]
        if k in seen_n: continue
        seen_n.add(k); r['src'] = 'dataforseo'; out.append(r)
    return out, round(cost, 4)

def db_postcode(pc):
    req = urllib.request.Request(f"{SUPA}/rest/v1/rpc/find_venue_hirers_by_postcode",
        data=json.dumps({"p_postcode": pc}).encode(),
        headers={"apikey": SKEY, "Authorization": "Bearer " + SKEY, "Content-Type": "application/json"})
    try: rows = json.load(urllib.request.urlopen(req, timeout=20))
    except Exception: return []
    out, seen = [], set()
    for r in rows:
        nm = r.get('company_name') or ''
        if is_junk(nm): continue
        k = collapse(nm)[:20]
        if k in seen: continue
        seen.add(k)
        out.append({'name': nm, 'domain': '', 'tie': 'postcode', 'url': r.get('website') or '',
                    'snippet': '', 'evidence': '', 'src': 'venue_db', 'db_id': r.get('id'), 'website': r.get('website')})
    return out

def fb_posts(venue, pc):
    if not APIFY: return []
    u = f"https://api.apify.com/v2/acts/powerai~facebook-post-search-scraper/run-sync-get-dataset-items?clean=true&token={APIFY}"
    body = json.dumps({"query": venue, "maxResults": 15, "recent_posts": True, "start_date": "2025-06-26"}).encode()
    try:
        rows = json.load(urllib.request.urlopen(urllib.request.Request(u, data=body, headers={"Content-Type": "application/json"}), timeout=180))
    except Exception as e:
        sys.stderr.write(f"fb err {e}\n"); return []
    vcol, pcol = collapse(venue), collapse(pc)
    out, seen = [], set()
    for p in rows:
        if not isinstance(p, dict): continue
        a = p.get('author') or {}; nm = a.get('name')
        if not nm: continue
        msg = p.get('message') or p.get('text') or ''
        if not (pcol in collapse(msg) or (len(vcol) > 6 and vcol in collapse(msg))): continue
        k = collapse(nm)[:20]
        if not k or k in seen or is_junk(nm): continue
        seen.add(k)
        out.append({'name': nm, 'domain': 'facebook', 'tie': 'venue', 'url': a.get('url') or '',
                    'snippet': msg[:280], 'evidence': msg[:200], 'src': 'facebook_post'})
    return out

# ---------------- LLM gate ----------------
def gate(cands, venue, pc):
    """Return (verdicts, gate_cost_usd). verdicts = list of (keep, confidence, category) aligned to cands.
    Uses OpenAI gpt-4o if key present, else a deterministic fallback (flagged)."""
    if not cands: return [], 0.0
    if not OPENAI_KEY:
        return [(True, ('confirmed' if c['tie'] == 'postcode' else 'likely'), '') for c in cands], 0.0
    items = "\n".join(f"{i}. {c['name']} — {(c.get('snippet') or c.get('evidence') or '')[:200]}" for i, c in enumerate(cands))
    prompt = (
        f'A specific UK venue, "{venue}" (postcode {pc}), hires its facilities to OUTSIDE organisations: '
        f'community groups, sports clubs, dance/gym/arts providers, faith and cultural groups, tuition/language/'
        f'supplementary schools, youth and toddler groups. For EACH candidate set useful=true ONLY if BOTH: '
        f'(1) it is a REAL, NAMED organisation/club/class/group (a proper-noun name) — NOT a generic phrase, page '
        f'title, booking label, map, directory index, product or section heading; AND (2) the text ties it to THIS '
        f'venue ("{venue}") or postcode {pc} as where it runs/plays/meets — not a different venue or just the wider area. '
        f'useful=false for: the venue itself/its pages/staff, jobs, news/Ofsted, directions/maps/transport, aggregator '
        f'index pages, estate agents/area guides, unrelated businesses, bare personal names. '
        f'confidence="confirmed" if it explicitly names this venue or postcode, else "likely". When in doubt useful=false.\n'
        f'Return ONLY a JSON array: [{{"i":<index>,"useful":true|false,"confidence":"confirmed"|"likely","category":"<short>"}}]\n\n'
        + items)
    body = json.dumps({"model": "gpt-4o", "temperature": 0, "max_tokens": 4000,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    try:
        r = urllib.request.Request("https://api.openai.com/v1/chat/completions", data=body,
            headers={"Authorization": "Bearer " + OPENAI_KEY, "Content-Type": "application/json"})
        d = json.load(urllib.request.urlopen(r, timeout=120))
        txt = d['choices'][0]['message']['content']
        u = d.get('usage', {})
        gate_cost = round(u.get('prompt_tokens', 0) / 1e6 * 2.50 + u.get('completion_tokens', 0) / 1e6 * 10.0, 4)
        arr = json.loads(re.search(r'\[[\s\S]*\]', txt).group(0))
        verdict = {v['i']: v for v in arr if isinstance(v, dict) and 'i' in v}
        res = []
        for i, c in enumerate(cands):
            v = verdict.get(i)
            if v and v.get('useful'):
                res.append((True, 'confirmed' if v.get('confidence') == 'confirmed' else 'likely', v.get('category', '')))
            else:
                res.append((False, '', ''))
        return res, gate_cost
    except Exception as e:
        sys.stderr.write(f"gate err {e}\n")
        return [(True, ('confirmed' if c['tie'] == 'postcode' else 'likely'), '') for c in cands], 0.0

# ---------------- supabase ----------------
def sreq(method, path, payload=None, params=''):
    url = f"{SUPA}/rest/v1/{path}{params}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
        headers={"apikey": SKEY, "Authorization": "Bearer " + SKEY, "Content-Type": "application/json", "Prefer": "return=representation"})
    return json.load(urllib.request.urlopen(req, timeout=30))

SPEND_LOG = os.path.join(os.path.dirname(__file__), 'spend_log.csv')
def log_spend(sid, venue, pc, kept, dfs_c, gate_c, apify_c, total):
    new = not os.path.exists(SPEND_LOG)
    with open(SPEND_LOG, 'a') as f:
        if new: f.write("timestamp_utc,search_id,venue,postcode,results,dataforseo_usd,gate_usd,apify_usd,total_usd\n")
        ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        v = (venue or '').replace(',', ' ')
        f.write(f"{ts},{sid},{v},{pc},{kept},{dfs_c},{gate_c},{apify_c},{total}\n")

def get_search(sid):
    rows = sreq("GET", "group_searches", params=f"?id=eq.{sid}&select=*")
    return rows[0] if rows else None

def set_status(sid, status):
    sreq("PATCH", "group_searches", {"status": status}, params=f"?id=eq.{sid}")

def cache_lookup(venue, pc, sid):
    since = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(time.time() - 14*86400))
    q = (f"?select=id&search_type=eq.venue_hirers&status=eq.complete"
         f"&venue_name=eq.{urllib.parse.quote(venue)}&postcode=eq.{urllib.parse.quote(pc)}"
         f"&id=neq.{sid}&created_at=gte.{urllib.parse.quote(since)}&order=created_at.desc&limit=1")
    rows = sreq("GET", "group_searches", params=q)
    return rows[0]['id'] if rows else None

# ---------------- main ----------------
def to_result(c):
    name = c['name']
    pid = c.get('place_id') or (synth('vdb', str(c['db_id'])) if c.get('db_id') else synth(c['src'][:3], (c.get('url') or '') + '|' + (c.get('domain') or name)))
    return {
        "company_name": name, "address": None, "postcode": None, "city": None, "phone_number": None,
        "website": c.get('website') or (c.get('url') if c['src'] == 'dataforseo' else None),
        "facebook_url": c['url'] if c['src'] == 'facebook_post' else None,
        "activity_description": c.get('snippet') or None, "additional_information": c.get('url') or None,
        "activity_type": c.get('category') or None, "place_id": pid,
        "evidence_source": c['src'], "evidence_url": c.get('url') or None, "source_url": c.get('url') or None,
        "evidence_text": c.get('evidence') or c.get('snippet') or None, "evidence_image_url": None,
        "evidence_date": None, "confidence_tier": c['tier'],
    }

def run(sid):
    s = get_search(sid)
    if not s: sys.exit(f"search {sid} not found")
    venue = (s.get('venue_name') or s.get('search_name') or '').strip()
    pc = (s.get('postcode') or '').strip()
    print(f"[{sid}] {venue} ({pc})")
    set_status(sid, 'searching')
    prior = cache_lookup(venue, pc, sid)
    if prior:
        sreq("POST", "rpc/copy_venue_search_results", {"p_from": prior, "p_to": sid})
        set_status(sid, 'complete'); print(f"  cache hit from {prior} — £0"); return
    web, dfs_cost = discover_web(venue, pc)
    db = db_postcode(pc)
    fb = fb_posts(venue, pc)
    cands = web + db + fb
    print(f"  candidates: web={len(web)} db={len(db)} fb={len(fb)} | gate={'gpt-4o' if OPENAI_KEY else 'DETERMINISTIC(no key)'}")
    verdicts, gate_cost = gate(cands, venue, pc)
    kept = []
    for c, (ok, conf, cat) in zip(cands, verdicts):
        if not ok: continue
        c['tier'] = conf or ('confirmed' if c['tie'] == 'postcode' else 'likely'); c['category'] = cat
        kept.append(to_result(c))
    apify_cost = 0.05 if fb else 0.0
    total = round(dfs_cost + gate_cost + apify_cost, 4)
    print(f"  kept {len(kept)} of {len(cands)}")
    print(f"  SPEND: dataforseo=${dfs_cost} gate=${gate_cost} apify=${apify_cost} | total=${total}")
    log_spend(sid, venue, pc, len(kept), dfs_cost, gate_cost, apify_cost, total)
    sreq("POST", "rpc/process_venue_hirer_results", {
        "p_search_id": sid, "p_results": kept,
        "p_cost_google": 0, "p_cost_dataforseo": dfs_cost, "p_cost_apify": apify_cost,
        "p_google_calls": 0})
    # shared enrichment (still n8n): promotes staged rows -> vivify_organisations + contact details + links + cleanup trigger
    try:
        urllib.request.urlopen(urllib.request.Request(
            "https://operosus.app.n8n.cloud/webhook/vivify-group-enrich",
            data=json.dumps({"search_id": sid}).encode(),
            headers={"Content-Type": "application/json"}), timeout=30)
        print("  enrichment triggered")
    except Exception as e:
        sys.stderr.write(f"enrich trigger err {e}\n")
    set_status(sid, 'complete')
    print(f"  done — status complete")

if __name__ == '__main__':
    run(int(sys.argv[1]))
