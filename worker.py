#!/usr/bin/env python3
"""Vivify venue-hirers discovery WORKER (Python replacement for the n8n flow).
Outcome: find groups that demonstrably book/hire a SPECIFIC venue, evidence required.

Pipeline: cache check -> discover (web search + read pages + 1-level crawl to "where we meet" pages,
charity register by postcode, exact-postcode DB, Facebook posts) -> LLM gate (real org + venue tie)
-> write via process_venue_hirer_results RPC -> status complete.

Run:  python3 worker.py <search_id>
"""
import os, re, json, base64, sys, html, time, faulthandler, multiprocessing as mp
import concurrent.futures as cf, urllib.request, urllib.parse

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
GPLACES = ENV.get('GOOGLE_PLACES_KEY', '')
import functools
print = functools.partial(print, flush=True)

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
    'netmums','trip.com','klook','viator','timeout.com','nhs.uk','heyschools','heygolf','schoolratings','schoolsfootball','schoolsnetball','schoolsbasketball',
    'allschools','schoolstogether','schoolowl','goodschools','locatethis','cleanair','primarytimes',
    # Nothing here has ever been a venue hirer, and every one of them is a page we pay to fetch. They
    # came off the slowest-page log on 10 August: usnews.com alone held a fetch open for 1,335 seconds
    # and pegged the instance for 22 minutes, and books.google, alamy, niche and bloomberg were sitting
    # in the same 200. US school directories arrive because "St Mary's Catholic High School" is a very
    # common name.
    'usnews.com','niche.com','greatschools','publicschoolreview','books.google','scholar.google','alamy',
    'shutterstock','istockphoto','dreamstime','bloomberg','thetimes','telegraph.co.uk','dailymail',
    'issuu.com','scribd','academia.edu','researchgate','jstor','bayut','arabiancampus','edarabia',
    'greater.jobs','jobsgopublic','catholicrecruitment','wigantoday','thisislocallondon','standard.co.uk',
    'expertini','glassdoor','ziprecruiter','trustpilot','yelp.com','crunchbase','bizapedia','opencorporates']
AGG = ['charitycommission','findachurch','classforkids','pitchfinder','clubspark','playfootball','happity',
       'footyaddicts','hoop.co.uk','eventbrite','meetup','allevents','skiddle','ticketsource','fatsoma',
       'dice.fm','eventful','tickettailor','trybooking','bookwhen']
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
    r'^results?$', r'^news$', r'leggings', r'^\d', r'^map of', r'^area information', r'postcode s',
    r"^(baby|toddler|kids|children'?s) .* classes", r'^classes (in|near)', r'^things to do',
    # session descriptions and age groups are not organisations
    r'^u\d{1,2}\b', r'^(under|year) \d', r'^\w+ (schools?|clubs?|classes) in ', r'training$', r'^(junior|senior|adult)s? ',
    r'^(winter|summer|spring|autumn) ', r'^(half.term|holiday) ', r'^book ', r'^join ', r'^register ',
    r'showcase$', r'\bshowcase\b', r'open day', r'presentation (evening|night)', r'fun day', r'taster session',
    r'(christmas|easter|summer) (show|fair|fayre|party)',
    # another mainstream school is a venue, not a hirer (supplementary/faith schools don't use these words)
    r'\b(high school|primary school|infant school|junior school|grammar school|voluntary academy|academy trust|sixth form)\b',
    # match reports and page headlines, not organisations
    r'\d+\s*-\s*\d+', r'^club matches$', r'^(fixtures?|matches|results|tables?|standings)$',
    r'\b(now offer|now offers|are pleased|is pleased|welcomes?|announce)\b',
    r'^(martial arts|dance|football|netball|cricket|tennis|badminton|gymnastics) (clubs?|classes|schools?) ']]
# A page title that is just the activity ("Netball") names no organisation — the hirer is unidentifiable.
# An activity is not an organisation. Vivify have to be able to ring a named club, so a result called
# "Zumba" is worthless to them even when the evidence genuinely names the venue — which it did on Luke's
# Blenheim search (search 189), where a real Zumba class at Blenheim High School surfaced under the bare
# brand name and read to him as a bug. Branded formats (zumba, clubbercise, boxercise) belong here for the
# same reason the generic ones do. Full-name match only, so "Revolution Martial Arts" is unaffected.
BARE_ACTIVITY = {'netball','football','basketball','cricket','tennis','badminton','dance','ballet','gymnastics',
                 'karate','yoga','pilates','drama','music','tuition','classes','clubs','camps','training',
                 'holiday camps','football training','sports','fitness','swimming','athletics','rugby','hockey',
                 'zumba','clubbercise','boxercise','bootcamp','boot camp','spin','spinning','aerobics','boxing',
                 'kickboxing','taekwondo','judo','jiu jitsu','martial arts','cheerleading','trampolining',
                 'volleyball','table tennis','squash','archery','fencing','street dance','musical theatre',
                 'after school club','after school clubs','holiday club','holiday clubs','walking football',
                 'sports coaching','multi sports','multi-sports','soft play','toddler group','youth club'}
# A supplementary, faith or activity school hires halls; a mainstream school is somebody else's venue.
# These tokens are what tells the two apart, since both are "... School".
SUPPLEMENTARY = re.compile(r'\b(tamil|persian|farsi|german|french|spanish|polish|greek|chinese|mandarin|arabic|'
    r'urdu|somali|turkish|russian|japanese|korean|saturday|sunday|supplementary|madrasah|madrasa|quran|islamic|'
    r'hebrew|torah|sikh|hindu|church|christian|catholic mission|dance|ballet|drama|stage|theatre|performing|'
    r'music|singing|maths|tuition|tutor|language|driving|swim|football|martial|karate|judo|gymnastic|circus|'
    r'forest|montessori|nursery|preschool|pre-school|holiday|coding|chess|art)\b', re.I)
EDU_SIGNAL = re.compile(r'\b(high|primary|junior|infant|secondary|grammar|comprehensive|voluntary|'
                        r'church of england|c of e|catholic|sixth form|academy trust|free school)\b', re.I)
def mainstream_school(n):
    """"Academy" alone is not a school — PSG Academy UK is a football coaching business and a real hirer.
    Treat it as a school only when the name says "school"/"college", or pairs "academy" with an
    education word (voluntary, catholic, grammar, high...)."""
    if SUPPLEMENTARY.search(n): return False
    if re.search(r'\b(school|college)\b', n, re.I): return True
    return bool(re.search(r'\bacademy\b', n, re.I) and EDU_SIGNAL.search(n))

# Facebook authors are often just people ("Wendy Chalmers"), not the group hiring the hall. A person's
# name is only a lead if they are visibly trading, which shows up as an organisation word in the name.
FIRST_NAMES = {
 'anna','eliza','wendy','sarah','claire','clare','emma','laura','lisa','karen','joanne','joanna','helen','julie',
 'rachel','rebecca','becky','hannah','kate','katie','katherine','catherine','elizabeth','charlotte','sophie','amy',
 'jessica','jess','lucy','olivia','emily','ellie','holly','megan','natalie','nicola','michelle','melanie','donna',
 'tracy','tracey','sharon','susan','sue','jane','janet','angela','angie','amanda','mandy','deborah','debbie','gemma',
 'jenny','jennifer','louise','lauren','leanne','kelly','stacey','danielle','chloe','abbie','abigail','georgia','grace',
 'john','david','dave','michael','mike','james','jim','paul','peter','pete','andrew','andy','mark','stephen','steven',
 'steve','chris','christopher','daniel','dan','matthew','matt','richard','rich','robert','rob','thomas','tom','anthony',
 'tony','gary','simon','martin','ian','alan','kevin','keith','neil','graham','philip','phil','adam','ben','benjamin',
 'jack','jake','josh','joshua','luke','ryan','sam','samuel','scott','sean','shaun','stuart','craig','carl','wayne',
 'darren','dean','glenn','lee','liam','nathan','oliver','owen','ross','shane','terry','trevor','vincent','warren',
 'aisha','fatima','mohammed','muhammad','ahmed','ali','omar','hassan','priya','raj','anita','sanjay','laural','shaz'}
ORG_WORDS = re.compile(r'\b(club|fc|afc|utd|united|academy|school|college|church|chapel|centre|center|studio|'
    r'society|association|assoc|group|trust|foundation|ltd|limited|cic|c\.i\.c|charity|team|league|coaching|'
    r'coach|classes|class|lessons|tuition|dance|arts|theatre|gym|gymnastics|martial|karate|judo|fitness|yoga|'
    r'pilates|scouts|guides|brownies|cubs|nursery|preschool|playgroup|toddler|kickers|tots|stars|sports|athletic|'
    r'cricket|netball|football|basketball|rugby|hockey|tennis|badminton|swim|music|choir|band|orchestra|drama)\b', re.I)
def personal_name(n):
    w = [x for x in re.split(r'\s+', (n or '').strip()) if x]
    if not (2 <= len(w) <= 3) or ORG_WORDS.search(n): return False
    if not all(re.fullmatch(r"[A-Za-z'’\-]+", x) for x in w): return False
    return w[0].lower() in FIRST_NAMES

def is_junk(name):
    n = (name or '').strip()
    if personal_name(n): return True
    if len(n) < 3 or not re.search(r'[a-z]', n, re.I): return True
    if n.lower() in BARE_ACTIVITY: return True
    if mainstream_school(n): return True
    if any(r.search(n) for r in JUNK_RE): return True
    w = n.split(); caps = sum(1 for x in w if x[:1].isupper() or x[:1].isdigit())
    return len(w) >= 6 and caps <= 1

UKPC_RE = re.compile(r'\b([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})\b', re.I)
GOOD_TYPES = ('school', 'primary_school', 'secondary_school', 'university', 'church', 'place_of_worship',
              'stadium', 'gym', 'community_center', 'establishment', 'point_of_interest')
AREA_TYPES = ('locality', 'sublocality', 'sublocality_level_1', 'neighborhood', 'postal_code', 'postal_town',
              'political', 'route', 'administrative_area_level_1', 'administrative_area_level_2')

# A trailing parenthetical is the user's own label, not part of the venue's name. Luke types
# "Surbiton High School (Raynes Park)" to note which Vivify site he is comparing against, and left in
# it poisons the whole search: 12 of the 49 queries become quoted searches for a phrase that appears
# nowhere on the web, the venue-name tie can never match a page that names the school (only the
# postcode ties), and the gate is told to reject anything that is not at "Raynes Park". Search the
# venue, keep the label for display.
VENUE_LABEL = re.compile(r'\s*\(([^()]{1,40})\)\s*$')
def split_venue_label(v):
    """("Surbiton High School (Raynes Park)") -> ("Surbiton High School", "Raynes Park")."""
    v = (v or '').strip()
    m = VENUE_LABEL.search(v)
    if not m: return v, ''
    base = VENUE_LABEL.sub('', v).strip()
    # A name that is nothing but a parenthetical is not a label, it is the name.
    return (base, m.group(1).strip()) if len(base) >= 4 else (v, '')

def resolve_venue(venue, pc):
    """Canonicalise the typed venue via Google Places text search.
    Users type an AREA as often as a venue ("St John's Wood" for Harris Academy St John's Wood); every
    downstream query keys on the venue NAME, so an area name gives area-wide noise. Places + the postcode
    resolves it to the actual establishment. Returns (name, postcode, own_domain)."""
    if not GPLACES: return venue, pc, ''
    def lookup(q):
        body = json.dumps({"textQuery": q, "maxResultCount": 5, "regionCode": "GB", "languageCode": "en"}).encode()
        try:
            r = urllib.request.Request("https://places.googleapis.com/v1/places:searchText", data=body,
                headers={"Content-Type": "application/json", "X-Goog-Api-Key": GPLACES,
                         "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.types,places.websiteUri"})
            return json.load(urllib.request.urlopen(r, timeout=25)).get('places', [])
        except Exception as e:
            sys.stderr.write(f"places err {e}\n"); return []
    pcol, oc = collapse(pc), outcode(pc)
    def score(p):
        addr = collapse(p.get('formattedAddress', '')); t = p.get('types') or []
        s = 0
        if pcol and pcol in addr: s += 4
        elif oc and oc in addr: s += 1
        if any(x in t for x in GOOD_TYPES): s += 2
        if any(x in t for x in AREA_TYPES): s -= 5
        return s
    # Query ladder: the typed text first; if that only lands on an area/postcode (users type "St John's Wood"
    # as often as the school name), ask Places for the establishment AT that postcode instead.
    best = None
    for q in [f"{venue} {pc}".strip()] + ([f"school {pc}", f"venue {pc}"] if pc else []):
        places = lookup(q)
        cand = max(places, key=score) if places else None
        if cand and score(cand) > 0: best = cand; break
    if not best: return venue, pc, ''
    name = ((best.get('displayName') or {}).get('text') or venue).strip()
    # Never downgrade to a vaguer name. Google lists Harrytown Catholic High School as plain "Harrytown"
    # (the road), and searching for "Harrytown" drags in every other school that mentions the street.
    # Upgrades are fine and are the whole point ("St John's Wood" -> "Harris Academy St John's Wood").
    if collapse(name) and collapse(name) in collapse(venue) and len(name) < len(venue):
        name = venue
    m = UKPC_RE.search(best.get('formattedAddress') or '')
    newpc = f"{m.group(1)} {m.group(2)}".upper() if m else pc
    if pc and collapse(newpc)[:len(oc)] != oc: newpc = pc  # different outcode = wrong place, keep what was typed
    own = urllib.parse.urlparse(best.get('websiteUri') or '').netloc.lower().replace('www.', '')
    return name, (newpc or pc), own

LOCATION_TAIL = re.compile(r'\s+(in|at|near)\s+[A-Z][\w\'\-]*(,[^,]{0,40})*\s*(,?\s*[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})?\s*$', re.I)
def tidy_name(n):
    """"Revolution Martial Arts in Marple, Stockport SK6 6LB" is the club plus a location tail scraped
    from the page title. Vivify wants the club."""
    out = LOCATION_TAIL.sub('', (n or '').strip()).strip(' ,-|')
    return out if len(out) >= 3 else (n or '').strip()

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
            cost = float(task.get('cost', 0) or 0)
            res = task.get('result') or []
            if not res:  # query ran and returned nothing — a retry just pays for the same empty answer
                return [], cost
            items = [{"url": i.get('url'), "title": i.get('title') or '', "snippet": i.get('description') or ''}
                     for i in (res[0].get('items') or []) if i.get('type') == 'organic' and i.get('url')]
            return items, cost
        except Exception as e:
            if attempt == retries: sys.stderr.write(f"dfs err [{kw[:30]}]: {e}\n"); return [], 0.0
            time.sleep(1.5)

def dfs_balance():
    """Remaining DataForSEO credit, or None if it cannot be read.

    Worth its own call because of how this fails otherwise. On 7 August the account hit zero and every
    query returned 402 Payment Required. dfs() logs that to stderr and returns an empty list, so a search
    ran to completion, found almost nothing, and reported itself complete. To Vivify that is
    indistinguishable from a venue with no hirers, and nothing anywhere said the reason was money."""
    try:
        req = urllib.request.Request("https://api.dataforseo.com/v3/appendix/user_data",
                                     headers={"Authorization": "Basic " + DFS_AUTH})
        d = json.load(urllib.request.urlopen(req, timeout=20))
        return float(((d.get('tasks') or [{}])[0].get('result') or [{}])[0]['money']['balance'])
    except Exception:
        return None

class _FewRedirects(urllib.request.HTTPRedirectHandler):
    """The socket timeout is per hop, so a ten-hop redirect chain costs ten times what it looks like.
    Pages took 116 to 140 seconds each on the Wimbledon and St Mary's runs against an 8 second timeout;
    three hops is plenty for a club website."""
    max_redirections = 3
    max_repeats = 2
PAGE_OPENER = urllib.request.build_opener(_FewRedirects)

def fetch(url, timeout=8, budget=15):
    """One slow site should not hold up the search: a tight timeout plus a read cap means the worst
    case per page is bounded. (Page reading swung between 46s and 289s a run before this.)

    The socket timeout bounds each individual recv, not the page. A host that trickles bytes, or a
    chain of redirects each getting its own eight seconds, sails past it: reading 200 real club sites
    took 1,201 seconds on 10 August against 22-42 seconds for the aggregator pages the broken venue
    name had been returning. The read now gets a wall-clock budget of its own."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
        with PAGE_OPENER.open(req, timeout=timeout) as r:
            ctype = (r.headers.get('Content-Type') or '').lower()
            if ctype and 'html' not in ctype and 'text' not in ctype:
                return url, ''  # PDFs and images cost time and never carry the evidence we want
            deadline, out, got = time.time() + budget, [], 0
            while got < 250000:
                chunk = r.read(65536)
                if not chunk: break
                out.append(chunk); got += len(chunk)
                if time.time() > deadline: break  # keep what arrived, stop waiting for the rest
            return url, b''.join(out).decode('utf-8', 'ignore')
    except Exception:
        return url, ''

def strip_markup(raw):
    """Scripts and styles out, tags out, entities decoded. This is the expensive pass over a page and
    every caller that wants the words rather than the markup should do it once and pass the result on:
    a tied page used to run it three times over the same quarter of a megabyte."""
    return re.sub(r'\s+', ' ', html.unescape(re.sub(r'(?is)<[^>]+>', ' ',
        re.sub(r'(?is)<(script|style|noscript|svg)[^>]*>.*?</\1>', ' ', raw or '')))).strip()

def page_blob(raw, snippet, title, text=None):
    return collapse(title + ' ' + snippet + ' ' + (strip_markup(raw) if text is None else text))

def page_text(raw):
    """Readable text, for showing a page to the model. page_blob() collapses everything for substring
    matching, which is unreadable."""
    return strip_markup(raw)

PROOF_STRONG = re.compile(r'\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|weekly|every|'
                          r'\d{1,2}(?:[:.]\d{2})?\s*(?:am|pm)|\d{1,2}\s*-\s*\d{1,2}\s*(?:am|pm))\b', re.I)
PROOF_VERB = re.compile(r'\b(meet|meets|meeting|train|trains|training|held|hold|holds|run|runs|running|'
                        r'play|plays|based|hire|hires|session|sessions|class|classes|practice|rehears)\w*\b', re.I)

def _sentences(text):
    return [s.strip() for s in re.split(r'(?<=[.!?])\s+|\s*\|\s*|\s+•\s+', text) if s.strip()]

def _navish(s):
    """A menu reads as many short capitalised words and almost no ordinary sentence structure."""
    words = s.split()
    if len(words) < 6: return False
    capped = sum(1 for w in words if w[:1].isupper())
    return capped / len(words) > 0.6 and not PROOF_VERB.search(s)

def page_evidence(raw, venue, pc, text=None):
    """The sentence that PROVES this organisation uses this venue, then context around it.

    The old version returned a fixed window either side of the venue mention, which on a page whose
    navigation happens to list the venue gave Vivify a menu: "Foundation 3G Pitch Register Football
    Foundation Pitch Finder Online Ticketing Contact Complaints Board Staff". Compare "Wednesday evening
    7-8.30pm at Blenheim High school in Epsom". Both are true, only one lets somebody pick up the phone
    and say why they are calling, and a lead Vivify cannot prove is not a lead they will use."""
    text = strip_markup(raw) if text is None else text
    if not text: return ''
    needles = [n for n in (pc.lower(), venue.lower().split(',')[0][:18]) if n]

    # The window, exactly as before. This is the floor: the card never gets less than it used to.
    context = ''
    low = text.lower()
    for needle in needles:
        i = low.find(needle)
        if i >= 0:
            s, e = max(0, i - 600), min(len(text), i + 900)
            context = ('...' if s > 0 else '') + text[s:e].strip() + ('...' if e < len(text) else '')
            break
    if not context:
        return ''

    # If a sentence in that window actually states the thing (a day, a time, "we train at"), lead with it.
    # Plenty of club sites are pure navigation and have no sentence at all, which is why this only ever
    # adds a line rather than replacing the passage.
    best, best_score = '', 0
    for s in _sentences(context):
        if not any(x in s.lower() for x in needles) or _navish(s) or len(s) < 25:
            continue
        sc = (2 if PROOF_STRONG.search(s) else 0) + (1 if PROOF_VERB.search(s) else 0)
        if sc > best_score:
            best, best_score = s, sc
    if best and not context.lstrip('. ').startswith(best[:40]):
        return f"{best}\n\n{context}"[:1800]
    return context[:1500]

DATE_META = [r'(?is)<meta[^>]+property=["\']article:(?:published|modified)_time["\'][^>]+content=["\']([^"\']+)',
             r'(?is)<meta[^>]+itemprop=["\']date(?:Published|Modified)["\'][^>]+content=["\']([^"\']+)',
             r'(?is)"date(?:Published|Modified)"\s*:\s*"([^"]+)"',
             r'(?is)<time[^>]+datetime=["\']([^"\']+)']
def page_date(raw):
    """When the page says it was published — an old fixture list is a weaker lead than a live timetable."""
    for pat in DATE_META:
        m = re.search(pat, raw or '')
        if m:
            d = m.group(1).strip()
            if re.match(r'^\d{4}-\d{2}-\d{2}', d): return d[:10]
    return None

def og_image(raw, base):
    """The page's own share image — the closest thing to a screenshot we can capture without a browser."""
    for pat in [r'(?is)<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)',
                r'(?is)<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)']:
        m = re.search(pat, raw or '')
        if m:
            u = html.unescape(m.group(1)).strip()
            if u.startswith('//'): u = 'https:' + u
            elif u.startswith('/'): u = urllib.parse.urljoin(base, u)
            if u.startswith('http'): return u[:500]
    return None

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

ASSET_EXT = ('.css', '.js', '.png', '.jpg', '.jpeg', '.svg', '.gif', '.webp', '.ico', '.woff', '.woff2', '.ttf', '.mp4', '.pdf', '.json', '.xml')
EMAIL_BADDOM = ('sentry', 'wixpress', 'example.', 'schema.org', 'w3.org', 'googleapis', 'gstatic', 'cloudflare',
                'jquery', 'bootstrap', 'fontawesome', '.min.', 'domain.com', 'email.com', 'yourdomain', 'sentry.io', 'gov.uk')
EMAIL_RE = re.compile(r'[a-z0-9][a-z0-9._%+\-]*@[a-z0-9.\-]+\.[a-z]{2,}', re.I)
PHONE_RE = re.compile(r'(?:\+44\s?|\b0)(?:\d[\d\s().\-]{7,12}\d)')
FREEMAIL = {'gmail.com','googlemail.com','hotmail.com','hotmail.co.uk','outlook.com','yahoo.com','yahoo.co.uk',
            'btinternet.com','live.co.uk','icloud.com','me.com','aol.com','sky.com','virginmedia.com'}
PREF_LOCAL = ('info', 'hello', 'contact', 'enquiries', 'enquiry', 'admin', 'office', 'hi', 'team', 'bookings', 'reception')
PLACEHOLDER_PHONES = {'01234567890', '02012345678', '07123456789', '00000000000', '01111111111', '07000000000', '07700900000'}

# The last two labels are NOT the registrable domain under a multi-part suffix: "troynetballclub.co.uk"
# reduced to "co.uk", so every .co.uk domain compared equal to every other .co.uk domain. That silently
# defeated the own-domain test in contacts() — an email at any unrelated .co.uk address on a page counted
# as the organisation's own — and the aggregator-brand check in discover_web(), which read the brand as
# "co". UK suffixes cover what this database contains; the rest fall back to two labels.
MULTI_SUFFIX = {'co.uk','org.uk','ac.uk','gov.uk','net.uk','sch.uk','me.uk','ltd.uk','plc.uk','nhs.uk',
                'police.uk','mod.uk','org.au','com.au','co.nz','co.za','com.pl','co.in'}
def _registrable(dom):
    parts = [p for p in (dom or '').lower().strip('.').split('.') if p]
    if len(parts) >= 3 and '.'.join(parts[-2:]) in MULTI_SUFFIX:
        return '.'.join(parts[-3:])
    return '.'.join(parts[-2:])

OBF_RE = re.compile(r'([a-z0-9._%+\-]{2,})\s*(?:\[\s*(?:at|@)\s*\]|\(\s*(?:at|@)\s*\)|\s+at\s+)\s*'
                    r'([a-z0-9\-]+(?:\s*(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\s+dot\s+|\.)\s*[a-z0-9\-]+)+)', re.I)
def deobfuscate(text):
    """info [at] example [dot] org — small orgs hide their address this way and we were missing them."""
    out = []
    for local, dom in OBF_RE.findall(text):
        d = re.sub(r'\s*(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\s+dot\s+)\s*', '.', dom, flags=re.I)
        d = re.sub(r'\s+', '', d)
        if d.count('.') >= 1 and len(d.split('.')[-1]) >= 2 and re.fullmatch(r'[a-z0-9.\-]+', d, re.I):
            out.append(f"{local.lower()}@{d.lower()}")
    return out

def contacts(raw, org_domain):
    """Pull a VALIDATED email + phone from an org's own page. Rejects asset strings, placeholders, junk.
    mailto:/tel: links are trusted first — they are what the org actually publishes as its contact."""
    if not raw: return None, None
    text = html.unescape(raw)
    linked = [m.lower().split('?')[0].strip() for m in re.findall(r'(?i)href=["\']mailto:([^"\'?>]+)', text)]
    # email
    email = None
    cands = []
    for m in linked + EMAIL_RE.findall(text) + deobfuscate(text):
        e = m.lower().strip('.')
        if e.count('@') != 1 or not EMAIL_RE.fullmatch(e): continue
        dom = e.split('@')[1]
        if any(e.endswith(x) or ('.' + x.strip('.')) in dom for x in ASSET_EXT): continue
        if dom.endswith(ASSET_EXT): continue
        if any(b in e for b in EMAIL_BADDOM): continue
        if dom.count('.') == 0 or len(dom.split('.')[-1]) < 2: continue
        if re.search(r'\d{4,}', e.split('@')[0]): continue  # filename-ish local part
        cands.append(e)
    if cands:
        org_reg = _registrable(org_domain)
        own = [e for e in cands if _registrable(e.split('@')[1]) == org_reg]
        # A page often lists OTHER organisations' addresses (a surgery listing local groups, a directory
        # entry). Only the org's own domain or a free mailbox on its own site can be trusted as its contact.
        free = [e for e in cands if _registrable(e.split('@')[1]) in FREEMAIL]
        pool = own or free
        pref = [e for e in pool if e.split('@')[0] in PREF_LOCAL]
        email = (pref[0] if pref else (pool[0] if pool else None))
    # phone (UK) — tel: links first, then anything phone-shaped in the text
    phone = None
    tel = [re.sub(r'[^\d+]', '', m) for m in re.findall(r'(?i)href=["\']tel:([^"\'>]+)', text)]
    for m in tel + PHONE_RE.findall(text):
        d = re.sub(r'\D', '', m)
        if d.startswith('44'): d = '0' + d[2:]
        if len(d) not in (10, 11) or not d.startswith('0'): continue
        if d[1] not in '12378': continue
        # Match valid_uk_phone in the database, or we write numbers it then calls invalid. 10-digit
        # numbers are real for 01 areas (01297 35800, Lyme Regis) and for 0800 freephone (0800 838909),
        # which plenty of small providers publish as their only number. A 10-digit 03 or 07 is
        # malformed, and "0341722133" was about to be stored off a live page.
        if len(d) == 10 and d[1] not in '18': continue
        if d[1] == '2' and d[2] not in '03489': continue
        if d[1] == '7' and d[2] not in '12345789': continue
        if d in PLACEHOLDER_PHONES or len(set(d)) <= 2: continue
        if d in '0123456789012345' or d in '0987654321098765': continue  # sequential
        phone = d; break
    return email, phone

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
EXCLUDE_SITES = ['tes.com','eteach.com','indeed.com','rightmove.co.uk','theguardian.com','linkedin.com',
                 'locrating.com','schoolsweek.co.uk']
def queries_for(venue, pc, own=''):
    # Exclude the venue's OWN domain + the big job/property/news sites from venue-name queries: the school's
    # own pages otherwise fill the ranking and the hirers (who name the venue on their own sites) never surface.
    ex = ' '.join(f'-site:{d}' for d in ([own] if own else []) + EXCLUDE_SITES)
    acts = ['football','netball','basketball','cricket','gymnastics','dance','ballet','karate','martial arts','tuition',
            'language school','saturday school','supplementary school','madrasah','quran','persian school','tamil school',
            'german saturday school','korean church','church','scouts','toddler group','holiday camp','music lessons',
            'drama','classes','club','academy','timetable']
    q = [f'{venue} {ex}', f'{venue} {pc} {ex}'] + [f'{a} {venue} {ex}' for a in acts]
    q += [f'{a} "{venue}" {ex}' for a in ['madrasah','tamil school','persian school','german saturday school','church','football','dance']]
    q += [f'"{pc}" charity', f'"{pc}" church', f'"{pc}" club', f'"{pc}" academy',
          f'site:register-of-charities.charitycommission.gov.uk "{pc}"', f'site:findachurch.co.uk "{pc}"']
    q += [f'site:{d} "{venue}"' for d in ['classforkids.io','happity.co.uk','playfootball.net','clubspark.lta.org.uk','pitchfinder.org.uk']]
    return q

PAGE_WORKERS = int(ENV.get('PAGE_WORKERS', '24'))
PAGE_BUDGET = int(ENV.get('PAGE_BUDGET', '300'))    # seconds for the whole page phase
CRAWL_BUDGET = int(ENV.get('CRAWL_BUDGET', '120'))  # and for the sub-page crawl
SEARCH_WALL = int(ENV.get('SEARCH_WALL', '900'))    # hard stop for a whole search, GIL or no GIL
FETCH_PROCS = int(ENV.get('FETCH_PROCS', '2'))      # child processes reading pages
PAGE_INFLIGHT = int(ENV.get('PAGE_INFLIGHT', '24'))   # pages a child will have open at once

def _page_rec(c, vmeta, venue, pc):
    """Fetch one candidate page and reduce it to the small record we keep. The HTML is dropped here and
    never leaves this function — holding a few hundred pages at once OOM-killed the 512MB instance."""
    t_f = time.time()
    _, raw = fetch(c['url'])
    t_p = time.time()
    text = strip_markup(raw)
    vtoks, oc = vmeta[2], vmeta[3]
    tie = tie_kind(page_blob(raw, c['snippet'], c['title'], text), *vmeta)
    rec = {'domain': c['domain'], 'title': c['title'], 'snippet': c['snippet'], 'url': c['url'], 'tie': tie}
    if tie:
        rec['sitename'] = site_name(raw); rec['evidence'] = page_evidence(raw, venue, pc, text)
        rec['evidence_date'] = page_date(raw); rec['image'] = og_image(raw, c['url'])
        # own site OR a charity/church register page (those list the org's own contact) — not class/sport directories
        if not is_agg(c['domain']) or 'charitycommission' in c['domain'] or 'findachurch' in c['domain']:
            rec['email'], rec['phone'] = contacts(raw, c['domain'])
            if not is_agg(c['domain']) and not rec.get('email') and not rec.get('phone'):
                rec['clinks'] = sublinks(raw, c['url'], c['domain'], ['contact', 'about'])
    else:
        rec['links'] = sublinks(raw, c['url'], c['domain'], list(vtoks) + [oc])
    rec['_t'] = (t_p - t_f, time.time() - t_p, len(raw))
    return rec

def _page_stream(cands, vmeta, venue, pc, q):
    """Runs in a CHILD process and posts each page back the moment it is done. Streaming rather than
    returning a batch matters: whatever a child has already sent survives being killed. The first
    version of this returned a chunk of ten at a time and lost the whole chunk to any one slow host in
    it — St Mary's read 10 pages of 200 that way."""
    # 24 open at a time. Opening all 200 at once was tried, on the theory that hung hosts were holding
    # every slot, and it read NOTHING at all: two hundred TLS handshakes on half a CPU finish none of
    # them inside the budget. Twenty-four is the figure that reads 200 pages in 38 seconds on a venue
    # whose hosts answer.
    ex = cf.ThreadPoolExecutor(max_workers=max(1, min(len(cands), PAGE_INFLIGHT)))
    for f in cf.as_completed([ex.submit(_page_rec, c, vmeta, venue, pc) for c in cands]):
        try: q.put(f.result())
        except Exception: pass
    q.put(None)

def _contact_stream(todo, _vmeta, _venue, _pc, q):
    """Same shape for the contact chase: (i, domain, url) in, (i, email, phone) out."""
    def one(t):
        i, dom, su = t
        return (i, *contacts(fetch(su)[1], dom))
    ex = cf.ThreadPoolExecutor(max_workers=max(1, min(len(todo), PAGE_INFLIGHT)))
    for f in cf.as_completed([ex.submit(one, t) for t in todo]):
        try: q.put(f.result())
        except Exception: pass
    q.put(None)

def in_children(items, fn, budget, vmeta=None, venue='', pc=''):
    """Do the fetch work in child processes and take whatever they have sent by the deadline.

    Every deadline that lived inside the search process failed, because a wedged fetch leaves the
    interpreter unable to run the code that is supposed to give up on it: St Mary's had a 300-second
    page budget and came back after 1,197, three times, losing the whole search each time. The parent
    here holds the clock and never touches a socket, so nothing a child does can stop it giving up, and
    SIGTERM needs no cooperation from a blocked read."""
    if not items: return []
    ctx = mp.get_context('fork')
    q = ctx.Queue()
    parts = [p for p in (items[i::FETCH_PROCS] for i in range(FETCH_PROCS)) if p]
    procs = [ctx.Process(target=fn, args=(part, vmeta, venue, pc, q), daemon=True) for part in parts]
    for p in procs: p.start()
    got, finished, deadline = [], 0, time.time() + budget
    while finished < len(procs):
        left = deadline - time.time()
        if left <= 0: break
        try: item = q.get(timeout=left)
        except Exception: break          # empty at the deadline
        if item is None: finished += 1
        else: got.append(item)
    for p in procs:
        if p.is_alive(): p.terminate()
        p.join(5)
    return got

def read_pages(cands, vmeta, venue, pc, budget=None):
    """Candidate pages, read in children that can be killed. Returns (records, dropped domains, timings)."""
    if not cands: return [], set(), []
    recs = in_children(cands, _page_stream, budget or PAGE_BUDGET, vmeta, venue, pc)
    seen_urls = {r['url'] for r in recs}
    timings = [(*r.pop('_t'), r['domain']) for r in recs if '_t' in r]
    dropped = {c['domain'] for c in cands if c['url'] not in seen_urls}
    return recs, dropped, timings

def tie_kind(blob, vcol, pcol, vtoks, oc):
    if pcol and pcol in blob: return 'postcode'
    if vcol and len(vcol) > 6 and vcol in blob: return 'venue'
    if len(vtoks) >= 2 and oc and oc in blob and all(t in blob for t in vtoks): return 'venue'
    return None

def discover_web(venue, pc, own=''):
    vcol, pcol, vtoks, oc = collapse(venue), collapse(pc), vtokens(venue), outcode(pc)
    ownreg = _registrable(own) if own else ''
    raw, seen, cost = [], set(), 0.0
    t0 = time.time()
    qs = queries_for(venue, pc, own)
    # 4 at a time: DataForSEO throttles heavier parallelism, and a throttled query still costs on retry
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        answers = list(ex.map(dfs, qs))
    print(f"  serp: {len(qs)} queries in {time.time()-t0:.0f}s")
    for items, qcost in answers:
        cost += qcost
        for r in items:
            if r['url'] in seen: continue
            seen.add(r['url']); r['domain'] = urllib.parse.urlparse(r['url']).netloc.lower().replace('www.', '')
            raw.append(r)
    cands = [r for r in raw if not noisy(r['domain']) and 'facebook.com' not in r['domain']
             and not (ownreg and _registrable(r['domain']) == ownreg)]
    cands.sort(key=lambda c: ('charitycommission' in c['domain']) or (pcol in collapse(c['title']+c['snippet'])) or (vcol in collapse(c['title']+c['snippet'])), reverse=True)
    cands = cands[:200]
    vmeta = (vcol, pcol, vtoks, oc)
    t1 = time.time()
    results, dropped, timings = read_pages(cands, vmeta, venue, pc)
    print(f"  pages: {len(results)} of {len(cands)} read in {time.time()-t1:.0f}s")
    if dropped:
        print(f"  pages: gave up on {len(dropped)} — {', '.join(sorted(dropped)[:10])}")
    if timings:
        slow = sorted(timings, reverse=True)[:5]
        print(f"  page split: fetch {sum(t[0] for t in timings):.0f}s + parse {sum(t[1] for t in timings):.0f}s "
              f"summed over {len(timings)} pages, {sum(t[2] for t in timings)/1e6:.1f}MB")
        print("  slowest: " + " | ".join(f"{d} f{a:.0f}s p{b:.0f}s {c//1024}KB" for a, b, c, d in slow))
    byd, agg = {}, []
    for r in results:
        if not r['tie']: continue
        if is_agg(r['domain']):
            agg.append({'name': brand([r['title']], r['domain']), 'domain': r['domain'], 'tie': r['tie'],
                        'url': r['url'], 'snippet': r['snippet'], 'evidence': r.get('evidence', ''),
                        'evidence_date': r.get('evidence_date'), 'image': r.get('image'),
                        'email': r.get('email'), 'phone': r.get('phone')})
        else:
            d = byd.setdefault(r['domain'], {'titles': [], 'tie': r['tie'], 'url': r['url'],
                                             'snippet': r['snippet'], 'sitename': r.get('sitename', ''), 'evidence': r.get('evidence', ''),
                                             'evidence_date': r.get('evidence_date'), 'image': r.get('image'),
                                             'email': r.get('email'), 'phone': r.get('phone'), 'clinks': r.get('clinks', [])})
            d['titles'].append(r['title'])
            if not d.get('sitename'): d['sitename'] = r.get('sitename', '')
            if not d.get('email'): d['email'] = r.get('email')
            if not d.get('phone'): d['phone'] = r.get('phone')
            if not d.get('clinks'): d['clinks'] = r.get('clinks', [])
            if r['tie'] == 'postcode': d['tie'] = 'postcode'
    # 1-level crawl: org pages whose venue mention is on a sub-page ("where we meet"/timetable)
    targets = []
    for r in results:
        if r['tie'] or r['domain'] in byd or is_agg(r['domain']): continue
        for su in r.get('links', []): targets.append((r['domain'], r['title'], su))
    targets = targets[:120]
    t2 = time.time()
    if targets:
        # The crawl fetches arbitrary pages too, so it goes through the same child processes. A sub-page
        # is just a candidate whose domain is the organisation's rather than the sub-page's own.
        subs = [{'url': su, 'domain': dom, 'title': title, 'snippet': ''} for dom, title, su in targets]
        srecs, sdropped, _ = read_pages(subs, vmeta, venue, pc, budget=CRAWL_BUDGET)
        for r in srecs:
            dom = r['domain']
            if r['tie'] and dom not in byd:
                byd[dom] = {'titles': [r['title']], 'tie': r['tie'], 'url': r['url'], 'snippet': '',
                            'sitename': r.get('sitename', ''), 'evidence': r.get('evidence', ''),
                            'evidence_date': r.get('evidence_date'), 'image': r.get('image'),
                            'email': r.get('email'), 'phone': r.get('phone')}
        if sdropped: print(f"  crawl: gave up on {len(subs) - len(srecs)} of {len(subs)} after {CRAWL_BUDGET}s")
    print(f"  crawl: {len(targets)} sub-pages in {time.time()-t2:.0f}s | {len(byd)} tied domains")
    out, seen_n = [], set()
    for dom, d in byd.items():
        nm = tidy_name(d.get('sitename') or brand(d['titles'], dom))
        if is_junk(nm) or collapse(nm) in vcol or vcol in collapse(nm): continue
        k = collapse(nm)[:20]
        if not k or k in seen_n: continue
        seen_n.add(k)
        out.append({'name': nm, 'domain': dom, 'tie': d['tie'], 'url': d['url'],
                    'snippet': d.get('snippet', ''), 'evidence': d.get('evidence', ''),
                    'evidence_date': d.get('evidence_date'), 'image': d.get('image'),
                    'email': d.get('email'), 'phone': d.get('phone'), 'clinks': d.get('clinks', []),
                    'src': 'dataforseo'})
    for r in agg:
        r['name'] = tidy_name(r['name'])
        if is_junk(r['name']) or collapse(r['name']) in vcol or vcol in collapse(r['name']): continue
        # On a directory page the hirer is the org LISTED, never the directory — drop Footyaddicts,
        # Netmums and friends when the extracted name is just the platform's own brand.
        aggbrand = collapse(_registrable(r['domain']).split('.')[0])
        nm = collapse(r['name'])
        if aggbrand and (aggbrand in nm or nm in aggbrand): continue
        k = collapse(r['name'])[:20]
        if k in seen_n: continue
        seen_n.add(k); r['src'] = 'dataforseo'; out.append(r)
    return out, round(cost, 4)

CONTACT_PATHS = ['/contact', '/contact-us', '/about', '/']
def fill_contacts(cands):
    """Chase a contact for the candidates that PASSED the gate only.
    Doing this for every tied domain meant ~800 concurrent page fetches and knocked the instance over;
    the survivors are ~15 rows, and they are the only ones anyone will ever ring."""
    todo = []
    for i, c in enumerate(cands):
        dom = c.get('domain') or ''
        if not dom or dom == 'facebook' or c.get('email') or is_agg(dom): continue
        urls = list(dict.fromkeys((c.get('clinks') or [])[:2] + [f"https://{dom}{p}" for p in CONTACT_PATHS]))
        for u in urls[:5]: todo.append((i, dom, u))
    if not todo: return 0
    # Child processes here too: 26 contact pages once took 555 seconds, and a page that never comes
    # back must not hold up a search whose results are already found.
    got = in_children(todo, _contact_stream, CRAWL_BUDGET)
    for i, em, ph in got:
        if em and not cands[i].get('email'): cands[i]['email'] = em
        if ph and not cands[i].get('phone'): cands[i]['phone'] = ph
    if len(got) < len(todo): print(f"  contacts: gave up on {len(todo)-len(got)} of {len(todo)} after {CRAWL_BUDGET}s")
    return len(todo)

# ---------------- own-site lookup ----------------
# Measured on the pilot data 2026-08-04: when discovery reaches the organisation's OWN website 73% of
# results carry a contact; when the proof page belongs to somebody else it is 55%, and when the only
# evidence is a booking platform, league listing, PDF or the charity register it is effectively zero.
# That is not an extraction failure. contacts() deliberately refuses to take an address off a domain
# that is not the organisation's, because a page listing many groups will otherwise leak one group's
# email onto another (a surgery page did exactly that on 27 July). The fix is to go and FIND the org's
# own site, then run the same trusted extraction against that.
ORG_STOP = {'the','and','of','uk','ltd','limited','cic','club','clubs','academy','group','groups','centre',
            'center','community','association','society','school','schools','sports','sport','fc','afc','rfc',
            'team','teams','company','classes','class','lessons','sessions','junior','juniors','youth','ladies'}
def _org_tokens(name):
    return [t for t in re.split(r'[^a-z0-9]+', (name or '').lower()) if len(t) > 2 and t not in ORG_STOP]

def domain_carries_name(name, dom):
    """The domain itself spells the organisation out. Strong enough to store as their website."""
    toks = _org_tokens(name)
    if not toks: return False
    dcol = collapse(_registrable(dom).split('.')[0])
    ncol = collapse(name)
    if len(ncol) >= 8 and ncol in dcol: return True
    joined = ''.join(toks)
    return len(joined) >= 8 and all(t in dcol for t in toks)

def site_is_theirs(name, dom, title, sitename):
    """The bar for believing a site belongs to this organisation.

    Deliberately strict. A loose match here staples a stranger's phone number onto a hirer, which is the
    same class of error as the name-only merge that leaked a private address onto a Blenheim record. A
    blank contact is a worse lead; a wrong contact is a wrong phone call. When in doubt, return False."""
    if domain_carries_name(name, dom): return True
    toks = _org_tokens(name)
    if not toks: return False
    dcol = collapse(_registrable(dom).split('.')[0])
    # Otherwise the page itself must carry the whole name AND the domain must corroborate it, either
    # with one of the distinctive words or with the organisation's initials. Title alone is not enough:
    # we searched for this name, so a local listings site will have it in its title too.
    tcol = collapse(f"{title or ''} {sitename or ''}")
    # Initials of the full name, not of the distinctive tokens: PQA trade as pqacademy.com, and the "A"
    # is the Academy that _org_tokens strips out.
    words = [w for w in re.split(r'[^a-z0-9]+', (name or '').lower()) if w and w not in {'of','the','and','for'}]
    initials = ''.join(w[0] for w in words)
    corroborated = (any(t in dcol for t in toks)
                    or (len(initials) >= 3 and dcol.startswith(initials[:3])))
    if len(toks) >= 2 and all(t in tcol for t in toks) and corroborated: return True
    if len(toks) == 1 and len(toks[0]) >= 8 and toks[0] in tcol and toks[0] in dcol: return True
    return False

def locality_needles(pc, venue=''):
    """Words that should appear on the site of a group that actually meets at this venue.

    Name matching alone cannot tell two identically-named groups apart, and generic names are common:
    "Superstars" in Northwich resolved to a Warrington business, "Mini Munchkins" in York to a Montessori
    nursery elsewhere. postcodes.io is free and needs no key."""
    out = {collapse(pc), outcode(pc)}
    out |= {collapse(t) for t in vtokens(venue)}
    try:
        with urllib.request.urlopen(f"https://api.postcodes.io/postcodes/{urllib.parse.quote(pc)}", timeout=10) as r:
            d = json.load(r).get('result') or {}
        # Ward, parish and district only. The region ("Yorkshire and The Humber", "North West") is far too
        # broad to prove anything.
        for k in ('admin_ward', 'parish', 'admin_district'):
            for part in re.split(r'[^A-Za-z]+', d.get(k) or ''):
                out.add(collapse(part))
    except Exception:
        pass
    # The postcode and its outcode are always kept: a three-character outcode like "cw8" is short but
    # it is the most precise needle we have.
    keep = {n for n in out if n and len(n) >= 4 and n not in NEEDLE_STOP}
    return keep | {n for n in (collapse(pc), outcode(pc)) if n}

# Words that prove nothing about location, either because they are county-scale or because they turn up
# in ordinary page furniture. "Fulford Social Hall" contributed "social", which matched "social media" on
# a Montessori nursery's about page; "Hartford Church of England High School" plus the district
# "Cheshire West and Chester" let a Warrington business through on the word "cheshire".
NEEDLE_STOP = {
    'social', 'hall', 'centre', 'center', 'church', 'school', 'academy', 'college', 'community', 'sports',
    'sport', 'leisure', 'club', 'park', 'green', 'grange', 'manor', 'lodge', 'house', 'high', 'primary',
    'junior', 'infant', 'grammar', 'catholic', 'england', 'wales', 'scotland', 'britain', 'kingdom',
    'north', 'south', 'east', 'west', 'central', 'upper', 'lower', 'great', 'little', 'city', 'town',
    'county', 'district', 'borough', 'council', 'unparished', 'area', 'ward', 'saint', 'trust',
    'cheshire', 'yorkshire', 'lancashire', 'humber', 'midlands', 'surrey', 'sussex', 'essex', 'kent',
    'hampshire', 'berkshire', 'cumbria', 'devon', 'dorset', 'norfolk', 'suffolk', 'somerset', 'wiltshire',
    'shire', 'greater', 'metropolitan', 'valley', 'moor', 'dale', 'hill', 'wood', 'field',
}

def find_own_sites(cands, pc, own='', venue=''):
    """For survivors with no contact whose only proof page is somebody else's, go and find the
    organisation's own website, then extract from there.

    Runs AFTER the gate and after fill_contacts, so it only ever looks at rows that survived and still
    have nothing to ring — a handful per search, one SERP query each. It also writes the discovered site
    back as the organisation's website, which is the right value: before this, a club whose only presence
    was a booking platform had that platform stored as its website."""
    oc = outcode(pc)
    # Every survivor with nothing to ring, whatever the proof domain. fill_contacts has already tried
    # that domain's own contact pages, so the only duplicated work is one SERP query, and the "somebody
    # else's site" bucket is not just booking platforms: a stats mirror, an issuu magazine, a county FA
    # PDF and a HESA spreadsheet all showed up in the pilot data.
    todo = [i for i, c in enumerate(cands) if not c.get('email') and not c.get('phone')]
    if not todo: return 0, 0.0, 0
    cost, found = 0.0, 0

    def search(i):
        # Unquoted: an exact-phrase search on a club name plus an outcode returns nothing at all for
        # plenty of small groups ("Sallys Dance", "Pentecostal Church Aveley" both came back empty).
        # Precision comes from the checks below, not from narrowing the query.
        items, qc = dfs(f'{cands[i]["name"]} {oc}', depth=10)
        return i, items, qc

    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        answers = list(ex.map(search, todo))

    targets = []
    for i, items, qc in answers:
        cost += qc
        c = cands[i]
        picked = 0
        for it in items[:8]:
            dom = urllib.parse.urlparse(it['url']).netloc.lower().replace('www.', '')
            if not dom or noisy(dom) or is_agg(dom) or 'facebook.com' in dom: continue
            if own and _registrable(dom) == _registrable(own): continue  # never the venue's own site
            targets.append((i, dom, it['url'], it['title']))
            picked += 1
            # Three shots, not one: the club's own site is often not the top non-directory hit
            # (Fulford Scouts sat behind the county scouting site).
            if picked >= 3: break

    needles = locality_needles(pc, venue)

    def probe(t):
        i, dom, url, title = t
        _, raw = fetch(url)
        if not raw: return None
        name = cands[i]['name']
        if not site_is_theirs(name, dom, title, site_name(raw)): return None
        em, ph = contacts(raw, dom)
        text = page_text(raw)
        if not em or not ph:
            for su in sublinks(raw, url, dom, ['contact', 'about'])[:2]:
                _, sraw = fetch(su)
                if not sraw: continue
                sem, sph = contacts(sraw, dom)
                em, ph = em or sem, ph or sph
                text += ' ' + page_text(sraw)
                if em and ph: break
        if not em and not ph: return None
        return {'i': i, 'name': name, 'dom': dom, 'url': url, 'title': title,
                'email': em, 'phone': ph, 'text': text[:1800]}

    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        hits = [r for r in ex.map(probe, targets) if r]
    # Keep the first surviving site per organisation, then let the model adjudicate. String rules got
    # this wrong in both directions on the pilot data: a perfect name match sent "Mini Munchkins" in York
    # to a Montessori nursery elsewhere, while a locality test strict enough to catch that threw away
    # Pan Nation, whose own site never names the ward it hires in. It also mis-scored both ways by
    # substring: "chester" matched inside "manchester". Deciding whether two things are the same
    # organisation is a judgement, so ask the model that already names organisations in this pipeline.
    # Measured on 30 contactless pilot rows: 7 resolved, all 7 correct on inspection.
    first, seen_i = [], set()
    for r in hits:
        if r['i'] in seen_i: continue
        seen_i.add(r['i']); first.append(r)
    verdicts, jcost = same_org_verdicts(first, venue, pc, needles)
    cost += jcost
    for h, ok in zip(first, verdicts):
        if not ok: continue
        c = cands[h['i']]
        if h['email']: c['email'] = h['email']
        if h['phone']: c['phone'] = h['phone']
        # Only claim it as their website when the domain itself spells them out. A profile on a niche
        # directory can carry the right person's email while still not being their site, and storing it
        # would repeat the deep-link problem fixed on 4 August.
        if domain_carries_name(c['name'], h['dom']): c['website'] = f"https://{h['dom']}/"
        c['own_site_url'] = h['url']
        found += 1
    return len(todo), round(cost, 4), found

def same_org_verdicts(hits, venue, pc, needles):
    """One call, all candidates: is this website the organisation's own?

    Defaults to NO. A blank contact is a lead Vivify cannot ring; a wrong contact is Vivify ringing a
    stranger about a booking they know nothing about, which is worse for them than the blank."""
    if not hits: return [], 0.0
    if not OPENAI_KEY: return [False] * len(hits), 0.0
    items = [f'[{n}] organisation name: {h["name"]}\n    candidate site: {h["dom"]}\n'
             f'    page title: {h["title"]}\n    page text: {h["text"]}'
             for n, h in enumerate(hits)]
    prompt = (
        f'An organisation was found advertising an activity at "{venue}" ({pc}). For each numbered item '
        f'below, decide whether the candidate website belongs to THAT organisation, the one operating at '
        f'that venue — not merely to a different organisation with a similar or identical name somewhere '
        f'else in the country.\n\n'
        f'Words associated with the venue\'s area: {", ".join(sorted(needles))}.\n'
        f'Treat a site that names the venue, that area, or a plainly compatible area as the same '
        f'organisation. Treat a site whose stated location is clearly somewhere else as a different one. '
        f'A national body\'s website is NOT the local branch\'s own site. If you cannot tell, answer false.\n\n'
        f'Return ONLY a JSON array like [{{"n":0,"same":true}},...], one entry per item.\n\n'
        + '\n\n'.join(items))
    body = json.dumps({"model": "gpt-4o", "temperature": 0, "max_tokens": 800,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    try:
        r = urllib.request.Request("https://api.openai.com/v1/chat/completions", data=body,
            headers={"Authorization": "Bearer " + OPENAI_KEY, "Content-Type": "application/json"})
        d = json.load(urllib.request.urlopen(r, timeout=120))
        u = d.get('usage', {})
        cost = round(u.get('prompt_tokens', 0) / 1e6 * 2.50 + u.get('completion_tokens', 0) / 1e6 * 10.0, 4)
        arr = json.loads(re.search(r'\[[\s\S]*\]', d['choices'][0]['message']['content']).group(0))
    except Exception as e:
        sys.stderr.write(f"same-org err {e}\n")
        return [False] * len(hits), 0.0
    out = [False] * len(hits)
    for it in arr:
        if isinstance(it, dict) and isinstance(it.get('n'), int) and 0 <= it['n'] < len(out):
            out[it['n']] = bool(it.get('same'))
    return out, cost

DEDUPE_STOP = {'the','and','of','in','at','uk','ltd','limited','cic','stockport','manchester','london','group'}
def _key_tokens(name):
    return {t for t in re.split(r'[^a-z0-9]+', (name or '').lower()) if len(t) > 2 and t not in DEDUPE_STOP}

def merge_duplicates(cands):
    """One hirer often surfaces under several names — "Boutique Baby Sale" and "Boutique Baby Sale
    Stockport Chorlton and Altrincham" are the same business, and a list that shows both looks careless.
    Merge when one name's words are contained in the other's, or when they share a domain, keeping the
    richer record and carrying across whatever contact or evidence the other one had."""
    out = []
    for c in sorted(cands, key=lambda x: (-len(x.get('evidence') or ''), len(x.get('name') or ''))):
        toks, dom = _key_tokens(c.get('name')), (c.get('domain') or '')
        hit = None
        for o in out:
            otoks = _key_tokens(o.get('name'))
            if not toks or not otoks: continue
            same_domain = dom and dom not in ('facebook', '') and dom == o.get('domain')
            contained = toks <= otoks or otoks <= toks
            overlap = len(toks & otoks) / max(1, min(len(toks), len(otoks)))
            if same_domain or contained or overlap >= 0.8:
                hit = o; break
        if hit is None:
            out.append(c); continue
        for f in ('email', 'phone', 'website', 'image'):
            if not hit.get(f) and c.get(f): hit[f] = c[f]
        # Newest proof wins: if a group was mentioned a year ago and again yesterday, Vivify sees
        # yesterday. Only fall back to "longest" when neither mention carries a date.
        cd, hd = (c.get('evidence_date') or '')[:10], (hit.get('evidence_date') or '')[:10]
        newer = (cd > hd) if (cd and hd) else (bool(cd) and not hd)
        longer = not cd and not hd and len(c.get('evidence') or '') > len(hit.get('evidence') or '')
        if newer or longer:
            hit['evidence'] = c.get('evidence') or hit.get('evidence')
            hit['evidence_date'] = c.get('evidence_date') or hit.get('evidence_date')
            if c.get('image'): hit['image'] = c['image']
        if len(c.get('name') or '') > len(hit.get('name') or '') and not is_junk(c.get('name')):
            hit['name'] = c['name']
    return out

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
                    'snippet': '', 'evidence': '', 'src': 'venue_db', 'db_id': r.get('id'), 'website': r.get('website'),
                    'email': r.get('email'), 'phone': r.get('phone_number')})
    return out

LETTINGS_HINT = re.compile(r'(letting|hire|community|club|facilit|what.?s.on|partner|user|activities|out.of.hours)', re.I)
def venue_own_site(venue, pc, own):
    """Read the venue's OWN lettings and community pages and ask who is named on them.

    Schools list their regular hirers on their own site far more often than the hirers publish it
    themselves, and we exclude that domain from the search queries (so the school's pages don't crowd
    out everyone else), which meant we were never reading the single best page. This puts it back."""
    if not own or not OPENAI_KEY: return [], 0.0
    root = f"https://{own}/"
    _, raw = fetch(root)
    if not raw: return [], 0.0
    pages, seen = [], set()
    for m in re.finditer(r'(?is)<a\b[^>]*href=["\']([^"\'#]+)["\'][^>]*>(.*?)</a>', raw):
        href, anchor = m.group(1), re.sub(r'<[^>]+>', ' ', m.group(2))
        if not LETTINGS_HINT.search(href + ' ' + anchor): continue
        full = urllib.parse.urljoin(root, href)
        if urllib.parse.urlparse(full).netloc.lower().replace('www.', '') != own: continue
        if full in seen: continue
        seen.add(full); pages.append(full)
    text = ''
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        for _, r in ex.map(fetch, pages[:8]):
            if not r: continue
            t = re.sub(r'\s+', ' ', html.unescape(re.sub(r'(?is)<[^>]+>', ' ',
                re.sub(r'(?is)<(script|style|noscript|svg)[^>]*>.*?</\1>', ' ', r))))
            text += t[:4000] + '\n'
    if len(text) < 200: return [], 0.0
    prompt = (f'Below is text from the website of "{venue}" ({pc}), a venue that hires its halls and pitches '
              f'to outside groups. List ONLY the outside organisations named as using, hiring or running '
              f'activities at this venue: community groups, clubs, classes, faith and cultural groups, '
              f'supplementary schools. Exclude the venue itself, its departments, staff, its own curriculum '
              f'or after-school provision, suppliers, other schools, and anything not clearly a named group.\n'
              f'Return ONLY a JSON array of objects: [{{"name":"<organisation>","note":"<what they do here>"}}]\n\n'
              + text[:24000])
    body = json.dumps({"model": "gpt-4o", "temperature": 0, "max_tokens": 1200,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    try:
        r = urllib.request.Request("https://api.openai.com/v1/chat/completions", data=body,
            headers={"Authorization": "Bearer " + OPENAI_KEY, "Content-Type": "application/json"})
        d = json.load(urllib.request.urlopen(r, timeout=120))
        u = d.get('usage', {})
        cost = round(u.get('prompt_tokens', 0) / 1e6 * 2.50 + u.get('completion_tokens', 0) / 1e6 * 10.0, 4)
        arr = json.loads(re.search(r'\[[\s\S]*\]', d['choices'][0]['message']['content']).group(0))
    except Exception as e:
        sys.stderr.write(f"own-site err {e}\n"); return [], 0.0
    out, seen_n = [], set()
    for it in arr:
        nm = (it.get('name') or '').strip() if isinstance(it, dict) else ''
        if not nm or is_junk(nm) or collapse(nm) in collapse(venue) or collapse(venue) in collapse(nm): continue
        k = collapse(nm)[:20]
        if not k or k in seen_n: continue
        seen_n.add(k)
        out.append({'name': nm, 'domain': '', 'tie': 'venue', 'url': root,
                    'snippet': (it.get('note') or '')[:280],
                    'evidence': f"Named on {venue}'s own website as a hirer. {(it.get('note') or '')}".strip()[:1200],
                    'src': 'venue_site'})
    return out, cost

def classforkids(venue, pc):
    """ClassForKids lists activity providers by venue. The postcode slug pins it to the right place,
    which is what makes this directory usable where the others are geographically vague."""
    slug = collapse(pc)
    slug = f"{slug[:-3]}-{slug[-3:]}" if len(slug) >= 5 else slug
    try:
        _, raw = fetch(f"https://classforkids.io/en-GB/classes/{slug}")
    except Exception:
        return []
    out, seen = [], set()
    for m in re.finditer(r'https?://([a-z0-9\-]+)\.classforkids\.io[^"\']*?venueName=([^"\'&]+)', raw or '', re.I):
        provider, venue_name = m.group(1), urllib.parse.unquote_plus(m.group(2))
        if collapse(venue)[:14] not in collapse(venue_name): continue
        nm = provider.replace('-', ' ').title()
        k = collapse(nm)[:20]
        if not k or k in seen or is_junk(nm): continue
        seen.add(k)
        out.append({'name': nm, 'domain': f'{provider}.classforkids.io', 'tie': 'venue',
                    'url': f'https://{provider}.classforkids.io', 'snippet': f'Classes at {venue_name}',
                    'evidence': f'Listed on ClassForKids as running classes at {venue_name}.', 'src': 'dataforseo'})
    return out

def fb_pages(venue, pc):
    """Pages whose own details name the venue — a standing group, where a post is a single occurrence."""
    if not APIFY: return []
    u = f"https://api.apify.com/v2/acts/apify~facebook-search-scraper/run-sync-get-dataset-items?clean=true&token={APIFY}"
    body = json.dumps({"query": f"{venue} {pc}", "search_type": "pages", "max_results": 12}).encode()
    try:
        rows = json.load(urllib.request.urlopen(urllib.request.Request(u, data=body, headers={"Content-Type": "application/json"}), timeout=180))
    except Exception as e:
        sys.stderr.write(f"fb pages err {e}\n"); return []
    vcol, pcol = collapse(venue), collapse(pc)
    out, seen = [], set()
    for p in rows if isinstance(rows, list) else []:
        if not isinstance(p, dict): continue
        nm = p.get('name') or p.get('title') or ''
        blob = collapse(json.dumps(p))
        if not nm or not (pcol in blob or (len(vcol) > 6 and vcol in blob)): continue
        k = collapse(nm)[:20]
        if not k or k in seen or is_junk(nm): continue
        seen.add(k)
        out.append({'name': nm, 'domain': 'facebook', 'tie': 'venue', 'url': p.get('url') or p.get('pageUrl') or '',
                    'snippet': (p.get('intro') or p.get('description') or '')[:280],
                    'evidence': (p.get('intro') or p.get('description') or '')[:1200],
                    'email': p.get('email'), 'phone': p.get('phone'),
                    'image': fb_image(p), 'src': 'facebook'})
    return out

def fb_image(p):
    """The post's own photo. Actors vary in shape, so look everywhere it might be."""
    for k in ('image', 'imageUrl', 'thumbnailUrl', 'photo', 'picture', 'full_picture'):
        v = p.get(k)
        if isinstance(v, str) and v.startswith('http'): return v[:500]
        if isinstance(v, dict):
            u = v.get('uri') or v.get('url')
            if isinstance(u, str) and u.startswith('http'): return u[:500]
    for k in ('images', 'photos', 'attachments', 'media'):
        v = p.get(k)
        if isinstance(v, list) and v:
            first = v[0]
            if isinstance(first, str) and first.startswith('http'): return first[:500]
            if isinstance(first, dict):
                u = first.get('url') or first.get('uri') or first.get('src') or first.get('image')
                if isinstance(u, str) and u.startswith('http'): return u[:500]
    return None

def fb_posts(venue, pc):
    if not APIFY: return []
    u = f"https://api.apify.com/v2/acts/powerai~facebook-post-search-scraper/run-sync-get-dataset-items?clean=true&token={APIFY}"
    # Vivify chase current hirers: a post from two years ago is not proof anyone still books the hall.
    since = time.strftime('%Y-%m-%d', time.gmtime(time.time() - 183 * 86400))
    body = json.dumps({"query": venue, "maxResults": 15, "recent_posts": True, "start_date": since}).encode()
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
        ts = p.get('timestamp') or p.get('time') or p.get('date')
        evdate = None
        try:
            if isinstance(ts, (int, float)): evdate = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(ts if ts < 1e11 else ts/1000))
            elif isinstance(ts, str) and ts: evdate = ts
        except Exception: evdate = None
        out.append({'name': nm, 'domain': 'facebook', 'tie': 'venue', 'url': a.get('url') or '',
                    'snippet': msg[:280], 'evidence': msg[:2000],  # the whole post, not a clipped fragment
                    'evidence_date': evdate, 'image': fb_image(p), 'src': 'facebook_post'})
    return out

# ---------------- LLM gate ----------------
def gate(cands, venue, pc):
    """Return (verdicts, gate_cost_usd). verdicts = list of (keep, confidence, category) aligned to cands.
    Uses OpenAI gpt-4o if key present, else a deterministic fallback (flagged)."""
    if not cands: return [], 0.0
    if not OPENAI_KEY:
        return [(True, ('confirmed' if c['tie'] == 'postcode' else 'likely'), '') for c in cands], 0.0
    # The evidence passage first and 700 characters of it, not 200 characters of the SERP snippet. The
    # gate was deciding "does this text tie the organisation to this venue" while being shown a fragment
    # of a different, shorter text than the one we store and show the customer: stored evidence averages
    # 984 characters. It was answering on less than we hold.
    items = "\n".join(
        f"{i}. {c['name']} — {((c.get('evidence') or '') + ' ' + (c.get('snippet') or '')).strip()[:700]}"
        for i, c in enumerate(cands))
    prompt = (
        f'A specific UK venue, "{venue}" (postcode {pc}), hires its facilities to OUTSIDE organisations: '
        f'community groups, sports clubs, dance/gym/arts providers, faith and cultural groups, tuition/language/'
        f'supplementary schools, youth and toddler groups. For EACH candidate set useful=true ONLY if BOTH: '
        f'(1) it is a REAL, NAMED organisation/club/class/group (a proper-noun name) — NOT a generic phrase, page '
        f'title, booking label, map, directory index, product or section heading; AND (2) the text ties it to THIS '
        f'venue ("{venue}") or postcode {pc} as where it runs/plays/meets — not a different venue or just the wider area. '
        f'useful=false for: the venue itself/its pages/staff, jobs, news/Ofsted, directions/maps/transport, aggregator '
        f'index pages, estate agents/area guides, unrelated businesses, bare personal names. '
        f'The hirer is whoever RUNS the session, never the website that merely lists somebody else\'s: if the candidate '
        f'is a directory, listings site, ticketing platform or publisher advertising other people\'s classes (Netmums, '
        f'Happity, Footyaddicts, Eventbrite, Trip.com, ClassForKids, Meetup and the like), useful=false even when the '
        f'listing names this venue. An operator that runs its OWN leagues, classes or camps at the venue IS a hirer, '
        f'even if it also sells places online. '
        f'Each candidate label below was scraped from a web page title, so it is often NOT a clean name: it may be '
        f'a headline, a match report ("Marple Athletic Red U16 6-0 Bollington Bullets"), a section heading '
        f'("Club matches"), a listing ("Martial Arts Clubs Stockport"), a session description ("Junior and Senior '
        f'winter training"), an age group ("U14"), or a name with a location tail ("Revolution Martial Arts in '
        f'Marple, Stockport SK6 6LB"). Your job includes NAMING the organisation.\n'
        f'Set org_name to the organisation\'s own trading name as it would appear on its letterhead — no location '
        f'tail, no "in Stockport", no session or age detail, no headline words. If the label is a headline or report, '
        f'extract the club from it. If you CANNOT identify a specific named organisation, set useful=false: Vivify '
        f'has to be able to ring a named club, so anything unnameable is worthless to them.\n'
        f'confidence="confirmed" if it explicitly names this venue or postcode, else "likely". When in doubt useful=false.\n'
        f'Return ONLY a JSON array: [{{"i":<index>,"useful":true|false,"org_name":"<clean trading name>",'
        f'"confidence":"confirmed"|"likely","category":"<short>"}}]\n\n'
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
                # The gate names the organisation; the scraped page title is only a hint. This is what
                # stops headlines and listing labels reaching Vivify — a pattern list never keeps up.
                nm = (v.get('org_name') or '').strip()
                if nm and 3 <= len(nm) <= 60 and not is_junk(nm): c['name'] = tidy_name(nm)
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
    body = {"status": status}
    # Stamp when THIS run began. The stalled-search watchdog used to measure from created_at, so
    # re-running a search made earlier in the day was declared stalled before it had done anything.
    if status == 'searching':
        body["started_at"] = time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime())
    sreq("PATCH", "group_searches", body, params=f"?id=eq.{sid}")

def cache_lookup(venue, pc, sid):
    since = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(time.time() - 14*86400))
    q = (f"?select=id&search_type=eq.venue_hirers&status=eq.complete"
         f"&venue_name=eq.{urllib.parse.quote(venue)}&postcode=eq.{urllib.parse.quote(pc)}"
         f"&id=neq.{sid}&created_at=gte.{urllib.parse.quote(since)}&order=created_at.desc&limit=1")
    rows = sreq("GET", "group_searches", params=q)
    return rows[0]['id'] if rows else None

# ---------------- main ----------------
def shot(url, src):
    """Fallback proof image: a live screenshot of the evidence page, rendered on demand by thum.io.
    Plenty of small club sites publish no share image, and Vivify wants to SEE the page that names
    the venue. No browser on this worker and no cost; swap the base URL if the service ever changes."""
    if src != 'dataforseo' or not url or not url.startswith('http'): return None
    return f"https://image.thum.io/get/width/900/crop/700/{url}"

def to_result(c):
    name = c['name']
    pid = c.get('place_id') or (synth('vdb', str(c['db_id'])) if c.get('db_id') else synth(c['src'][:3], (c.get('url') or '') + '|' + (c.get('domain') or name)))
    return {
        "company_name": name, "address": None, "postcode": None, "city": None,
        "phone_number": c.get('phone'), "email": c.get('email'),
        "website": c.get('website') or (c.get('url') if c['src'] == 'dataforseo' else None),
        "facebook_url": c['url'] if c['src'] == 'facebook_post' else None,
        "activity_description": c.get('snippet') or None, "additional_information": c.get('url') or None,
        "activity_type": c.get('category') or None, "place_id": pid,
        "evidence_source": c['src'], "evidence_url": c.get('url') or None, "source_url": c.get('url') or None,
        "evidence_text": c.get('evidence') or c.get('snippet') or None,
        "evidence_image_url": c.get('image') or shot(c.get('url'), c['src']),
        "evidence_date": c.get('evidence_date'), "confidence_tier": c['tier'],
    }

def run(sid, force=False):
    """force=True skips the 14-day cache. Monitoring runs MUST bypass it: their whole job is to spot
    hirers who appeared since last time, and a cached copy of the previous run can never do that."""
    s = get_search(sid)
    if not s: sys.exit(f"search {sid} not found")
    venue, label = split_venue_label(s.get('venue_name') or s.get('search_name') or '')
    pc = (s.get('postcode') or '').strip()
    print(f"[{sid}] {venue} ({pc})" + (f" [label: {label}]" if label else ""))
    # No postcode, no search. The postcode is the only thing that ties evidence to THIS venue rather
    # than a same-named place elsewhere, and running without one burns money on unusable results.
    if not UKPC_RE.fullmatch((pc or '').strip()):
        print(f"  refusing to run: '{pc}' is not a valid UK postcode")
        set_status(sid, 'error'); return
    # Refuse to start rather than return an empty search. Discovery is ~49 DataForSEO queries; with no
    # credit every one of them 402s and the search completes having found nothing, which reads to the
    # customer as "this venue has no hirers" rather than "nobody paid the bill".
    bal = dfs_balance()
    if bal is not None and bal < 0.50:
        print(f"  refusing to run: DataForSEO balance is ${bal:.2f}")
        sreq("PATCH", "group_searches",
             # venue_note is customer-facing. Vivify need to know the search did not run and that it is
             # not their fault; they do not need to read about our supplier's billing. The detail goes
             # to the console and to Dean through the stalled-search watchdog.
             {"status": "failed",
              "venue_note": "This search could not run because of a temporary problem at our end, so no "
                            "results were gathered. It has been reported and can be run again shortly."},
             params=f"?id=eq.{sid}")
        return
    # A hard wall that does not need the interpreter to cooperate. Twice now a single host has wedged
    # a fetch for twenty minutes while holding the GIL, which starves every deadline in this file —
    # the 300-second page budget for St Mary's returned after 1,197 seconds because the waiting thread
    # never got to run. faulthandler's timer lives in C, so it fires anyway: it prints the stack of
    # every thread, which names the exact call that is stuck, and takes the process down. A search that
    # dies at fifteen minutes with a traceback is worth more than one killed silently at thirty.
    faulthandler.dump_traceback_later(SEARCH_WALL, exit=True)
    typed_pc = pc
    set_status(sid, 'searching')
    cname, cpc, own = resolve_venue(venue, pc)
    print(f"  places={'on' if GPLACES else 'OFF'} resolved={cname!r} {cpc!r} own={own!r}")
    if (cname, cpc) != (venue, pc):
        print(f"  canonical: {cname} ({cpc}){' own site ' + own if own else ''}")
        # Keep the user's own label on the record they see. Only the search itself drops it.
        patch = {"venue_name": cname + (f" ({label})" if label else ""), "postcode": cpc}
        # Say so when we searched a different postcode from the one typed. Silently correcting it means
        # a mistyped postcode belonging to ANOTHER school would be searched with nobody any the wiser.
        if collapse(cpc) != collapse(typed_pc):
            note = f"Searched {cname} at {cpc}. You entered {typed_pc}; Google Places lists this venue at {cpc}."
            patch["venue_note"] = note
            print(f"  NOTE: postcode corrected {typed_pc} -> {cpc}")
        sreq("PATCH", "group_searches", patch, params=f"?id=eq.{sid}")
        venue, pc = cname, cpc
    elif own:
        print(f"  own site {own}")
    prior = None if force else cache_lookup(venue, pc, sid)
    if prior:
        sreq("POST", "rpc/copy_venue_search_results", {"p_from": prior, "p_to": sid})
        set_status(sid, 'complete'); print(f"  cache hit from {prior} — £0"); return
    web, dfs_cost = discover_web(venue, pc, own)
    t_own = time.time()
    site, site_cost = venue_own_site(venue, pc, own)
    print(f"  venue site: {len(site)} named hirers in {time.time()-t_own:.0f}s")
    db = db_postcode(pc)
    t_fb = time.time()
    fb = fb_posts(venue, pc) + fb_pages(venue, pc)
    print(f"  facebook: {len(fb)} authors/pages in {time.time()-t_fb:.0f}s")
    t_cfk = time.time()
    cfk = classforkids(venue, pc)
    print(f"  classforkids: {len(cfk)} providers in {time.time()-t_cfk:.0f}s")
    cands = web + site + db + fb + cfk
    print(f"  candidates: web={len(web)} db={len(db)} fb={len(fb)} | gate={'gpt-4o' if OPENAI_KEY else 'DETERMINISTIC(no key)'}")
    verdicts, gate_cost = gate(cands, venue, pc)
    survivors = []
    for c, (ok, conf, cat) in zip(cands, verdicts):
        if not ok: continue
        c['tier'] = conf or ('confirmed' if c['tie'] == 'postcode' else 'likely'); c['category'] = cat
        survivors.append(c)
    # Old evidence is kept — it still shows a group used the venue, and the card carries the date so
    # Vivify can judge it. What matters is that where a group appears more than once we show its MOST
    # RECENT proof, which merge_duplicates does below.
    before = len(survivors)
    survivors = merge_duplicates(survivors)
    if before != len(survivors): print(f"  merged {before - len(survivors)} duplicate listings")
    t_c = time.time()
    pages = fill_contacts(survivors)
    print(f"  contacts: {pages} pages in {time.time()-t_c:.0f}s")
    t_o = time.time()
    # venue matters: it feeds locality_needles, which is how the adjudicator tells this venue's
    # "Superstars" from an identically named business in another town. Omitting it silently weakened
    # every live search while the test harness, which does pass it, kept looking correct.
    looked, own_cost, gained = find_own_sites(survivors, pc, own, venue)
    print(f"  own-site lookup: {looked} without a contact, {gained} resolved in {time.time()-t_o:.0f}s (${own_cost})")
    kept = [to_result(c) for c in survivors]
    apify_cost = 0.05 if fb else 0.0
    g_calls = 1 if GPLACES else 0
    g_cost = round(0.032 * g_calls, 4)  # Places Text Search
    total = round(dfs_cost + gate_cost + apify_cost + g_cost + site_cost + own_cost, 4)
    with_contact = sum(1 for k in kept if k.get('email') or k.get('phone_number'))
    print(f"  kept {len(kept)} of {len(cands)} | {with_contact} with a contact")
    print(f"  SPEND: dataforseo=${dfs_cost} gate=${gate_cost} apify=${apify_cost} places=${g_cost} | total=${total}")
    log_spend(sid, venue, pc, len(kept), dfs_cost, gate_cost, apify_cost, total)
    # Past the point where anything can hang: from here it is two database calls, and being killed
    # between them would leave the search half-written.
    faulthandler.cancel_dump_traceback_later()
    sreq("POST", "rpc/process_venue_hirer_results", {
        "p_search_id": sid, "p_results": kept,
        "p_cost_google": g_cost, "p_cost_dataforseo": dfs_cost, "p_cost_apify": apify_cost,
        "p_google_calls": g_calls})
    # Promote straight to the organisation table ourselves. The shared n8n enrichment used to do this,
    # but it re-scraped every site and wrote contacts this worker had already rejected, and it took
    # minutes. Everything written here has been validated, and the cleanup trigger fires on 'complete'.
    res = sreq("POST", "rpc/promote_venue_results", {"p_search_id": sid})
    print(f"  promoted {res.get('promoted') if isinstance(res, dict) else res} organisations — status complete")

if __name__ == '__main__':
    run(int(sys.argv[1]), force='--force' in sys.argv[2:])
    # Leave immediately rather than letting Python join the fetch threads on the way out. Abandoning a
    # page is the whole point of the deadlines above, and a normal exit would sit waiting for exactly
    # the thread we gave up on — the search would be finished and written, and the process would still
    # be killed at the 30-minute cap and marked failed.
    sys.stdout.flush(); sys.stderr.flush()
    os._exit(0)
