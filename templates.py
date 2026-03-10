"""
HTML-шаблоны для веб-интерфейсов: история арбитража и debug-лог.
"""

# ── History Dashboard HTML ────────────────────────────────────────────────────
_HISTORY_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>&#9889; Arb History</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');
  :root{--bg:#0a0a0f;--bg2:#12121a;--border:#1e1e2e;--border2:#161620;
    --tx:#c9d1d9;--tx2:#9ca3af;--tx3:#6b7280;--dim:#3a3a4a;
    --green:#00ff87;--purple:#c084fc;--blue:#60a5fa;--yellow:#fbbf24;--red:#ff6b6b;--orange:#fb923c;}
  .light{--bg:#f5f6f8;--bg2:#fff;--border:#d4d8e0;--border2:#e4e8ee;
    --tx:#1a1a2e;--tx2:#4a5068;--tx3:#7a8098;--dim:#b0b8c8;
    --green:#0a9;--purple:#7c3aed;--blue:#2563eb;--yellow:#b45309;--red:#dc2626;--orange:#ea580c;}
  *{margin:0;padding:0;box-sizing:border-box;}
  body{background:var(--bg);color:var(--tx);font-family:'JetBrains Mono','Fira Code',monospace;font-size:12px;padding:10px 14px 60px;}
  .tw{overflow-x:auto;}
  table{width:100%;border-collapse:separate;border-spacing:0;font-size:11px;}
  th{padding:8px 5px;text-align:center;color:var(--tx3);font-size:9px;font-weight:600;
    text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid var(--border);
    position:sticky;top:0;background:var(--bg);z-index:2;white-space:nowrap;}
  th.l{text-align:left;}
  td{padding:3px 5px;white-space:nowrap;text-align:center;vertical-align:middle;}
  td.l{text-align:left;}
  td.sp{width:12px;min-width:12px;padding:0;}
  tr.rw td{background:linear-gradient(180deg,#181825 0%,#0f0f17 60%,#0c0c14 100%);
    border-top:1px solid rgba(255,255,255,.045);border-bottom:1px solid rgba(0,0,0,.45);}
  tr.rw td:first-child{border-radius:4px 0 0 4px;border-left:1px solid rgba(255,255,255,.04);}
  tr.rw td:last-child{border-radius:0 4px 4px 0;border-right:1px solid rgba(0,0,0,.3);}
  tr.rw+tr.rw td{box-shadow:0 -12px 0 var(--bg);}
  .light tr.rw td{background:linear-gradient(180deg,#fff 0%,#f0f2f5 100%);
    border-top:1px solid rgba(255,255,255,.9);border-bottom:1px solid rgba(0,0,0,.08);}
  .p1{font-weight:600;font-size:11px;}
  .p2{font-weight:600;font-size:11px;color:var(--tx2);}
  .ov{font-weight:700;font-variant-numeric:tabular-nums;font-size:13px;}
  .o-t{color:var(--purple);}
  .o-s{color:var(--blue);}
  .o-l{color:var(--red);font-size:12px;}
  .arb{font-weight:700;font-size:12px;font-variant-numeric:tabular-nums;padding:2px 3px;border-radius:3px;}
  .arb.pos{color:var(--green);background:rgba(0,255,135,.09);}
  .arb.neg{color:var(--dim);}
  .arb.near{color:var(--yellow);}

  .ph-strip{display:flex;align-items:baseline;gap:5px;margin-top:3px;padding-top:3px;border-top:1px solid var(--border2);}
  .ph-lbl{font-size:10px;font-weight:700;}
  .ph-BREAK_POINT{color:#f87171;} .ph-TIEBREAK{color:#818cf8;}
  .ph-DEUCE,.ph-ADVANTAGE{color:#fbbf24;} .ph-CHANGEOVER,.ph-SET_BREAK{color:#00ff87;}
  .ph-POST_GAME,.ph-IN_POINT{color:#8899aa;}
  .sc-wrap{display:inline-block;background:var(--bg2);border:1px solid var(--border);border-radius:5px;overflow:hidden;}
  .sc-row{display:flex;align-items:center;height:20px;padding:0 5px;}
  .sc-row+.sc-row{border-top:1px solid var(--border2);}
  .sc-set{width:16px;text-align:center;font-size:12px;font-variant-numeric:tabular-nums;}
  .sc-gs{min-width:20px;height:15px;padding:0 3px;border-radius:2px;background:var(--bg);
    border:1px solid var(--border);display:inline-flex;align-items:center;justify-content:center;
    font-size:10px;font-weight:700;margin-left:5px;}
  .sc-dot{width:6px;height:6px;border-radius:50%;background:var(--green);
    box-shadow:0 0 5px var(--green);margin:0 3px;flex-shrink:0;}
  .sc-dot-h{width:6px;height:6px;border-radius:50%;margin:0 3px;flex-shrink:0;}
  .badge{display:inline-block;padding:1px 5px;border-radius:3px;font-size:9px;
    font-weight:700;letter-spacing:.03em;text-transform:uppercase;white-space:nowrap;}
  .badge-bl{color:#60a5fa;background:rgba(96,165,250,.12);border:1px solid rgba(96,165,250,.25);}
  .badge-bb{color:#c084fc;background:rgba(192,132,252,.12);border:1px solid rgba(192,132,252,.25);}
  .badge-warn{color:var(--orange);background:rgba(251,146,60,.1);border:1px solid rgba(251,146,60,.25);}
  .age-ok{color:var(--green);font-size:10px;} .age-warn{color:var(--yellow);font-size:10px;}
  .age-stale{color:var(--red);font-size:10px;}
  .empty{text-align:center;padding:40px;color:var(--dim);}
  .params{background:var(--bg2);border:1px solid var(--border);border-radius:6px;
    padding:8px 12px;margin-bottom:10px;display:flex;gap:8px;align-items:center;flex-wrap:wrap;}
  .params label{display:flex;align-items:center;gap:3px;color:var(--tx2);font-size:11px;}
  .params input{width:44px;padding:2px 3px;background:var(--bg);border:1px solid var(--border);
    border-radius:3px;color:var(--tx);font-family:inherit;font-size:11px;text-align:center;}
  .ptitle{color:var(--tx3);font-size:9px;text-transform:uppercase;letter-spacing:.06em;padding-right:4px;}
  .pgrp{display:flex;gap:8px;align-items:center;padding:0 8px;border-left:1px solid var(--border);}
  .reload-btn{padding:3px 10px;border-radius:4px;font-size:11px;font-family:inherit;cursor:pointer;
    border:1px solid var(--green);background:rgba(0,255,135,.07);color:var(--green);}
  .reload-btn:hover{background:rgba(0,255,135,.15);}
  .reset-btn{padding:3px 9px;border-radius:4px;font-size:11px;font-family:inherit;cursor:pointer;
    border:1px solid var(--border);background:transparent;color:var(--tx3);}
  .sbar{position:fixed;bottom:0;left:0;right:0;display:flex;align-items:center;
    justify-content:space-between;padding:5px 14px;opacity:.3;transition:opacity .2s;background:var(--bg);}
  .sbar:hover{opacity:.9;}
  .sbar-l{display:flex;gap:12px;font-size:9px;color:var(--tx3);}
  .tbtn{padding:2px 6px;border-radius:3px;cursor:pointer;font-size:11px;
    border:1px solid var(--border);background:transparent;color:var(--tx3);}
  .tour-hdr td{background:linear-gradient(90deg,rgba(251,191,36,.07) 0%,transparent 70%);
    border-left:3px solid var(--yellow);border-bottom:1px solid var(--border);
    border-top:2px solid var(--border);padding:6px 12px;text-align:left;}
  .tour-name{color:var(--yellow);font-size:11px;font-weight:700;}
  .tour-cnt{color:var(--dim);margin-left:8px;font-size:10px;}
</style>
</head>
<body>
<div id="root"></div>
<script>
'use strict';
const _DEF={min_arb:2.0,sharp_delay:3.0,window_start:15.0,window_end:20.0,
            cooldown:60.0,freshness_sec:45.0,lag_sec:2.0,max_thrill_odds:30.0,
            lookback_hours:0,limit:100};
let S={data:null,theme:'dark',params:Object.assign({},_DEF)};
try{if(localStorage.getItem('arb_t')==='light'){S.theme='light';document.body.classList.add('light');}}catch(e){}
try{const p=localStorage.getItem('arb_hp');if(p)S.params=Object.assign({},_DEF,JSON.parse(p));}catch(e){}

const F=(v,d=2)=>v!=null?Number(v).toFixed(d):'--';
const E=s=>s?String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'):'';
const Ago=ts=>{const s=Math.round(Date.now()/1000-ts);return s<60?s+'s':Math.floor(s/60)+'m'+String(s%60).padStart(2,'0')+'s';};
const AgeCls=s=>s==null?'':(s<20?'age-ok':s<40?'age-warn':'age-stale');
const arbCls=v=>v==null?'arb neg':v>0?'arb pos':v>-2?'arb near':'arb neg';
const arbTxt=v=>v==null?'--':(v>0?'+':'')+Number(v).toFixed(2)+'%';

function fmtScore(sc){
  if(!sc)return '';
  const ps=sc.period_scores||[];
  if(!ps.length)return '';
  const hg=sc.home_gamescore,ag=sc.away_gamescore,srv=sc.current_server;
  const row=isH=>{
    let r='<div class="sc-row">';
    for(let i=0;i<ps.length;i++){
      const p=ps[i],v=isH?+p.home_score:+p.away_score,o=isH?+p.away_score:+p.home_score,cur=i===ps.length-1;
      const c=cur||v>o?'var(--tx)':'var(--dim)',fw=cur||v>o?700:400;
      r+=`<span class="sc-set" style="color:${c};font-weight:${fw}">${v}</span>`;
    }
    r+=srv===(isH?1:2)?'<span class="sc-dot"></span>':'<span class="sc-dot-h"></span>';
    const gv=(isH?hg:ag)!=null?String(isH?hg:ag):'0';
    return r+`<span class="sc-gs">${gv}</span></div>`;
  };
  return `<div class="sc-wrap">${row(true)}${row(false)}</div>`;
}

function phBadge(ph){
  if(!ph)return '';
  return `<div class="ph-strip"><span class="ph-lbl ph-${ph.phase||''}">${E(ph.label||ph.phase)}</span></div>`;
}

function buildRow(entry){
  const m=entry.snap,fs=entry.frozen_sharp||{},arbs=entry.arbs||[null,null,null,null];
  const sc=fmtScore(m.score);
  const isBL=entry.arb_type_idx===2||entry.arb_type_idx===3;
  const typeBadge=`<span class="badge ${isBL?'badge-bl':'badge-bb'}">${E(entry.arb_type||'')}</span>`;
  const scWarn=entry.score_changed?'<span class="badge badge-warn" title="Score changed during window">sc!</span> ':'';
  const ta=entry.thrill_age!=null?Math.round(entry.thrill_age):null;
  const sa=entry.sharp_age!=null?Math.round(entry.sharp_age):null;
  let h='<tr class="rw">';
  h+=`<td class="l"><span class="p1">${E(m.player1)}</span><br><span class="p2">${E(m.player2)}</span>${phBadge(m.phase)}</td>`;
  h+='<td class="sp"></td>';
  h+=`<td>${sc||'<span style="color:var(--dim)">--</span>'}</td>`;
  h+='<td class="sp"></td>';
  h+=`<td><span class="ov o-t">${F(m.thrill_p1_back)}</span></td>`;
  h+=`<td><span class="ov o-t">${F(m.thrill_p2_back)}</span></td>`;
  h+='<td class="sp"></td>';
  h+=`<td><span class="ov o-s">${F(fs.p1_back)}</span></td>`;
  h+=`<td><span class="ov o-l">${F(fs.p1_lay)}</span></td>`;
  h+=`<td><span class="ov o-s">${F(fs.p2_back)}</span></td>`;
  h+=`<td><span class="ov o-l">${F(fs.p2_lay)}</span></td>`;
  h+='<td class="sp"></td>';
  for(let i=0;i<4;i++) h+=`<td><span class="${arbCls(arbs[i])}">${arbTxt(arbs[i])}</span></td>`;
  h+='<td class="sp"></td>';
  h+=`<td>${scWarn}${typeBadge}</td>`;
  h+=`<td><span class="${AgeCls(ta)}">T:${ta!=null?ta+'s':'--'}</span><br><span class="${AgeCls(sa)}">S:${sa!=null?sa+'s':'--'}</span></td>`;
  h+=`<td style="color:var(--tx3);font-variant-numeric:tabular-nums">${Ago(entry.recorded_ts)}</td>`;
  return h+'</tr>';
}

function renderParams(){
  const p=S.params;
  const PI=(k,l,v,u,st='0.5')=>`<label>${l}<input type="number" step="${st}" value="${v}"
    oninput="S.params['${k}']=parseFloat(this.value)||0">${u}</label>`;
  return `<div class="params">
    <span class="ptitle">Simulation</span>
    <div class="pgrp">
      ${PI('min_arb','Min arb',p.min_arb,'%')}
      ${PI('sharp_delay','Delay',p.sharp_delay,'s')}
      ${PI('window_start','Win start',p.window_start,'s')}
      ${PI('window_end','Win end',p.window_end,'s')}
      ${PI('cooldown','Cooldown',p.cooldown,'s')}
    </div>
    <span class="ptitle">Quality</span>
    <div class="pgrp">
      ${PI('freshness_sec','Freshness',p.freshness_sec,'s')}
      ${PI('lag_sec','Lag',p.lag_sec,'s','0.5')}
      ${PI('max_thrill_odds','MaxOdds',p.max_thrill_odds,'x','1')}
    </div>
    <div class="pgrp">
      ${PI('lookback_hours','Lookback',p.lookback_hours,'h','1')}
      ${PI('limit','Limit',p.limit,'','1')}
    </div>
    <button class="reload-btn" onclick="reload()">&#8635; Recalculate</button>
    <button class="reset-btn" onclick="resetP()">&#8634; Reset</button>
    <button class="reset-btn" onclick="exportCSV()" id="csv-btn" style="display:none">&#8595; CSV</button>
    <span id="spinner" style="color:var(--tx3);font-size:11px;display:none">&#8987;</span>
  </div>`;
}

function render(){
  const hist=(S.data&&S.data.history)||[];
  const stats=(S.data&&S.data.stats)||{};
  let h='';
  h+=`<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap">
    <span style="color:var(--yellow);font-size:15px;font-weight:700">&#9889; Arb History</span>
    <span style="color:var(--tx3);font-size:11px">${E(stats.db_time_range||'')}</span>
  </div>`;
  if(S.data){
    const warned=hist.filter(e=>e.score_changed).length;
    const blCount=hist.filter(e=>e.arb_type_idx===2||e.arb_type_idx===3).length;
    h+=`<div style="margin-bottom:8px;display:flex;gap:14px;flex-wrap:wrap;font-size:11px;color:var(--tx3)">
      <span>Thrill <b style="color:var(--tx)">${stats.thrill_events||0}</b></span>
      <span>Sharp <b style="color:var(--tx)">${stats.sharp_events||0}</b></span>
      <span>Matched <b style="color:var(--tx)">${stats.matched_pairs||0}</b></span>
      <span>Arbs <b style="color:var(--green)">${hist.length}</b></span>
      ${blCount?`<span>back-lay <b style="color:var(--blue)">${blCount}</b></span>`:''}
      ${warned?`<span style="color:var(--orange)">score changed &#9888; ${warned}</span>`:''}
      <span style="color:var(--dim)">${stats.computed_in_ms||0}ms</span>
    </div>`;
  }
  document.getElementById('csv-btn')&&(document.getElementById('csv-btn').style.display=hist.length?'':'none');
  h+=renderParams();
  if(!S.data){
    h+='<div class="empty">Loading...</div>';
  } else if(!hist.length){
    h+='<div class="empty">No arb opportunities found with current params</div>';
  } else {
    const byTour={};
    hist.forEach(e=>{const t=e.snap.tournament||'Unknown';(byTour[t]=byTour[t]||[]).push(e);});
    h+=`<div class="tw"><table><thead><tr>
      <th class="l">Match</th><th class="sp"></th><th>Score</th><th class="sp"></th>
      <th colspan="2" style="color:var(--purple)">Thrill P1/P2</th><th class="sp"></th>
      <th style="color:var(--blue)">P1 back</th><th style="color:var(--red)">P1 lay</th>
      <th style="color:var(--blue)">P2 back</th><th style="color:var(--red)">P2 lay</th>
      <th class="sp"></th>
      <th style="color:var(--dim)" title="Back T.P1 + Back S.P2">bb1</th>
      <th style="color:var(--dim)" title="Back S.P1 + Back T.P2">bb2</th>
      <th style="color:var(--blue)" title="Back T.P1 + Lay S.P1">bl1</th>
      <th style="color:var(--blue)" title="Back T.P2 + Lay S.P2">bl2</th>
      <th class="sp"></th>
      <th>Type</th><th>Ages</th><th>Ago</th>
    </tr></thead><tbody>`;
    for(const [tour,rows] of Object.entries(byTour).sort((a,b)=>a[0].localeCompare(b[0]))){
      h+=`<tr class="tour-hdr"><td colspan="21">
        <span class="tour-name">${E(tour)}</span>
        <span class="tour-cnt">${rows.length} entry${rows.length!==1?'s':''}</span>
      </td></tr>`;
      rows.forEach(e=>h+=buildRow(e));
    }
    h+='</tbody></table></div>';
  }
  h+=`<div class="sbar"><div class="sbar-l">
    <span>DB ${stats.total_rows||0} rows</span>
    <span>Thrill ${stats.thrill_rows||0}</span>
    <span>Sharp ${stats.sharp_rows||0}</span>
    <span>HB ${stats.hb_rows||0}</span>
  </div><button class="tbtn" onclick="TT()">${S.theme==='dark'?'&#9728;':'&#9790;'}</button></div>`;
  document.getElementById('root').innerHTML=h;
}

function TT(){
  S.theme=S.theme==='dark'?'light':'dark';
  document.body.classList.toggle('light');
  try{localStorage.setItem('arb_t',S.theme);}catch(e){}
  render();
}
function resetP(){
  S.params=Object.assign({},_DEF);
  try{localStorage.removeItem('arb_hp');}catch(e){}
  reload();
}
function exportCSV(){
  if(!S.data||!S.data.history||!S.data.history.length) return;
  const rows=S.data.history;
  const cols=['recorded_ago','tournament','player1','player2','score','phase',
              'thrill_p1_back','thrill_p2_back',
              'sharp_p1_back','sharp_p1_lay','sharp_p2_back','sharp_p2_lay',
              'bb1','bb2','bl1','bl2',
              'arb_type','thrill_age','sharp_age','score_changed',
              'th_event_id','sh_event_id'];
  function fmtScore(sc){
    if(!sc||!(sc.period_scores||[]).length) return '';
    const ps=sc.period_scores;
    const p1=ps.map(p=>p.home_score).join(' ');
    const p2=ps.map(p=>p.away_score).join(' ');
    const hg=sc.home_gamescore!=null?sc.home_gamescore:'';
    const ag=sc.away_gamescore!=null?sc.away_gamescore:'';
    return `${p1} (${hg}) / ${p2} (${ag})`;
  }
  function q(v){
    if(v==null) return '';
    const s=String(v);
    return s.includes(',')||s.includes('"')||s.includes('\n')?`"${s.replace(/"/g,'""')}"`:s;
  }
  const lines=[cols.join(',')];
  rows.forEach(e=>{
    const m=e.snap,fs=e.frozen_sharp||{},arbs=e.arbs||[null,null,null,null];
    const ago=Math.round(Date.now()/1000-e.recorded_ts);
    const phase=m.phase?m.phase.label||m.phase.phase||'':'';
    lines.push([
      ago+'s ago',
      m.tournament,m.player1,m.player2,
      fmtScore(m.score),phase,
      m.thrill_p1_back,m.thrill_p2_back,
      fs.p1_back,fs.p1_lay,fs.p2_back,fs.p2_lay,
      arbs[0]!=null?arbs[0].toFixed(2):null,
      arbs[1]!=null?arbs[1].toFixed(2):null,
      arbs[2]!=null?arbs[2].toFixed(2):null,
      arbs[3]!=null?arbs[3].toFixed(2):null,
      e.arb_type,e.thrill_age,e.sharp_age,
      e.score_changed?1:0,
      e.th_event_id,e.sh_event_id
    ].map(q).join(','));
  });
  const blob=new Blob([lines.join('\n')],{type:'text/csv'});
  const a=document.createElement('a');
  a.href=URL.createObjectURL(blob);
  const d=new Date().toISOString().slice(0,16).replace('T','_').replace(':','-');
  a.download=`arb_history_${d}.csv`;
  a.click();
}

function reload(){
  try{localStorage.setItem('arb_hp',JSON.stringify(S.params));}catch(e){}
  document.querySelectorAll('#spinner').forEach(el=>el.style.display='inline');
  const q=new URLSearchParams(S.params).toString();
  fetch('/history-data?'+q)
    .then(r=>r.json())
    .then(d=>{S.data=d;render();})
    .catch(e=>{console.error(e);document.querySelectorAll('#spinner').forEach(el=>el.style.display='none');});
}
reload();
</script>
</body>
</html>"""


# ── Debug Log HTML ────────────────────────────────────────────────────────────
_LOG_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Collector Log</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d0d0d;color:#ccc;font:12px/1.5 'Courier New',monospace}
#bar{position:fixed;top:0;left:0;right:0;background:#111;border-bottom:1px solid #1e1e1e;
     padding:8px 16px;display:flex;gap:16px;align-items:center;z-index:9}
#bar h1{font-size:13px;color:#fff;letter-spacing:.05em}
.cnt{color:#555;font-size:11px}.cnt b{color:#aaa}
#pause{margin-left:auto;background:#1a1a1a;border:1px solid #2a2a2a;color:#aaa;
       padding:4px 14px;cursor:pointer;border-radius:3px;font:inherit}
#pause:hover{color:#fff}
#dot{width:8px;height:8px;border-radius:50%;background:#ef4444;flex-shrink:0}
#dot.on{background:#22c55e;box-shadow:0 0 6px #22c55e}
#log{margin-top:38px}
.row{display:grid;grid-template-columns:68px 110px 1fr;gap:0 10px;
     padding:3px 16px;border-bottom:1px solid #111}
.row:hover{background:#131313}
.ts{color:#3f3f3f}.src{font-weight:700;white-space:nowrap}
.src-THRILL{color:#a78bfa}.src-SHARP{color:#38bdf8}
.src-SHARPSCORE{color:#6ee7b7}.src-SHARPCATALOG{color:#555}
.detail{color:#52525b;word-break:break-all}
.detail .v{color:#86efac}.detail .name{color:#e2e8f0}
.detail .mkt{color:#f59e0b}.detail .tour{color:#818cf8}
</style>
</head>
<body>
<div id="bar">
  <span id="dot"></span>
  <h1>ODDS COLLECTOR</h1>
  <div class="cnt">thrill <b id="ct">0</b></div>
  <div class="cnt">sharp <b id="cs">0</b></div>
  <div class="cnt">score <b id="csc">0</b></div>
  <div class="cnt">total <b id="ctot">0</b></div>
  <button id="pause" onclick="toggle()">&#9646;&#9646; Pause</button>
</div>
<div id="log"></div>
<script>
let paused=false, ct=0, cs=0, csc=0;
const logEl=document.getElementById('log');
const dot=document.getElementById('dot');
function toggle(){paused=!paused;document.getElementById('pause').textContent=paused?'&#9654; Resume':'&#9646;&#9646; Pause';}
function colorize(s){
  return s
    .replace(/(\\d+\\.\\d+)/g,'<span class="v">$1</span>')
    .replace(/\\|([^|]+)\\|/g,'|<span class="name">$1</span>|')
    .replace(/mkt=([^|]+)/,'mkt=<span class="mkt">$1</span>')
    .replace(/tour=([^|]+)/,'tour=<span class="tour">$1</span>');
}
function addRow(line){
  const parts=line.split('\\t');
  if(parts.length<3) return;
  const [ts,src,detail]=parts;
  const k=src.trim().replace(/-/g,'');
  if(k==='THRILL') ct++;
  else if(k==='SHARP') cs++;
  else if(k==='SHARPSCORE') csc++;
  document.getElementById('ct').textContent=ct;
  document.getElementById('cs').textContent=cs;
  document.getElementById('csc').textContent=csc;
  document.getElementById('ctot').textContent=ct+cs+csc;
  const div=document.createElement('div');
  div.className='row';
  div.innerHTML=`<span class="ts">${ts}</span><span class="src src-${k}">[${src.trim()}]</span><span class="detail">${colorize(detail)}</span>`;
  logEl.prepend(div);
  while(logEl.children.length>400) logEl.removeChild(logEl.lastChild);
}
const es=new EventSource('/log-stream');
es.onopen=()=>dot.classList.add('on');
es.onerror=()=>dot.classList.remove('on');
es.onmessage=e=>{if(!paused) addRow(e.data);};
fetch('/log-history').then(r=>r.json()).then(lines=>{lines.slice(-150).forEach(addRow);});
</script>
</body>
</html>"""