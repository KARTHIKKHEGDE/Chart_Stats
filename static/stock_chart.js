(function(global){
  const $=selector=>document.querySelector(selector);
  const formatDateMDY=value=>{
    const raw=String(value??'');
    const match=raw.slice(0,10).match(/^(\d{4})-(\d{2})-(\d{2})$/);
    return match?`${match[2]}/${match[3]}/${match[1]}`:raw;
  };
  const _istClock=new Intl.DateTimeFormat('en-US',{timeZone:'Asia/Kolkata',month:'2-digit',day:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
  const _etClock=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
  function istFromEasternClock(day,minute,includeDate=true){
    const [year,month,date]=String(day||'').split('-').map(Number),clock=Math.max(0,Math.min(1439,Math.round(Number(minute)||0)));
    if(!year||!month||!date)return 'time unavailable';
    const civil=Date.UTC(year,month-1,date,Math.floor(clock/60),clock%60);
    const et=Object.fromEntries(_etClock.formatToParts(new Date(civil)).map(part=>[part.type,part.value]));
    const easternCivil=Date.UTC(Number(et.year),Number(et.month)-1,Number(et.day),Number(et.hour),Number(et.minute));
    const ist=Object.fromEntries(_istClock.formatToParts(new Date(civil-(easternCivil-civil))).map(part=>[part.type,part.value]));
    return `${includeDate?`${ist.month}/${ist.day}/${ist.year} `:''}${ist.hour}:${ist.minute} IST`;
  }
const MC={"2m ORB low break":"#ff5a5a","5m ORB low break":"#ffd23f","Short at open":"#ff9f43"};
const MA={"2m ORB low break":"2m ORB","5m ORB low break":"5m ORB","Short at open":"Open"};
const hm=(m,off)=>{m=((m+off)%1440+1440)%1440;return String(Math.floor(m/60)).padStart(2,'0')+':'+String(m%60).padStart(2,'0')};
function tri(x,px,py,dir,col){x.fillStyle=col;x.beginPath();
  if(dir==='down'){x.moveTo(px-5,py-9);x.lineTo(px+5,py-9);x.lineTo(px,py-1);}
  else{x.moveTo(px-5,py+9);x.lineTo(px+5,py+9);x.lineTo(px,py+1);}
  x.closePath();x.fill();}

function bone(b){
  const n=b.c.length,a9=2/10,a21=2/22,e9=[],e21=[],bu=[],bl=[];
  for(let i=0;i<n;i++){
    e9.push(i?a9*b.c[i]+(1-a9)*e9[i-1]:b.c[i]); e21.push(i?a21*b.c[i]+(1-a21)*e21[i-1]:b.c[i]);
    if(i>=19){let s=0;for(let k=i-19;k<=i;k++)s+=b.c[k];const m=s/20;let v=0;for(let k=i-19;k<=i;k++)v+=(b.c[k]-m)**2;const sd=Math.sqrt(v/20);bu.push(m+2*sd);bl.push(m-2*sd);}
    else{bu.push(null);bl.push(null);}
  }
  return {e9,e21,bu,bl};
}
function aggregateEvBars(source, tf){
  if(!source || !source.t || tf===1 || tf==='1d') return source;
  const out={t:[],d:[],o:[],h:[],l:[],c:[],v:[],vw:[]}, groups=new Map();
  for(let i=0;i<source.t.length;i++){
    const start=Math.floor(source.t[i]/tf)*tf,day=source.d?.[i]||'';
    const key=day+'|'+start;
    if(!groups.has(key)) groups.set(key,[]);
    groups.get(key).push(i);
  }
  for(const [key,ids] of groups){
    const start=Number(key.split('|').pop());out.t.push(start);out.d.push(source.d?.[ids[0]]||'');out.o.push(source.o[ids[0]]);out.h.push(Math.max(...ids.map(i=>source.h[i])));
    out.l.push(Math.min(...ids.map(i=>source.l[i])));out.c.push(source.c[ids[ids.length-1]]);
    out.v.push(ids.reduce((sum,i)=>sum+(source.v[i]||0),0));out.vw.push(source.vw[ids[ids.length-1]]);
  }
  return out;
}
function gapRangeResolve(point,g){
  if(!point||!g||!g.bars)return -1;const b=g.bars,n=b.t.length,dates=b.d||[];let first=0,last=n;
  if(dates.length){let lo=0,hi=n;while(lo<hi){const mid=(lo+hi)>>1;if((dates[mid]||g.chart.date)<point.date)lo=mid+1;else hi=mid;}first=lo;lo=first;hi=n;while(lo<hi){const mid=(lo+hi)>>1;if((dates[mid]||g.chart.date)<=point.date)lo=mid+1;else hi=mid;}last=lo;}
  if(first>=last)return -1;const dayIndex=g.multi?g.dates.indexOf(point.date):-1,minuteAt=i=>b.t[i]-(dayIndex>=0?dayIndex*g.sessionSpan:0);
  let lo=first,hi=last;while(lo<hi){const mid=(lo+hi)>>1;if(minuteAt(mid)<point.minute)lo=mid+1;else hi=mid;}
  if(lo>=last)return last-1;if(lo===first)return lo;return Math.abs(minuteAt(lo)-point.minute)<Math.abs(minuteAt(lo-1)-point.minute)?lo:lo-1;
}
function gapRangePointAtIndex(g,index){
  const b=g.bars,day=b.d?.[index]||g.chart.date,dayIndex=g.multi?g.dates.indexOf(day):-1;
  const minute=b.t[index]-(dayIndex>=0?dayIndex*g.sessionSpan:0),hh=String(Math.floor(minute/60)).padStart(2,'0'),mm=String(minute%60).padStart(2,'0');
  return {date:day,minute,price:b.c[index],label:istFromEasternClock(day,minute)};
}
function gapRangeX(point,g){if(Number.isFinite(point.plotTime))return g.X(point.plotTime);const dayIndex=g.multi?g.dates.indexOf(point.date):-1;return g.X(point.minute+(dayIndex>=0?dayIndex*g.sessionSpan:0));}
function gapRangeTimestamp(point,offsetForDay){
  const [y,m,d]=point.date.split('-').map(Number),etOffset=330-offsetForDay(point.date);
  return Date.UTC(y,m-1,d,0,point.minute-etOffset);
}
function gapRangeStats(measure,g){
  const a=gapRangeResolve(measure.a,g),z=gapRangeResolve(measure.b,g);if(a<0||z<0)return null;
  const from=Math.min(a,z),to=Math.max(a,z),b=g.bars,bars=to-from+1;let cache=b._rangeStatsCache;
  if(!cache){const n=b.t.length,volumePrefix=new Float64Array(n+1),dollarPrefix=new Float64Array(n+1);
    for(let i=0;i<n;i++){const v=Number(b.v[i])||0;volumePrefix[i+1]=volumePrefix[i]+v;dollarPrefix[i+1]=dollarPrefix[i]+(Number(b.c[i])||0)*v;}
    cache=b._rangeStatsCache={volumePrefix,dollarPrefix};}
  const volume=cache.volumePrefix[to+1]-cache.volumePrefix[from],dollarVolume=cache.dollarPrefix[to+1]-cache.dollarPrefix[from];
  const change=measure.b.price-measure.a.price,pct=measure.a.price?change/measure.a.price*100:0;
  return {a,z,from,to,bars,volume,dollarVolume,change,pct};
}
function drawGapDeepDiveRanges(cv,x,g){
  const measures=cv._rangeMeasures||[],palette=['#62c8ff','#ffd23f','#ff78cb','#b8ff6e','#ff9f43'];cv._rangeScreenMeasures=[];
  if(!measures.length){if(cv._rangeMeasureBox){cv._rangeMeasureBox.hidden=true;cv._rangeMeasureBox.textContent='';}return;}
  x.save();x.beginPath();x.rect(g.L,g.T,g.plotW,g.plotH);x.clip();
  for(let n=0;n<measures.length;n++){
    const measure=measures[n],stats=gapRangeStats(measure,g);if(!stats)continue;
    const a={x:gapRangeX(measure.a,g),y:g.Y(measure.a.price)},b={x:gapRangeX(measure.b,g),y:g.Y(measure.b.price)},color=palette[n%palette.length];
    x.strokeStyle=color;x.lineWidth=cv._selectedRangeId===measure.id?2.8:1.8;x.beginPath();x.moveTo(a.x,a.y);x.lineTo(b.x,b.y);x.stroke();
    [a,b].forEach(pt=>{x.fillStyle='#090c10';x.strokeStyle=color;x.lineWidth=2;x.beginPath();x.arc(pt.x,pt.y,5,0,Math.PI*2);x.fill();x.stroke();});
    cv._rangeScreenMeasures.push({id:measure.id,a,b,color});
  }
  x.restore();
  const selected=measures.find(m=>m.id===cv._selectedRangeId),stats=selected?gapRangeStats(selected,g):null,box=cv._rangeMeasureBox;
  if(box&&stats){const sign=stats.change>=0?'+':'−',pctSign=stats.pct>=0?'+':'−',money=n=>'$'+Math.abs(n).toFixed(Math.abs(n)<1?4:2),millions=n=>(Number(n)/1e6).toFixed(2)+'M',float=Number(cv._floatShares??g.chart?.float_shares),compact=n=>Number.isFinite(n)?new Intl.NumberFormat('en-US',{notation:'compact',maximumFractionDigits:2}).format(n):'Unavailable',rotation=float>0?`${(stats.volume/float).toFixed(2)}x`:'Unavailable';
    box.hidden=false;box.style.borderColor=stats.change>=0?'#3ddc84':'#ff5a5a';box.style.color=stats.change>=0?'#d8ffe8':'#ffe1e1';
    box.textContent=`% Change  ${pctSign}${Math.abs(stats.pct).toFixed(2)}%\n$ Change  ${sign}${money(stats.change)}\nVolume  ${millions(stats.volume)} sh\n$ Volume  $${millions(stats.dollarVolume)}\nFloat  ${compact(float)} sh\nFloat rotation  ${rotation}`;
    const line=cv._rangeScreenMeasures.find(item=>item.id===selected.id),wrap=box.parentElement;if(line&&wrap){const W=wrap.clientWidth,H=wrap.clientHeight,bw=box.offsetWidth,bh=box.offsetHeight,mx=(line.a.x+line.b.x)/2,my=(line.a.y+line.b.y)/2;let left=mx+14,top=my+14;if(left+bw>W-8)left=mx-bw-14;if(top+bh>H-8)top=my-bh-14;box.style.left=Math.max(8,Math.min(W-bw-8,left))+'px';box.style.top=Math.max(8,Math.min(H-bh-8,top))+'px';box.style.right='auto';}
  }else if(box){box.hidden=true;box.textContent='';}
}
function drawGapDeepDiveRangeOverlay(cv){
  const overlay=cv._rangeOverlay||$('#gap_chart_range_overlay'),g=cv._rangeGeometry;if(!overlay||!g)return;
  const W=cv.clientWidth,H=cv.clientHeight;if(W<2||H<2)return;
  if(overlay.width!==cv.width)overlay.width=cv.width;if(overlay.height!==cv.height)overlay.height=cv.height;if(overlay.style.width!==W+'px')overlay.style.width=W+'px';if(overlay.style.height!==H+'px')overlay.style.height=H+'px';
  const x=overlay.getContext('2d');x.setTransform(overlay.width/W,0,0,overlay.height/H,0,0);x.clearRect(0,0,W,H);
  drawGapDeepDiveRanges(cv,x,g);
  const z=cv._zoomDrag;if(z){const left=Math.min(z.x0,z.x1),width=Math.abs(z.x1-z.x0),top=g.T,height=g.plotH;x.save();x.fillStyle='rgba(98,200,255,.14)';x.fillRect(left,top,width,height);x.strokeStyle='#62c8ff';x.lineWidth=1.5;x.setLineDash([5,4]);x.strokeRect(left,top,width,height);x.restore();}
}
function scheduleGapDeepDiveRangeOverlay(cv){if(cv._rangeOverlayFrame)return;cv._rangeOverlayFrame=requestAnimationFrame(()=>{cv._rangeOverlayFrame=0;drawGapDeepDiveRangeOverlay(cv);});}
function drawGapDeepDiveCrosshair(cv){const g=cv._rangeGeometry,p=cv._crosshair,v=cv._crosshairV||$('#gap_chart_crosshair_v'),h=cv._crosshairH||$('#gap_chart_crosshair_h');let priceLabel=cv._priceLabel||(cv.id==='gap_chart_modal_canvas'?$('#gap_chart_price_label'):null);if(!priceLabel&&cv._crosshairH?.parentElement){const wrap=cv._crosshairH.parentElement;priceLabel=wrap.querySelector('.chart-price-axis-label');if(!priceLabel){priceLabel=document.createElement('div');priceLabel.className='chart-price-axis-label';priceLabel.hidden=true;priceLabel.style.cssText='position:absolute;z-index:5;left:3px;padding:2px 4px;background:#183248;border:1px solid #62c8ff;color:#eef8ff;font:11px Consolas,monospace;line-height:14px;text-align:right;pointer-events:none';wrap.appendChild(priceLabel);}cv._priceLabel=priceLabel;}if(!v||!h)return;const hide=()=>{v.hidden=true;h.hidden=true;if(priceLabel)priceLabel.hidden=true;};if(!cv._showCrosshair||!g||!p||p.x<g.L||p.x>g.L+g.plotW||p.y<g.T||p.y>g.T+g.plotH){hide();return;}v.hidden=false;h.hidden=false;v.style.left=p.x+'px';v.style.top=g.T+'px';v.style.height=((g.crosshairBottom||g.T+g.plotH)-g.T)+'px';h.style.left=g.L+'px';h.style.top=p.y+'px';h.style.width=g.plotW+'px';if(priceLabel){const price=g.yMax-(p.y-g.T)/g.plotH*(g.yMax-g.yMin),digits=Math.abs(price)<1?4:2;priceLabel.textContent='$'+price.toFixed(digits);priceLabel.hidden=false;priceLabel.style.width=Math.max(42,g.L-10)+'px';priceLabel.style.top=Math.max(g.T,Math.min(g.T+g.plotH-20,p.y-9))+'px';}}
function gapDeepDiveEventMarkerAt(cv,x,y){return (cv._eventMarkersScreen||[]).find(marker=>x>=marker.x&&x<=marker.x+marker.w&&y>=marker.y&&y<=marker.y+marker.h)||null;}
function setGapDeepDiveZoomView(cv,g,zoom,targetStart){
  const span=Math.min(g.span,g.baseViewSpan/zoom),panLimit=Math.max(0,g.span-span)/2;
  let viewBase=g.fullStart+panLimit,panMin=-panLimit,panMax=panLimit;
  if(g.multi){const anchorIndex=g.dates.indexOf(cv._anchorDate||g.chart.date);if(anchorIndex>=0){viewBase=g.fullStart+anchorIndex*g.sessionSpan+g.sessionSpan/2-span/2;panMin=g.fullStart-viewBase;panMax=g.fullStart+g.span-span-viewBase;}}
  cv._rangeZoom=zoom;cv._panOffset=Math.max(panMin,Math.min(panMax,targetStart-viewBase));
}
function drawGapDeepDiveEventMarkers(cv,x,g){
  const events=cv._marketEvents||[],b=g?.bars;if(!events.length||!b?.t?.length){cv._eventMarkersScreen=[];return;}
  const placed=[],counts=new Map();x.save();x.font='bold 10px Consolas,monospace';x.textAlign='center';x.textBaseline='middle';
  for(const event of events){
    const eventStamp=Date.parse(`${event.chart_date}T00:00:00Z`)+Number(event.minute_et??570)*60000;
    let best=-1,bestDistance=Infinity;
    for(let i=0;i<b.t.length;i++){
      const barDate=b.d?.[i]||g.chart.date;if(!barDate)continue;
      const dayIndex=g.multi?Math.max(0,g.dates.indexOf(barDate)):0,minute=g.daily?720:(g.multi?b.t[i]-dayIndex*g.sessionSpan:b.t[i]);
      const barStamp=Date.parse(`${barDate}T00:00:00Z`)+minute*60000,distance=Math.abs(barStamp-eventStamp);
      if(distance<bestDistance){best=i;bestDistance=distance;}
    }
    if(best<0||!Number.isFinite(bestDistance)||bestDistance>72*60*60000)continue;
      const barTime=g.daily?b.t[best]+.5:b.t[best]+g.tf/2,centerX=g.X(barTime);
    if(centerX<g.L-10||centerX>g.L+g.plotW+10)continue;
    const barHigh=Number(b.h[best])||g.yMax,baseY=Math.max(g.T+3,Math.min(g.T+g.plotH-18,g.Y(barHigh)-22)),stackKey=`${best}|${event.chart_date}`,stack=counts.get(stackKey)||0;counts.set(stackKey,stack+1);
    const w=18,h=16,px=Math.max(g.L,Math.min(g.L+g.plotW-w,centerX-w/2+stack*20)),py=Math.max(g.T,baseY-stack*17),isNews=event.kind==='news'||String(event.kind||'').toLowerCase().includes('news'),color=isNews?'#ffd23f':'#62c8ff',label=isNews?'N':'S';
    x.fillStyle='rgba(7,12,18,.97)';x.strokeStyle=color;x.lineWidth=1.2;x.beginPath();x.roundRect(px,py,w,h,3);x.fill();x.stroke();x.fillStyle=color;x.fillText(label,px+w/2,py+h/2+.5);
    const time=event.time_exact?istFromEasternClock(event.chart_date,event.minute_et):'time unavailable · marker placed at session open';
    const newsSource=[event.source,event.publisher].filter(Boolean).join(' · ')||'News';
    const baseTitle=`${isNews?newsSource+' news':'SEC '+(event.form||'filing')} · ${time}\n${event.title}`;
    placed.push({x:px,y:py,w,h,url:event.url,event,isNews,baseTitle,title:baseTitle});
  }
  x.restore();cv._eventMarkersScreen=placed;
}
function scheduleGapDeepDiveHover(cv,x,y){cv._pendingHover={x,y};if(cv._hoverUpdateTimer)return;cv._hoverUpdateTimer=setTimeout(()=>{cv._hoverUpdateTimer=0;const p=cv._pendingHover;if(p)cv._showChartHover?.(p.x,p.y);},16);}
function gapDeepDiveRangePoint(cv,x,y){return cv._rangePointAt?.(x,y)||cv._rangePointAtX?.(x)||null;}
function bindGapDeepDiveRangeInput(cv){
  if(cv._rangeInputBound)return;cv._rangeInputBound=true;
  if(!cv._eventHoverTip&&cv.parentElement){const tip=document.createElement('div');tip.style.cssText='display:none;position:absolute;z-index:20;pointer-events:none;max-width:min(520px,84vw);white-space:pre-line;background:rgba(8,12,16,.97);border:1px solid #ffd23f;color:#f1f5f9;padding:10px 12px;border-radius:4px;font:13px/1.5 Consolas,monospace;box-shadow:0 2px 10px #000';cv.parentElement.appendChild(tip);cv._eventHoverTip=tip;}
  const stop=e=>{e.preventDefault();e.stopPropagation();e.stopImmediatePropagation();};
  cv.addEventListener('pointerdown',e=>{
    const eventMarker=gapDeepDiveEventMarkerAt(cv,e.offsetX,e.offsetY);if(eventMarker&&eventMarker.url){cv._eventMarkerPointer={marker:eventMarker,pointerId:e.pointerId};cv.setPointerCapture(e.pointerId);cv.style.cursor='pointer';stop(e);return;}
    if(cv._zoomSelectMode&&e.button===0&&cv._rangeGeometry){const g=cv._rangeGeometry;if(e.offsetX<g.L||e.offsetX>g.L+g.plotW||e.offsetY<g.T||e.offsetY>g.T+g.plotH)return;cv._zoomDrag={x0:e.offsetX,y0:g.T,x1:e.offsetX,y1:g.T+g.plotH};cv.setPointerCapture(e.pointerId);stop(e);drawGapDeepDiveRangeOverlay(cv);return;}
    if(!cv._rangeMeasureMode||e.button!==0||!cv._rangeGeometry)return;
    const g=cv._rangeGeometry;if(e.offsetX<g.L||e.offsetX>g.L+g.plotW||e.offsetY<g.T||e.offsetY>g.T+g.plotH)return;
    const hit=(cv._rangeScreenMeasures||[]).map(m=>({m,which:['a','b'].sort((u,v)=>Math.hypot(m[u].x-e.offsetX,m[u].y-e.offsetY)-Math.hypot(m[v].x-e.offsetX,m[v].y-e.offsetY))[0]})).find(({m,which})=>Math.hypot(m[which].x-e.offsetX,m[which].y-e.offsetY)<12);
    let drag;
    if(hit){const measure=cv._rangeMeasures.find(m=>m.id===hit.m.id);cv._selectedRangeId=hit.m.id;drag={kind:'endpoint',id:hit.m.id,which:hit.which,original:{...measure[hit.which]}};}
    else{
      const p=gapDeepDiveRangePoint(cv,e.offsetX,e.offsetY);if(!p)return;
      const lineHit=(cv._rangeScreenMeasures||[]).find(m=>{const dx=m.b.x-m.a.x,dy=m.b.y-m.a.y,t=Math.max(0,Math.min(1,((e.offsetX-m.a.x)*dx+(e.offsetY-m.a.y)*dy)/(dx*dx+dy*dy||1)));return Math.hypot(e.offsetX-(m.a.x+t*dx),e.offsetY-(m.a.y+t*dy))<8;});
      if(lineHit){const measure=cv._rangeMeasures.find(m=>m.id===lineHit.id),i=gapRangeResolve(p,g);cv._selectedRangeId=lineHit.id;drag={kind:'move',id:lineHit.id,index:i,price:p.price,a:{...measure.a},b:{...measure.b}};}
      else{const measure={id:crypto.randomUUID(),a:p,b:p};cv._rangeMeasures||=[];cv._rangeMeasures.push(measure);cv._selectedRangeId=measure.id;drag={kind:'create',id:measure.id,which:'b'};}
    }
    cv._rangeDrag=drag;cv.setPointerCapture(e.pointerId);cv.style.cursor='crosshair';stop(e);drawGapDeepDiveRangeOverlay(cv);
  },true);
  cv.addEventListener('pointermove',e=>{const marker=gapDeepDiveEventMarkerAt(cv,e.offsetX,e.offsetY),tip=cv._eventHoverTip;if(marker){cv._activeEventMarker=marker;cv.style.cursor='pointer';cv.removeAttribute('title');if(tip){tip.textContent=marker.title;tip.style.whiteSpace='pre-wrap';tip.style.display='block';const parent=cv.parentElement,w=tip.offsetWidth,h=tip.offsetHeight;tip.style.left=Math.max(4,Math.min(parent.clientWidth-w-4,cv.offsetLeft+e.offsetX+12))+'px';tip.style.top=Math.max(4,Math.min(parent.clientHeight-h-4,cv.offsetTop+e.offsetY+12))+'px';}const summaryRequest=window.requestSecMarkerAiSummary;if(!marker.isNews&&!marker.aiRequested&&typeof summaryRequest==='function'&&(marker.event?.accession||marker.event?.document_url||marker.event?.url)){marker.aiRequested=true;marker.title=`${marker.baseTitle}\n\nAI summary: Loading…`;if(tip)tip.textContent=marker.title;Promise.resolve(summaryRequest(marker.event,cv._eventTicker||'')).then(summary=>{marker.aiSummary=String(summary||'No AI summary available.');marker.title=`${marker.baseTitle}\n\nAI summary: ${marker.aiSummary}`;if(cv._activeEventMarker===marker&&tip)tip.textContent=marker.title;},error=>{marker.aiSummary=`Unavailable: ${error?.message||'could not generate summary.'}`;marker.title=`${marker.baseTitle}\n\nAI summary: ${marker.aiSummary}`;if(cv._activeEventMarker===marker&&tip)tip.textContent=marker.title;});}}else{cv._activeEventMarker=null;if(tip)tip.style.display='none';if(!cv._rangeDrag&&!cv._zoomDrag&&!cv._panActive)cv.style.cursor=(cv._rangeMeasureMode||cv._zoomSelectMode)?'crosshair':'grab';}},true);
  cv.addEventListener('pointermove',e=>{
    if(cv._zoomDrag){cv._zoomDrag.x1=e.offsetX;cv._zoomDrag.y1=cv._rangeGeometry.T+cv._rangeGeometry.plotH;stop(e);scheduleGapDeepDiveRangeOverlay(cv);return;}
    const drag=cv._rangeDrag,g=cv._rangeGeometry;if(!drag||!g)return;
    const measure=cv._rangeMeasures.find(m=>m.id===drag.id),p=gapDeepDiveRangePoint(cv,e.offsetX,e.offsetY);if(!measure||!p)return;
    if(drag.kind==='move'){
      const idx=gapRangeResolve(p,g),delta=idx-drag.index,priceDelta=p.price-drag.price;
      const movePoint=original=>{const old=gapRangeResolve(original,g),next=gapRangePointAtIndex(g,Math.max(0,Math.min(g.bars.t.length-1,old+delta)));return {...next,price:original.price+priceDelta};};
      measure.a=movePoint(drag.a);measure.b=movePoint(drag.b);
    }else measure[drag.which]=p;
    cv._selectedRangeId=measure.id;stop(e);scheduleGapDeepDiveRangeOverlay(cv);
  },true);
  const finish=e=>{
    if(cv._eventMarkerPointer){const {marker,pointerId}=cv._eventMarkerPointer;cv._eventMarkerPointer=null;cv.style.cursor=(cv._rangeMeasureMode||cv._zoomSelectMode)?'crosshair':'grab';if(e.pointerId===pointerId&&e.offsetX>=marker.x&&e.offsetX<=marker.x+marker.w&&e.offsetY>=marker.y&&e.offsetY<=marker.y+marker.h)window.open(marker.url,'_blank','noopener,noreferrer');stop(e);return;}
    if(cv._zoomDrag){const z=cv._zoomDrag;cv._zoomDrag=null;const g=cv._rangeGeometry,dx=Math.abs(z.x1-z.x0);if(dx>=12&&g){const left=Math.max(g.L,Math.min(z.x0,z.x1)),right=Math.min(g.L+g.plotW,Math.max(z.x0,z.x1)),requestedSpan=Math.max(1,(right-left)*g.viewSpan/g.plotW),minZoom=g.baseViewSpan/g.span,nextZoom=Math.max(minZoom,Math.min(24,g.baseViewSpan/requestedSpan)),targetStart=g.viewStart+((left-g.L)/g.plotW)*g.viewSpan;setGapDeepDiveZoomView(cv,g,nextZoom,targetStart);cv._yZoom=1;cv._gapDeepDiveRedraw?.();}else drawGapDeepDiveRangeOverlay(cv);cv.style.cursor=(cv._rangeMeasureMode||cv._zoomSelectMode)?'crosshair':'grab';stop(e);return;}
    if(!cv._rangeDrag)return;cv._rangeDrag=null;cv.style.cursor=(cv._rangeMeasureMode||cv._zoomSelectMode)?'crosshair':'grab';stop(e);drawGapDeepDiveRangeOverlay(cv);
  };
  cv.addEventListener('pointerup',finish,true);cv.addEventListener('pointercancel',finish,true);
  cv.addEventListener('dblclick',e=>{if(!cv._fullFeatures)return;cv._rangeZoom=1;cv._panOffset=0;cv._yZoom=1;cv._gapDeepDiveRedraw?.();},true);
  cv.addEventListener('wheel',e=>{
  if(!cv._fullFeatures||!cv._wheelZoomEnabled||!cv._rangeGeometry)return;
    const g=cv._rangeGeometry;if(e.offsetX<g.L||e.offsetX>g.L+g.plotW)return;
    const ratio=(e.offsetX-g.L)/g.plotW,anchor=g.viewStart+ratio*g.viewSpan,rawZoom=(cv._rangeZoom||1)*Math.exp(-Math.sign(e.deltaY)*.18),minZoom=g.baseViewSpan/g.span;
    if(e.deltaY>0&&rawZoom<minZoom-1e-6&&cv._onZoomOutMore){const requestedSessions=Math.ceil(g.baseViewSpan/rawZoom)+1;Promise.resolve(cv._onZoomOutMore(requestedSessions)).then(added=>{if(added){cv._wheelZoomMore?.(ratio,-1);return;}const zoom=minZoom,nextSpan=Math.min(g.span,g.baseViewSpan/zoom);setGapDeepDiveZoomView(cv,g,zoom,anchor-ratio*nextSpan);cv._gapDeepDiveRedraw?.();}).catch(()=>{});e.preventDefault();e.stopPropagation();e.stopImmediatePropagation();return;}
    const zoom=Math.max(minZoom,Math.min(24,rawZoom)),nextSpan=Math.min(g.span,g.baseViewSpan/zoom);
    setGapDeepDiveZoomView(cv,g,zoom,anchor-ratio*nextSpan);
    e.preventDefault();e.stopPropagation();e.stopImmediatePropagation();cv._gapDeepDiveRedraw?.();
  },{capture:true,passive:false});
}
function drawGapDeepDiveNavigator(mainCv,baseChart,timeframe){
  const nav=mainCv._navigator||$('#gap_chart_navigator');if(!nav||!mainCv._rangeGeometry)return;const g=mainCv._rangeGeometry,b=g.bars,W=nav.clientWidth,H=nav.clientHeight,dpr=Math.min(2,window.devicePixelRatio||1);
  if(W<2||H<2)return;nav.width=Math.round(W*dpr);nav.height=Math.round(H*dpr);nav.style.width=W+'px';nav.style.height=H+'px';const x=nav.getContext('2d');x.setTransform(dpr,0,0,dpr,0,0);x.clearRect(0,0,W,H);x.fillStyle='#090c10';x.fillRect(0,0,W,H);
  const pad=8,plotW=W-pad*2,lo=Math.min(...b.l),hi=Math.max(...b.h),range=hi-lo||1,at=t=>pad+plotW*(t-g.fullStart)/g.span;
  x.strokeStyle='#5c788e';x.lineWidth=1;x.beginPath();let started=false;for(let i=0;i<b.t.length;i++){const px=at(b.t[i]),py=H-7-(b.c[i]-lo)/(range)*(H-14);if(px<pad||px>W-pad)continue;if(!started){x.moveTo(px,py);started=true;}else x.lineTo(px,py);}x.stroke();
  const left=Math.max(pad,Math.min(W-pad,at(g.viewStart))),right=Math.max(left+10,Math.min(W-pad,at(g.viewStart+g.viewSpan)));nav._gapNav={left,right,pad,plotW,geometry:g};
  x.fillStyle='rgba(98,200,255,.17)';x.fillRect(left,2,right-left,H-4);x.strokeStyle='#62c8ff';x.lineWidth=1.5;x.strokeRect(left+.5,2.5,right-left-1,H-5);
  x.fillStyle='#d9f2ff';x.fillRect(left-3,2,6,H-4);x.fillRect(right-3,2,6,H-4);
  if(!nav._gapNavBound){
    nav._gapNavBound=true;let drag=null;
    nav.addEventListener('pointerdown',e=>{const q=nav._gapNav;if(!q)return;const px=e.offsetX;drag={x:px,left:q.left,right:q.right,mode:px<q.left+9?'left':px>q.right-9?'right':px<q.right?'body':'jump'};nav.setPointerCapture(e.pointerId);nav.style.cursor=drag.mode==='body'?'grabbing':'ew-resize';e.preventDefault();});
    nav.addEventListener('pointermove',e=>{if(!drag||!nav._gapNav)return;const q=nav._gapNav,g=q.geometry,dx=e.offsetX-drag.x;let left=drag.left,right=drag.right;if(drag.mode==='body'){left+=dx;right+=dx;}else if(drag.mode==='left')left+=dx;else if(drag.mode==='right')right+=dx;else{left=e.offsetX-(right-left)/2;right=e.offsetX+(right-left)/2;}left=Math.max(q.pad,Math.min(right-10,left));right=Math.min(nav.clientWidth-q.pad,Math.max(left+10,right));const newSpan=Math.max(1,(right-left)*g.span/q.plotW),zoom=Math.max(g.baseViewSpan/g.span,Math.min(24,g.baseViewSpan/newSpan)),viewBase=g.viewBase,targetStart=g.fullStart+(left-q.pad)*g.span/q.plotW;mainCv._rangeZoom=zoom;mainCv._panOffset=targetStart-viewBase;mainCv._onPanChange?.(mainCv._panOffset);mainCv._gapDeepDiveRedraw?.();e.preventDefault();});
    const end=()=>{if(drag&&mainCv._panMinOffset!==undefined&&mainCv._panMaxOffset!==undefined){if(mainCv._panOffset<=mainCv._panMinOffset+1)mainCv._onPanEdge?.(-1);else if(mainCv._panOffset>=mainCv._panMaxOffset-1)mainCv._onPanEdge?.(1);}drag=null;nav.style.cursor='grab';};
    nav.addEventListener('pointerup',end);nav.addEventListener('pointercancel',end);
  }
}
function setGapDeepDiveMouseMode(mode,cv,ui={}){
  const measure=ui.measure||$('#gap_chart_measure'),pan=ui.pan||$('#gap_chart_pan');
  cv._rangeMeasureMode=mode==='measure';cv._zoomSelectMode=mode!=='measure'&&mode!=='pan'&&mode!=='avwap';cv._panToolMode=mode==='pan';
  if(mode!=='avwap')cv._avwapSelectMode=false;
  if(measure){measure.dataset.active=cv._rangeMeasureMode?'1':'';measure.style.borderColor=cv._rangeMeasureMode?'#62c8ff':'';measure.textContent=cv._rangeMeasureMode?'Measure active':'Measure range';}
  if(pan){pan.dataset.active=cv._panToolMode?'1':'';pan.style.borderColor=cv._panToolMode?'#62c8ff':'';}
  cv.style.cursor=(cv._rangeMeasureMode||cv._zoomSelectMode||mode==='avwap')?'crosshair':'grab';
}
function bindUnifiedChartFeatures(cv,ui,persist,redraw){
  cv._fullFeatures=true;cv._featureUi=ui;cv._featurePersist=persist;cv._featureRedraw=redraw;cv._rangeOverlay=ui.overlay;cv._crosshairV=ui.crosshairV;cv._crosshairH=ui.crosshairH;cv._rangeMeasureBox=ui.measureBox;cv._navigator=ui.navigator;cv._quoteLine=ui.quoteLine;cv._showCrosshair=true;cv._showOhlcTip=true;cv._wheelZoomEnabled=true;
  cv._afterDraw=()=>{drawGapDeepDiveRangeOverlay(cv);drawGapDeepDiveCrosshair(cv);drawGapDeepDiveNavigator(cv,cv._chart,cv._timeframe);};
  cv._gapDeepDiveRedraw=()=>{cv._zoom=cv._rangeZoom||cv._zoom||1;cv._featurePersist?.();cv._featureRedraw?.();};
  cv._avwapOnChange=message=>{ui.status.textContent=message;cv._featurePersist?.();if(message.startsWith('AVWAP drawn'))setGapDeepDiveMouseMode('zoom',cv,ui);};
  bindGapDeepDiveRangeInput(cv);
  if(cv._featureControlsBound)return;cv._featureControlsBound=true;
  ui.measure.onclick=()=>{const mode=cv._rangeMeasureMode?'zoom':'measure';setGapDeepDiveMouseMode(mode,cv,ui);ui.status.textContent=mode==='measure'?'Drag between any two chart points to measure.':'Hold and drag across the chart to zoom.';drawGapDeepDiveRangeOverlay(cv);};
  ui.pan.onclick=()=>{const mode=cv._panToolMode?'zoom':'pan';setGapDeepDiveMouseMode(mode,cv,ui);ui.status.textContent=mode==='pan'?'Pan active · drag to move the chart.':'Pointer active · drag across the chart to zoom.';};
  ui.avwap.onclick=()=>{setGapDeepDiveMouseMode('avwap',cv,ui);cv._avwapAnchors=null;cv._avwapSelectMode=true;ui.status.textContent='AVWAP tool active · click anchor A, then a later point B.';cv._featurePersist?.();cv._featureRedraw?.();};
  ui.clearAvwap.onclick=()=>{cv._avwapAnchors=null;cv._avwapSelectMode=false;setGapDeepDiveMouseMode('zoom',cv,ui);ui.status.textContent='AVWAP cleared.';cv._featurePersist?.();cv._featureRedraw?.();};
  ui.deleteMeasure.onclick=()=>{cv._rangeMeasures=(cv._rangeMeasures||[]).filter(m=>m.id!==cv._selectedRangeId);cv._selectedRangeId=cv._rangeMeasures.at(-1)?.id||null;cv._featurePersist?.();cv._featureRedraw?.();};
  ui.clearMeasures.onclick=()=>{cv._rangeMeasures=[];cv._selectedRangeId=null;cv._featurePersist?.();cv._featureRedraw?.();};
  const zoomBy=factor=>{const g=cv._rangeGeometry;if(!g)return;const rawZoom=(cv._rangeZoom||1)*factor,minZoom=g.baseViewSpan/g.span;if(factor<1&&rawZoom<minZoom-1e-6&&cv._onZoomOutMore){const requestedSessions=Math.ceil(g.baseViewSpan/rawZoom)+1;Promise.resolve(cv._onZoomOutMore(requestedSessions)).then(added=>{if(added){zoomBy(factor);return;}const zoom=minZoom,anchor=g.viewStart+g.viewSpan/2,nextSpan=Math.min(g.span,g.baseViewSpan/zoom);setGapDeepDiveZoomView(cv,g,zoom,anchor-nextSpan/2);cv._gapDeepDiveRedraw?.();}).catch(()=>{});return;}const zoom=Math.max(minZoom,Math.min(24,rawZoom)),anchor=g.viewStart+g.viewSpan/2,nextSpan=Math.min(g.span,g.baseViewSpan/zoom);setGapDeepDiveZoomView(cv,g,zoom,anchor-nextSpan/2);cv._gapDeepDiveRedraw?.();};ui.zoomIn.onclick=()=>zoomBy(1.25);ui.zoomOut.onclick=()=>zoomBy(.8);
  cv._wheelZoomMore=(ratio,direction=-1)=>{const geometry=cv._rangeGeometry;if(!geometry)return;const rawZoom=(cv._rangeZoom||1)*Math.exp(direction*.18),minZoom=geometry.baseViewSpan/geometry.span,anchor=geometry.viewStart+ratio*geometry.viewSpan;if(rawZoom<minZoom-1e-6&&cv._onZoomOutMore){const requestedSessions=Math.ceil(geometry.baseViewSpan/rawZoom)+1;Promise.resolve(cv._onZoomOutMore(requestedSessions)).then(added=>{if(added){cv._wheelZoomMore(ratio,direction);return;}const zoom=minZoom,nextSpan=Math.min(geometry.span,geometry.baseViewSpan/zoom);setGapDeepDiveZoomView(cv,geometry,zoom,anchor-ratio*nextSpan);cv._gapDeepDiveRedraw?.();}).catch(()=>{});return;}const zoom=Math.max(minZoom,Math.min(24,rawZoom)),nextSpan=Math.min(geometry.span,geometry.baseViewSpan/zoom);setGapDeepDiveZoomView(cv,geometry,zoom,anchor-ratio*nextSpan);cv._gapDeepDiveRedraw?.();};
  ui.reset.onclick=()=>{cv._rangeZoom=1;cv._zoom=1;cv._panOffset=0;cv._yZoom=1;cv._featurePersist?.();cv._featureRedraw?.();};
  ui.copy.onclick=async()=>{try{await copyChartWithOverlays(cv,ui.overlay,ui.measureBox);ui.copy.textContent='Copied ✓';setTimeout(()=>ui.copy.textContent='Copy PNG',1200);}catch(error){ui.status.textContent='PNG copy unavailable: '+error.message;}};
  [ui.vwap,ui.twap,ui.ema5ext].forEach((toggle,index)=>toggle?.addEventListener('change',()=>{if(index===0)cv._showVWAP=toggle.checked;else if(index===1)cv._showTWAP=toggle.checked;else cv._showEMA5Ext=toggle.checked;cv._featurePersist?.();cv._featureRedraw?.();}));
  if(ui.ema5extVal){
    const saved = localStorage.getItem('gap_ema5ext_threshold') || '30';
    ui.ema5extVal.value = String(cv._ema5ExtThreshold || saved);
    cv._ema5ExtThreshold = Number(ui.ema5extVal.value) || 30;
    const onVal = e => {
      e.stopPropagation();
      const val = Math.max(1, Math.min(500, Number(ui.ema5extVal.value) || 30));
      cv._ema5ExtThreshold = val;
      try { localStorage.setItem('gap_ema5ext_threshold', String(val)); } catch{}
      cv._featurePersist?.();
      cv._featureRedraw?.();
    };
    ui.ema5extVal.onchange = onVal;
    ui.ema5extVal.oninput = onVal;
    ui.ema5extVal.onclick = e => e.stopPropagation();
  }
}
async function copyChartWithOverlays(cv,overlay,measureBox){
  const width=Math.max(1,cv.clientWidth),height=Math.max(1,cv.clientHeight),out=document.createElement('canvas');out.width=cv.width;out.height=cv.height;
  const x=out.getContext('2d');x.setTransform(out.width/width,0,0,out.height/height,0,0);x.fillStyle='#050607';x.fillRect(0,0,width,height);x.drawImage(cv,0,0,width,height);if(overlay)x.drawImage(overlay,0,0,width,height);
  if(measureBox&&!measureBox.hidden&&measureBox.textContent.trim()){
    const rect=measureBox.getBoundingClientRect(),parent=measureBox.parentElement?.getBoundingClientRect(),style=getComputedStyle(measureBox),left=rect.left-(parent?.left||0),top=rect.top-(parent?.top||0),padX=parseFloat(style.paddingLeft)||8,padY=parseFloat(style.paddingTop)||8;
    x.fillStyle=style.backgroundColor||'rgba(6,13,21,.96)';x.fillRect(left,top,rect.width,rect.height);x.strokeStyle=style.borderTopColor||style.borderColor||'#62c8ff';x.lineWidth=parseFloat(style.borderTopWidth)||1;x.strokeRect(left+.5,top+.5,rect.width-1,rect.height-1);
    x.fillStyle=style.color||'#edf6ff';x.font=`${style.fontWeight||'400'} ${style.fontSize||'13px'} ${style.fontFamily||'Consolas,monospace'}`;x.textBaseline='top';const lineHeight=parseFloat(style.lineHeight)||20;measureBox.textContent.split('\n').forEach((line,index)=>x.fillText(line,left+padX,top+padY+index*lineHeight));
  }
  const blob=await new Promise(resolve=>out.toBlob(resolve,'image/png'));if(!blob)throw Error('Could not render PNG');await navigator.clipboard.write([new ClipboardItem({'image/png':blob})]);
}
function chartFeatureUi(prefix){
  const by=(...suffixes)=>suffixes.map(suffix=>document.getElementById(prefix+suffix)).find(Boolean)||null;
  return {
    overlay:by('_overlay','_range_overlay'),crosshairV:by('_crosshair_v'),crosshairH:by('_crosshair_h'),
    measureBox:by('_measure_box'),navigator:by('_navigator'),quoteLine:by('_quote_line'),
    status:by('_status','_avwap_status'),measure:by('_measure'),pan:by('_pan'),avwap:by('_avwap'),
    clearAvwap:by('_clear_avwap'),deleteMeasure:by('_delete_measure'),clearMeasures:by('_clear_measures'),
    zoomIn:by('_zoom_in'),zoomOut:by('_zoom_out'),reset:by('_zoom_reset','_reset'),
    copy:by('_copy'),vwap:by('_vwap'),twap:by('_twap'),ema5ext:by('_ema5ext'),ema5extVal:by('_ema5ext_val')
  };
}
function cancelUnifiedChartSelection(cv,ui={}){
  const drag=cv._rangeDrag,cancelAvwap=cv._avwapSelectMode,wasPanning=!!cv._panActive,keepMeasure=!!cv._rangeMeasureMode,selectedId=cv._selectedRangeId,removeId=drag?.id||selectedId;
  if(!selectedId&&!keepMeasure&&!cv._panToolMode&&!cancelAvwap&&!cv._zoomDrag&&!drag&&!wasPanning)return false;
  if(removeId)cv._rangeMeasures=(cv._rangeMeasures||[]).filter(item=>item.id!==removeId);
  if(wasPanning){cv._panOffset=cv._panStartOffset||0;cv._yZoom=cv._panStartYZoom||1;cv._onPanChange?.(cv._panOffset);cv._onYZoomChange?.(cv._yZoom);}
  cv._panActive=false;cv._selectedRangeId=null;cv._rangeDrag=null;cv._zoomDrag=null;cv._avwapSelectMode=false;
  if(cancelAvwap)cv._avwapAnchors=null;
  if(keepMeasure)setGapDeepDiveMouseMode('measure',cv,ui);else if(cv._panToolMode||cv._rangeMeasureMode)setGapDeepDiveMouseMode('zoom',cv,ui);
  cv._featurePersist?.();
  const box=ui.measureBox||$('#gap_chart_measure_box'),status=ui.status||$('#gap_chart_avwap_status');if(box)box.hidden=true;
  if(status)status.textContent=removeId?(keepMeasure?'Drawing removed · Measure remains active.':'Drawing removed.'):drag?(keepMeasure?'Drawing cancelled · Measure remains active.':'Drawing cancelled.'):cancelAvwap?'AVWAP selection cancelled.':keepMeasure?'Measure remains active.':'Selection cleared. Hold and drag on the chart to zoom.';
  cv._featureRedraw?.();cv._gapDeepDiveRedraw?.();drawGapDeepDiveRangeOverlay(cv);drawGapDeepDiveCrosshair(cv);return true;
}
function drawEv(cv, c, sel, bzChecked, targetH, timeframe='2m', zoom=1){
  const parentW = (cv.parentElement && cv.parentElement.clientWidth) ? cv.parentElement.clientWidth : 0;
  const dpr = Math.min(3,Math.max(window.devicePixelRatio||1,Number(cv._renderDpr)||2));
  const W = Math.max(300, parentW - (targetH ? 0 : 16));
  const H = targetH || 540;
  cv.style.width = W + 'px';
  cv.style.height = H + 'px';
  cv.width = Math.round(W * dpr);
  cv.height = Math.round(H * dpr);
  const x = cv.getContext('2d');
  x.setTransform(cv.width / W, 0, 0, cv.height / H, 0, 0);
  x.clearRect(0, 0, W, H);
  const isLarge = H > 450;
  x.font = (isLarge ? '15px' : '12px') + ' Consolas,monospace';
  const daily=timeframe==='1d', tf=parseInt(timeframe,10)||2, source=c.bars, sessionSpan=c.after_hours?960:720;
  let b,dates,multi;const cached=cv._seriesCache;
  if(cached?.source===source&&cached.tf===timeframe){b=cached.bars;dates=cached.dates;multi=cached.multi;}
  else{
    b=daily||source?.timeframe===timeframe?source:aggregateEvBars(source,tf);
    dates=[...new Set(b?.d||[])];multi=!daily&&dates.length>1;
    if(multi){let last='',n=-1;b={...b,t:b.t.map((t,i)=>{if(b.d[i]!==last){last=b.d[i];n++;}return 240+n*sessionSpan+t-240;})};}
    cv._seriesCache={source,tf:timeframe,bars:b,dates,multi};
  }
  if(!b || !b.t.length){
    x.fillStyle = '#8a8a8a';
    x.fillText('No intraday data', 10, 20);
    return;
  }
  const L = isLarge ? 64 : 50, R = 12, T = 10;
  const VH = Math.round(H * 0.18), B = isLarge ? 24 : 18;
  const ph = H - T - VH - B - 6, pw = W - L - R, vy0 = T + ph + 6, vy1 = vy0 + VH;
  const sg = sel === 'none' ? [] : (c.sig || []).filter(s => sel === 'all' || s.name === sel);
  const span=daily?Math.max(1,b.t.length):multi?dates.length*sessionSpan:sessionSpan, fullStart=daily?-.5:240;
  // Keep intraday zoom centered on the selected session.  Do not clamp zoom
  // to 1x: that made every zoom-out click a no-op.  The loaded date range is
  // still the upper limit, so zooming out reveals neighboring sessions.
  const baseViewSpan=daily?Math.min(span,80):sessionSpan;
  const viewSpan=Math.min(span,baseViewSpan/Math.max(.01,zoom)),panLimit=Math.max(0,span-viewSpan)/2;
  let viewBase=fullStart+panLimit,panMin=-panLimit,panMax=panLimit;
  if(multi){const anchorIndex=dates.indexOf(cv._anchorDate||c.date);if(anchorIndex>=0){viewBase=fullStart+anchorIndex*sessionSpan+sessionSpan/2-viewSpan/2;panMin=fullStart-viewBase;panMax=fullStart+span-viewSpan-viewBase;}}
  cv._panMinOffset=panMin;cv._panMaxOffset=panMax;
  cv._panOffset=Math.max(panMin,Math.min(panMax,cv._panOffset||0));
  const viewStart=viewBase+cv._panOffset,X=m=>L+pw*(m-viewStart)/viewSpan;
  let visible=Array.from({length:b.t.length},(_,i)=>i).filter(i=>b.t[i]>=viewStart-tf&&b.t[i]<=viewStart+viewSpan+tf);
  if(!visible.length){let nearest=0,distance=Infinity;for(let i=0;i<b.t.length;i++){const d=Math.abs(b.t[i]-(viewStart+viewSpan/2));if(d<distance){nearest=i;distance=d;}}visible=[nearest];}
  let lo=Math.min(...visible.map(i=>b.l[i])),hi=Math.max(...visible.map(i=>b.h[i]));
  const pad=(hi-lo)*0.05||hi*0.02;lo-=pad;hi+=pad;
  const yMid=(lo+hi)/2,yHalf=(hi-lo)/(2*Math.max(.5,cv._yZoom||1));lo=yMid-yHalf;hi=yMid+yHalf;
  const Y=v=>T+ph*(1-(v-lo)/(hi-lo));
  cv._rangeGeometry={bars:b,chart:c,dates,multi,daily,sessionSpan,tf,X,Y,L,T,plotW:pw,plotH:ph,crosshairBottom:vy1,yMin:lo,yMax:hi,span,fullStart,viewStart,viewSpan,baseViewSpan,viewBase};
  cv._hoverItems=visible.map(i=>{const day=b.d?.[i]||c.date||'',dayIndex=multi?dates.indexOf(day):-1,minute=daily?0:b.t[i]-(dayIndex>=0?dayIndex*sessionSpan:0);return {key:`${day}|${b.t[i]}`,index:i,date:day,minute,x:X(b.t[i]+(daily?.5:tf/2)),label:daily?`${istFromEasternClock(day,0).split(' ')[0]} · Daily IST`:istFromEasternClock(day,minute),o:b.o[i],h:b.h[i],l:b.l[i],c:b.c[i],prevC:i>0?b.c[i-1]:c.pc,v:b.v[i]||0};});
  cv._rangePointAt=(px,py)=>{if(px<L||px>W-R||py<T||py>T+ph)return null;const plotTime=viewStart+(px-L)/pw*viewSpan,price=hi-(py-T)/ph*(hi-lo);let date=c.date||'',minute=plotTime;if(daily){const index=Math.max(0,Math.min(b.d.length-1,Math.round(plotTime)));date=b.d?.[index]||date;minute=0;}else if(multi){const index=Math.max(0,Math.min(dates.length-1,Math.floor((plotTime-240)/sessionSpan)));date=dates[index]||date;minute=plotTime-index*sessionSpan;}return {date,minute,plotTime,price,label:daily?`${formatDateMDY(date)} · Daily IST`:istFromEasternClock(date,minute)};};
  cv._rangePointAtX=px=>{const p=cv._hoverItems?.reduce((best,item)=>Math.abs(item.x-px)<Math.abs(best.x-px)?item:best,cv._hoverItems?.[0]);return p?{date:p.date,minute:p.minute,price:p.c,label:p.label}:null;};
  cv._showChartHover=(px,py)=>{const readout=cv._hoverTimeReadout;if(px<L||px>W-R||!cv._hoverItems?.length){if(cv._hoverTip)cv._hoverTip.style.display='none';if(readout)readout.textContent='Hover a candle to see its time · IST';return}let item=cv._hoverItems.reduce((best,p)=>Math.abs(p.x-px)<Math.abs(best.x-px)?p:best,cv._hoverItems[0]);cv._activeHoverIndex=item.index;const fmt=n=>Number.isFinite(Number(n))?Number(n).toFixed(Number(n)<1?4:2):'—',ohlc=`O ${fmt(item.o)}  H ${fmt(item.h)}  L ${fmt(item.l)}  C ${fmt(item.c)}  V ${Number(item.v||0).toLocaleString('en-US')}`,bottomOhlc=!!(cv._showOhlcTip&&readout&&cv.id!=='kwModalCanvas');if(readout)readout.textContent=bottomOhlc?`${item.label} · ${ohlc}`:item.label;const tip=cv._hoverTip;if(!tip)return;tip.textContent=cv._showOhlcTip?`O ${fmt(item.o)}  H ${fmt(item.h)}\nL ${fmt(item.l)}  C ${fmt(item.c)}  V ${Number(item.v||0).toLocaleString('en-US')}`:'';tip.style.whiteSpace=cv._showOhlcTip?'pre-line':'nowrap';tip.style.display=cv._showOhlcTip&&!bottomOhlc?'block':'none';if(cv._quoteLine){const change=item.c-Number(item.prevC||0),pct=item.prevC?change/Number(item.prevC)*100:0,tp=(item.h+item.l+item.c)/3,dv=tp*item.v,float=Number(cv._floatShares),compact=n=>Number.isFinite(n)?new Intl.NumberFormat('en-US',{notation:'compact',maximumFractionDigits:2}).format(n):'—',money=n=>Number.isFinite(n)?(Math.abs(n)>=1000?'$'+compact(Math.abs(n)):Math.abs(n)<1?'$'+Math.abs(n).toFixed(4):'$'+Math.abs(n).toFixed(2)):'—',signedMoney=n=>`${n>=0?'+':'-'}${money(change)}`;const extEd=cv._extData?[item.index]:null;cv._quoteLine.textContent=`${item.label} · %Chg ${pct>=0?'+':''}${pct.toFixed(2)}% · $Chg ${signedMoney(change)} · Vol ${Number(item.v).toLocaleString('en-US')} · $Vol ${money(dv)} · Float ${Number.isFinite(float)?compact(float)+' sh':'unavailable'}${extEd?` · Ext ${extEd.extHigh>=0?'+':''}${extEd.extHigh.toFixed(1)}% (Day max ${extEd.dayMax>=0?'+':''}${extEd.dayMax.toFixed(1)}%)`:''}`;cv._quoteLine.style.color=change>=0?'#64d99b':'#ff7777';}if(cv.id==='kwModalCanvas'){tip.style.left='' ;tip.style.top='' ;}else if(cv._showOhlcTip&&!bottomOhlc){tip.style.left=Math.min(W-210,Math.max(L,px+14))+'px';tip.style.top=(cv.offsetTop+8)+'px';}else{tip.style.left=Math.max(4,cv.offsetLeft+8)+'px';tip.style.top=(cv.offsetTop+cv.offsetHeight+2)+'px';}cv.removeAttribute('title');};
  cv._viewSpan=viewSpan;cv._panSpan=span;cv._plotWidth=pw;cv._drawEvArgs=[c,sel,bzChecked,targetH,timeframe,zoom];
  if(!cv._panBound){
    cv._panBound=true;cv.style.cursor='grab';
    const hoverParent=cv.parentElement;if(hoverParent){if(getComputedStyle(hoverParent).position==='static')hoverParent.style.position='relative';const tip=document.createElement('div');tip.className='chartHoverTip';tip.style.cssText='display:none;position:absolute;z-index:9;pointer-events:none;background:rgba(8,12,16,.96);border:1px solid #465362;color:#e7eef5;padding:5px 7px;border-radius:3px;font:11px Consolas,monospace;white-space:nowrap;box-shadow:0 2px 8px #000';if(cv.id==='kwModalCanvas'){tip.style.position='static';tip.style.background='transparent';tip.style.border='0';tip.style.padding='0';document.querySelector('.kwModalLegend')?.appendChild(tip)}else hoverParent.appendChild(tip);cv._hoverTip=tip;}
    cv.addEventListener('pointerdown',e=>{if(e.button!==0)return;if(cv._avwapSelectMode){if(e.offsetX<L||e.offsetX>W-R)return;const p=cv._hoverItems?.reduce((a,z)=>Math.abs(z.x-e.offsetX)<Math.abs(a.x-e.offsetX)?z:a,cv._hoverItems?.[0]);if(!p)return;if(cv._avwapAnchors?.a==null){cv._avwapAnchors={a:p.index,b:null};cv._avwapOnChange?.(`Anchor A selected · ${p.label} · ${p.c}`);}else if(p.index<=cv._avwapAnchors.a){cv._avwapOnChange?.('Choose point B after point A in time.');}else{cv._avwapAnchors={...cv._avwapAnchors,b:p.index};cv._avwapSelectMode=false;cv._avwapOnChange?.(`AVWAP drawn · A ${cv._hoverItems.find(z=>z.index===cv._avwapAnchors.a)?.label||''} → B ${p.label}`);}e.preventDefault();e.stopPropagation();drawEv(cv,...cv._drawEvArgs);return;}if(cv._measureMode&&e.offsetX>L&&e.offsetX<W-R){const p=cv._hoverItems?.reduce((a,z)=>Math.abs(z.x-e.offsetX)<Math.abs(a.x-e.offsetX)?z:a,cv._hoverItems?.[0]);if(p){cv._measureActive=true;cv._measureStartKey=p.key;cv._measureEndKey=p.key;cv.setPointerCapture(e.pointerId);drawEv(cv,...cv._drawEvArgs);}return;}cv._panActive=true;cv._panMode=e.offsetX<=L?'price':'time';cv._panStartX=e.clientX;cv._panStartY=e.clientY;cv._panStartOffset=cv._panOffset||0;cv._panStartYZoom=cv._yZoom||1;cv._panDidMove=false;cv.setPointerCapture(e.pointerId);cv.style.cursor=cv._panMode==='price'?'ns-resize':'grabbing';});
    cv.addEventListener('pointermove',e=>{if(cv._measureActive){const p=cv._hoverItems?.reduce((a,z)=>Math.abs(z.x-e.offsetX)<Math.abs(a.x-e.offsetX)?z:a,cv._hoverItems?.[0]);if(p){cv._measureEndKey=p.key;drawEv(cv,...cv._drawEvArgs);}return;}if(!cv._panActive){cv.style.cursor=(cv._rangeMeasureMode||cv._avwapSelectMode||cv._zoomSelectMode)?'crosshair':(e.offsetX<=L?'ns-resize':'grab');if(cv._showCrosshair){cv._crosshair={x:e.offsetX,y:e.offsetY};drawGapDeepDiveCrosshair(cv);}scheduleGapDeepDiveHover(cv,e.offsetX,e.offsetY);return;}if(cv._hoverTip)cv._hoverTip.style.display='none';const dx=e.clientX-cv._panStartX,dy=e.clientY-cv._panStartY;if(Math.abs(dx)>3||Math.abs(dy)>3)cv._panDidMove=true;if(cv._panMode==='price'){cv._yZoom=Math.max(.5,Math.min(20,cv._panStartYZoom*Math.exp(-dy/140)));cv._onYZoomChange?.(cv._yZoom);}else{cv._panOffset=Math.max(cv._panMinOffset,Math.min(cv._panMaxOffset,cv._panStartOffset-dx*cv._viewSpan/Math.max(1,cv._plotWidth)));cv._onPanChange?.(cv._panOffset);}if(cv._drawEvArgs)drawEv(cv,...cv._drawEvArgs);});
    cv.addEventListener('pointerleave',()=>{cv._activeHoverIndex=null;if(cv._hoverTip)cv._hoverTip.style.display='none';if(cv._hoverTimeReadout)cv._hoverTimeReadout.textContent='Hover a candle to see its time · IST';cv._pendingHover=null;if(cv._hoverUpdateTimer){clearTimeout(cv._hoverUpdateTimer);cv._hoverUpdateTimer=0;}if(cv._showCrosshair&&cv._crosshair){cv._crosshair=null;drawGapDeepDiveCrosshair(cv);}});
    cv.addEventListener('pointerup',e=>{if(cv._measureActive){cv._measureActive=false;return;}if(!cv._panActive)return;cv._panActive=false;cv.style.cursor='grab';if(cv._panDidMove&&cv._panMode==='time'){const dx=e.clientX-cv._panStartX;if(cv._panMinOffset===cv._panMaxOffset&&Math.abs(dx)>20)cv._onPanEdge?.(dx<0?1:-1);else if(cv._panOffset>=cv._panMaxOffset-1)cv._onPanEdge?.(1);else if(cv._panOffset<=cv._panMinOffset+1)cv._onPanEdge?.(-1);}});
    cv.addEventListener('pointercancel',()=>{cv._panActive=false;cv.style.cursor='grab';});
    cv.addEventListener('wheel',e=>{if(e.offsetX>L)return;e.preventDefault();cv._yZoom=Math.max(.5,Math.min(20,(cv._yZoom||1)*Math.exp(-Math.sign(e.deltaY)*.15)));cv._onYZoomChange?.(cv._yZoom);if(cv._drawEvArgs)drawEv(cv,...cv._drawEvArgs);},{passive:false});
    cv.addEventListener('click',e=>{if(cv._panDidMove||cv._measureMode){e.preventDefault();e.stopPropagation();e.stopImmediatePropagation();cv._panDidMove=false;}},true);
  }
  // Premarket light grey background
  x.fillStyle = 'rgba(255, 255, 255, 0.085)';
  if(!daily)for(let day=0;day<(multi?dates.length:1);day++)x.fillRect(X(240+day*sessionSpan),T,X(570+day*sessionSpan)-X(240+day*sessionSpan),ph+6+VH);
  x.fillStyle = '#8e8e9e';
  x.font = (isLarge?'bold 12px':'bold 10px')+' Consolas,monospace';
  if(!daily){const anchorIndex=multi?Math.max(0,dates.indexOf(cv._anchorDate||c.date)):0;x.fillText('PRE-MARKET', X(240+anchorIndex*sessionSpan) + 6, T + 13);}
  x.lineWidth = 1;
  x.strokeStyle = '#292d32';
  x.fillStyle = '#b3b8be';
  for(let k = 0; k <= 5; k++){
    const v = lo + (hi - lo) * k / 5, y = Y(v);
    x.beginPath();
    x.moveTo(L, y);
    x.lineTo(W - R, y);
    x.stroke();
    x.fillText(v >= 1 ? v.toFixed(2) : v.toFixed(3), 2, y + 4);
  }
  const tickStep=daily?(b.t.length>12?Math.ceil(b.t.length/6):1):multi?180:(c.after_hours?120:60);
  for(let m = daily?0:240; m <= (daily?b.t.length-1:240+span); m += tickStep){
    const xx = X(daily?m+.5:m);
    x.beginPath();
    x.moveTo(xx, T);
    x.lineTo(xx, vy1);
    x.stroke();
    x.fillText(daily?formatDateMDY(b.d?.[m]||''):multi?((m-240)%sessionSpan===0?formatDateMDY(dates[Math.min(dates.length-1,Math.floor((m-240)/sessionSpan))]||''):''):hm(m,c.off), xx - 14, H - 5);
  }
  x.strokeStyle = '#777';
  x.setLineDash([2, 3]);
  x.beginPath();
  if(!daily&&!multi){x.moveTo(X(570), T);x.lineTo(X(570), vy1);if(c.after_hours){x.moveTo(X(960),T);x.lineTo(X(960),vy1);}}
  x.stroke();
  x.setLineDash([]);
  const dash = (v, col) => {
    if(v == null || v < lo || v > hi) return;
    x.strokeStyle = col;
    x.setLineDash([4, 4]);
    x.beginPath();
    x.moveTo(L, Y(v));
    x.lineTo(W - R, Y(v));
    x.stroke();
    x.setLineDash([]);
  };
  if(!daily){dash(c.pc, '#888');dash(c.pmh, '#ffd23f');}
  // ---- bone zone: 9/21 EMA cloud + Bollinger Bands (21, 2) on the selected interval
  const BZ = bzChecked ? bone(b) : null;
  const clipP = () => { x.save(); x.beginPath(); x.rect(L, T, pw, ph); x.clip(); };
  const poly = (a, col, w) => {
    x.strokeStyle = col;
    x.lineWidth = w;
    x.beginPath();
    let on = false;
    for(let i = 0; i < a.length; i++){
      if(a[i] == null){ on = false; continue; }
      const xx = X(b.t[i] + tf / 2), yy = Y(a[i]);
      if(!on){ x.moveTo(xx, yy); on = true; } else x.lineTo(xx, yy);
    }
    x.stroke();
    x.lineWidth = 1;
  };
  if(BZ){
    clipP();
    for(let i = 1; i < b.t.length; i++){
      x.fillStyle = BZ.e9[i] > BZ.e21[i] ? 'rgba(0,255,0,0.30)' : 'rgba(255,0,0,0.30)';
      x.beginPath();
      x.moveTo(X(b.t[i-1] + 1), Y(BZ.e9[i-1]));
      x.lineTo(X(b.t[i] + 1), Y(BZ.e9[i]));
      x.lineTo(X(b.t[i] + 1), Y(BZ.e21[i]));
      x.lineTo(X(b.t[i-1] + 1), Y(BZ.e21[i-1]));
      x.closePath();
      x.fill();
    }
    poly(BZ.bu, '#7a7a7a', 1);
    poly(BZ.bl, '#7a7a7a', 1);
    x.restore();
  }
  const bw = daily?Math.max(3,Math.min(24,pw/b.t.length*.65)):Math.max(1.4, pw / (viewSpan / tf) * 0.84), vmax = Math.max(...visible.map(i=>b.v[i]||0)) || 1;
  for(let i = 0; i < b.t.length; i++){
    if(b.t[i]<viewStart-tf||b.t[i]>viewStart+viewSpan+tf)continue;
    const xx = X(b.t[i] + (daily?.5:tf / 2)), up = b.c[i] >= b.o[i], col = up ? '#3ddc84' : '#ff5a5a';
    x.fillStyle = up ? 'rgba(61,220,132,0.55)' : 'rgba(255,90,90,0.55)';
    const vh = VH * b.v[i] / vmax;
    x.fillRect(xx - bw / 2, vy1 - vh, bw, vh);
    x.strokeStyle = col;
    x.lineWidth = Math.max(1.1, Math.min(1.8, tf / 2));
    x.beginPath();
    x.moveTo(xx, Y(b.h[i]));
    x.lineTo(xx, Y(b.l[i]));
    x.stroke();
    x.fillStyle = col;
    const yt = Y(Math.max(b.o[i], b.c[i]));
    x.fillRect(xx - bw / 2, yt, bw, Math.max(1.5, Math.abs(Y(b.o[i]) - Y(b.c[i]))));
    x.lineWidth = 1;
  }
  if(BZ){
    clipP();
    poly(BZ.e9, 'rgb(215,255,110)', isLarge ? 1.6 : 1.2);
    poly(BZ.e21, 'rgb(255,165,0)', isLarge ? 1.6 : 1.2);
    x.restore();
  }
  if(!daily&&cv._showVWAP!==false){
    x.strokeStyle = '#ffffff';
    x.lineWidth = isLarge ? 2 : 1.4;
    x.beginPath();
    let on = false, lastVwapDate=null;
    for(let i = 0; i < b.t.length; i++){
      const v = b.vw[i];
      if(v == null){ on = false; continue; }
      if(multi&&b.d?.[i]!==lastVwapDate)on=false;
      const xx = X(b.t[i] + tf / 2), yy = Y(Math.min(hi, Math.max(lo, v)));
      if(!on){ x.moveTo(xx, yy); on = true; } else x.lineTo(xx, yy);
      lastVwapDate=b.d?.[i]||null;
    }
    x.stroke();
  }
  if(!daily){
    const twap=[], activeTwap=[], avh=[]; let dayKey=null, count=0, timeSum=0, activeCount=0, activeSum=0;
    let prevT=null, prevTp=null, anchorDollar=0, anchorVol=0;
    const hodByDay=new Map();for(let i=0;i<b.t.length;i++){const day=b.d?.[i]||c.date||'';if(!hodByDay.has(day)||b.h[i]>b.h[hodByDay.get(day)])hodByDay.set(day,i);}
    for(let i=0;i<b.t.length;i++){
      const day=b.d?.[i]||c.date||'', tp=(b.h[i]+b.l[i]+b.c[i])/3;
      if(day!==dayKey){dayKey=day;count=0;timeSum=0;activeCount=0;activeSum=0;prevT=null;prevTp=null;anchorDollar=0;anchorVol=0;}
      const steps=prevT==null?1:Math.max(1,Math.round((b.t[i]-prevT)/tf));
      const fillTp=prevTp==null?tp:prevTp;
      if(count){timeSum+=fillTp*(steps-1);count+=steps-1;}
      timeSum+=tp;count++;twap.push(timeSum/count);
      if((b.v[i]||0)>0){activeSum+=tp;activeCount++;}activeTwap.push(activeCount?activeSum/activeCount:null);
      if(i>=hodByDay.get(day)){anchorDollar+=tp*(b.v[i]||0);anchorVol+=b.v[i]||0;avh.push(anchorVol?anchorDollar/anchorVol:null);}else avh.push(null);
      prevT=b.t[i];prevTp=tp;
    }
    clipP();
    if(cv._showTWAP===true){poly(twap,'#c084fc',isLarge?1.9:1.4);x.setLineDash([2,3]);poly(activeTwap,'#e879f9',isLarge?1.5:1.1);x.setLineDash([]);}
    if(cv._showHODAVWAP===true){x.setLineDash([5,3]);poly(avh,'#ffd23f',isLarge?2.2:1.5);x.setLineDash([]);}
    const anchors=cv._avwapAnchors;
    if(anchors&&Number.isInteger(anchors.a)&&Number.isInteger(anchors.b)&&anchors.b>anchors.a){
      const anchored=new Array(b.t.length).fill(null);let vol=0,dollar=0;
      for(let i=anchors.a;i<=anchors.b;i++){const v=Math.max(0,b.v[i]||0),tp=(b.h[i]+b.l[i]+b.c[i])/3;vol+=v;dollar+=tp*v;if(vol)anchored[i]=dollar/vol;}
      poly(anchored,'#ffe45c',isLarge?2.6:1.8);
      [[anchors.a,'A'],[anchors.b,'B']].forEach(([i,label])=>{const px=X(b.t[i]+tf/2),py=Y(b.c[i]);x.fillStyle='#ffe45c';x.beginPath();x.arc(px,py,5,0,Math.PI*2);x.fill();x.font='bold 12px Consolas,monospace';x.fillText(label,px+7,py-7);});
    }
    x.restore();
  }
  // ---- EMA5 Extension indicator (Thinkorswim: 5m 9-EMA, user-editable threshold) ----
  let extData = null; cv._extData = null;
  if(!daily && cv._showEMA5Ext !== false){
    const defaultThreshold = 30.0;
    let userThresh = Number(cv._ema5ExtThreshold);
    if(!Number.isFinite(userThresh) || userThresh <= 0){
      try {
        const saved = localStorage.getItem('gap_ema5ext_threshold');
        if(saved) userThresh = Number(saved);
      } catch{}
    }
    const extThreshold = Number.isFinite(userThresh) && userThresh > 0 ? userThresh : defaultThreshold;

    // Group all candles into chronological 5-minute buckets per session day
    const bucketMap = new Map();
    const bucketList = [];
    for(let i = 0; i < b.t.length; i++){
      const day = b.d?.[i] || c.date || '';
      const dayIndex = multi ? dates.indexOf(day) : -1;
      const minute = daily ? 0 : b.t[i] - (dayIndex >= 0 ? dayIndex * sessionSpan : 0);
      const bucket5 = Math.floor(minute / 5) * 5;
      const key = day + '|' + bucket5;
      if(!bucketMap.has(key)){
        const item = { key, day, bucket5, close: b.c[i] };
        bucketMap.set(key, item);
        bucketList.push(item);
      } else {
        bucketMap.get(key).close = b.c[i];
      }
    }

    // Compute 9-period EMA on 5m close across all sessions
    const alpha = 2 / (9 + 1); // 0.2
    let emaVal = null;
    const ema5Map = new Map();
    for(let j = 0; j < bucketList.length; j++){
      const cl = bucketList[j].close;
      if(emaVal == null) emaVal = cl;
      else emaVal = alpha * cl + (1 - alpha) * emaVal;
      ema5Map.set(bucketList[j].key, emaVal);
    }

    // Calculate extHigh, dayMax, sig per chart bar
    extData = new Array(b.t.length); cv._extData = extData;
    let curDay = '', curDayMax = -Infinity;
    for(let i = 0; i < b.t.length; i++){
      const day = b.d?.[i] || c.date || '';
      const dayIndex = multi ? dates.indexOf(day) : -1;
      const minute = daily ? 0 : b.t[i] - (dayIndex >= 0 ? dayIndex * sessionSpan : 0);
      const bucket5 = Math.floor(minute / 5) * 5;
      if(day !== curDay){ curDay = day; curDayMax = -Infinity; }
      const ema = ema5Map.get(day + '|' + bucket5);
      if(ema != null && ema > 0){
        const extHigh = (b.h[i] - ema) / ema * 100;
        curDayMax = Math.max(curDayMax, extHigh);
        const sig = extHigh >= extThreshold;
        extData[i] = { ema, extHigh, dayMax: curDayMax, sig };
      } else {
        extData[i] = null;
      }
    }

    // Plot signals above candle ONLY (no red background):
    // Arrow = sig and !sig[1] (RED down arrow)
    // AddChartBubble(sig, high, Round(extHigh, 0) + "%", Color.RED, yes)
    clipP();
    let prevSig = false;
    for(let i = 0; i < b.t.length; i++){
      if(b.t[i] < viewStart - tf || b.t[i] > viewStart + viewSpan + tf){ prevSig = false; continue; }
      const ed = extData[i];
      const sig = !!ed?.sig;
      if(sig){
        const xx = X(b.t[i] + tf / 2), barHigh = Y(b.h[i]);
        const text = Math.round(ed.extHigh) + '%';
        x.font = 'bold 9px Consolas,monospace';
        const tw = x.measureText(text).width, bw = tw + 8, bh = 14;
        const bx = Math.round(xx - bw / 2), by = Math.round(barHigh - bh - 5);
        // Bubble pill
        x.fillStyle = 'rgba(210, 25, 25, 0.95)';
        x.strokeStyle = '#ff6666';
        x.lineWidth = 1;
        x.beginPath();
        x.roundRect(bx, by, bw, bh, 3);
        x.fill();
        x.stroke();
        // Bubble pointer
        x.beginPath();
        x.moveTo(xx - 3, by + bh);
        x.lineTo(xx + 3, by + bh);
        x.lineTo(xx, by + bh + 3);
        x.closePath();
        x.fill();
        // Bubble text
        x.fillStyle = '#ffffff';
        x.textAlign = 'center';
        x.textBaseline = 'middle';
        x.fillText(text, xx, by + bh / 2 + 0.5);
        // Down Arrow on first bar of signal run
        if(!prevSig){
          const arrowTipY = by - 3;
          x.fillStyle = '#ff2222';
          x.strokeStyle = '#ffbbbb';
          x.lineWidth = 1;
          x.beginPath();
          x.moveTo(xx - 6, arrowTipY - 10);
          x.lineTo(xx + 6, arrowTipY - 10);
          x.lineTo(xx, arrowTipY);
          x.closePath();
          x.fill();
          x.stroke();
        }
      }
      prevSig = sig;
    }
    x.restore();

    // Top-left live readout label (Thinkorswim AddLabel)
    let targetIdx = cv._activeHoverIndex;
    if(targetIdx == null || !extData[targetIdx]){
      for(let i = b.t.length - 1; i >= 0; i--){
        if(b.t[i] <= viewStart + viewSpan && extData[i]){
          targetIdx = i;
          break;
        }
      }
    }
    if(targetIdx != null && extData[targetIdx]){
      const ed = extData[targetIdx], isSig = ed.sig;
      const labelText = `Ext: ${ed.extHigh >= 0 ? '+' : ''}${ed.extHigh.toFixed(1)}%  |  Day max: ${ed.dayMax >= 0 ? '+' : ''}${ed.dayMax.toFixed(1)}% (≥${extThreshold}%)`;
      x.font = 'bold 11px Consolas,monospace';
      const lw = x.measureText(labelText).width + 16, lh = 20, lx = L + 8, ly = T + 6;
      x.fillStyle = isSig ? 'rgba(130, 15, 15, 0.94)' : 'rgba(10, 14, 20, 0.88)';
      x.strokeStyle = isSig ? '#ff4d4d' : '#3a4654';
      x.lineWidth = 1;
      x.beginPath();
      x.roundRect(lx, ly, lw, lh, 4);
      x.fill();
      x.stroke();
      x.fillStyle = isSig ? '#ff9999' : '#a0abb6';
      x.textAlign = 'left';
      x.textBaseline = 'middle';
      x.fillText(labelText, lx + 8, ly + lh / 2 + 0.5);
    }
  }
  // ---- end EMA5 Extension indicator ----
  x.lineWidth = 1;
  x.fillStyle = '#8a8a8a';
  x.fillText('vol', L + 2, vy0 + 10);
  if(!multi&&!daily)sg.forEach((s, idx) => {
    const col = MC[s.name] || '#fff', xs = X(s.m), ys = Y(s.p);
    let xm, ym, pl;
    if(s.stop_m != null){
      xm = X(s.stop_m);
      ym = Y(s.stop_price ?? s.p * (1 + (s.stop_pct ?? c.stop)));
      pl = -(s.stop_pct ?? c.stop);
    } else if(c.cover){
      xm = X(c.cover.m);
      ym = Y(c.cover.p);
      pl = s.pnl;
    } else return;
    x.strokeStyle = col;
    x.setLineDash([3, 3]);
    x.beginPath();
    x.moveTo(xs, ys);
    x.lineTo(xm, ym);
    x.stroke();
    x.setLineDash([]);
    tri(x, xs, ys, 'down', col);
    tri(x, xm, ym, 'up', '#3ddc84');
    x.fillStyle = col;
    x.fillText('SELL ' + (MA[s.name] || ''), xs + 6, ys - 8 - idx * 0);
    x.fillStyle = pl >= 0 ? '#3ddc84' : '#ff5a5a';
    x.fillText('BUY ' + (pl * 100 >= 0 ? '+' : '') + (pl * 100).toFixed(0) + '%', xm + 6, ym + 16);
  });
  if(cv._measureStartKey&&cv._measureEndKey){
    const a=cv._hoverItems?.find(p=>p.key===cv._measureStartKey),z=cv._hoverItems?.find(p=>p.key===cv._measureEndKey);
    const ia=cv._hoverItems?.findIndex(p=>p.key===cv._measureStartKey),iz=cv._hoverItems?.findIndex(p=>p.key===cv._measureEndKey);
    if(a&&z&&ia>=0&&iz>=0){const left=Math.min(a.x,z.x),right=Math.max(a.x,z.x),from=Math.min(a.index,z.index),to=Math.max(a.index,z.index);let vol=0,dv=0;for(let i=from;i<=to;i++){const v=Number(b.v[i])||0;vol+=v;dv+=((Number(b.h[i])+Number(b.l[i])+Number(b.c[i]))/3)*v;}
      x.save();x.fillStyle='rgba(72,183,255,.14)';x.fillRect(left,T,Math.max(2,right-left),vy1-T);x.strokeStyle='#62c8ff';x.lineWidth=2;x.beginPath();x.moveTo(left,T);x.lineTo(left,vy1);x.moveTo(right,T);x.lineTo(right,vy1);x.stroke();
      const float=Number(cv._floatShares??c.float_shares),rot=float>0?`${(vol/float).toFixed(2)}x`:'Unavailable',compact=n=>Number.isFinite(n)?new Intl.NumberFormat('en-US',{notation:'compact',maximumFractionDigits:2}).format(n):'Unavailable';const lines=[`${a.label} to ${z.label}`,`Volume ${vol.toLocaleString('en-US',{maximumFractionDigits:0})}`,`Dollar volume ~$${dv.toLocaleString('en-US',{maximumFractionDigits:0})}`,`Float ${compact(float)} sh`,`Float rotation ${rot}${float>0?' (float may be stale)':''}`];
      x.font='bold 11px Consolas,monospace';const boxW=Math.min(W-L-R,Math.max(...lines.map(t=>x.measureText(t).width))+16),boxH=lines.length*16+12,bx=Math.min(W-R-boxW,Math.max(L,left+6)),by=T+5;x.fillStyle='rgba(6,13,21,.96)';x.fillRect(bx,by,boxW,boxH);x.strokeStyle='#62c8ff';x.strokeRect(bx,by,boxW,boxH);lines.forEach((line,k)=>{x.fillStyle=k===0?'#8cb8d6':'#edf6ff';x.fillText(line,bx+8,by+16+k*16)});x.restore();}
  }
  drawGapDeepDiveEventMarkers(cv,x,cv._rangeGeometry);
  cv._afterDraw?.();
}

  const api=Object.freeze({
  MC,
  MA,
  hm,
  tri,
  bone,
  aggregateEvBars,
  gapRangeResolve,
  gapRangePointAtIndex,
  gapRangeX,
  gapRangeTimestamp,
  gapRangeStats,
  drawGapDeepDiveRanges,
  drawGapDeepDiveRangeOverlay,
  scheduleGapDeepDiveRangeOverlay,
  drawGapDeepDiveCrosshair,
  gapDeepDiveEventMarkerAt,
  drawGapDeepDiveEventMarkers,
  scheduleGapDeepDiveHover,
  gapDeepDiveRangePoint,
  bindGapDeepDiveRangeInput,
  drawGapDeepDiveNavigator,
  setGapDeepDiveMouseMode,
  bindUnifiedChartFeatures,
  copyChartWithOverlays,
  chartFeatureUi,
  cancelUnifiedChartSelection,
  drawEv
  });
  global.SharedStockChart=api;
  global.drawKeywordGapEv=drawEv;
  global.drawSharedStockChart=drawEv;
  global.aggregateKeywordBars=aggregateEvBars;
  global.bindUnifiedChartFeatures=bindUnifiedChartFeatures;
  global.chartFeatureUi=chartFeatureUi;
  global.setGapDeepDiveMouseMode=setGapDeepDiveMouseMode;
  global.cancelUnifiedChartSelection=cancelUnifiedChartSelection;
})(window);
