import express from "express";
import cors from "cors";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";
import { authenticator } from "otplib";
import webpush from "web-push";

const PORT = Number(process.env.PORT || 10000);
const CLIENT_ID = (process.env.DHAN_CLIENT_ID || "").trim();
const PIN = (process.env.DHAN_PIN || "").trim();
const TOTP_SECRET = (process.env.DHAN_TOTP_SECRET || "").replace(/\s+/g, "").toUpperCase();
const STATIC_TOKEN = (process.env.DHAN_ACCESS_TOKEN || "").trim();
const VAPID_PUBLIC_KEY = (process.env.VAPID_PUBLIC_KEY || "").trim();
const VAPID_PRIVATE_KEY = (process.env.VAPID_PRIVATE_KEY || "").trim();
const VAPID_SUBJECT = (process.env.VAPID_SUBJECT || "mailto:alerts@example.com").trim();

const MAX_TICKS_PER_INSTRUMENT = Math.max(100, Number(process.env.MAX_TICKS_PER_INSTRUMENT || 3000));
const MAX_CANDLES_PER_INSTRUMENT = Math.max(100, Number(process.env.MAX_CANDLES_PER_INSTRUMENT || 2000));
const TICK_STALE_MS = Math.max(5000, Number(process.env.TICK_STALE_MS || 15000));
const LIVE_HISTORY_REFRESH_MS = Math.max(3000, Number(process.env.LIVE_HISTORY_REFRESH_MS || 10000));
const CANDLE_TIMEFRAME_MS = Math.max(10000, Number(process.env.CANDLE_TIMEFRAME_MS || 60000));
const OPTION_CHAIN_REFRESH_MS = Math.max(3000, Number(process.env.OPTION_CHAIN_REFRESH_MS || 3200));
const DEPTH_MAX_INSTRUMENTS = Math.min(50, Math.max(1, Number(process.env.DEPTH_MAX_INSTRUMENTS || 20)));
const DEFAULT_INDEX = String(process.env.DEFAULT_INDEX || "NIFTY").toUpperCase();

if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
}

const app = express();
app.use(cors({ origin: true }));
app.use(express.json({ limit: "2mb" }));
app.use((req, res, next) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  next();
});

const INDEXES = {
  NIFTY: { securityId: String(process.env.NIFTY_SECURITY_ID || "13"), segment: "IDX_I", feedSegment: "IDX_I", name: "NIFTY", step: 50 },
  BANKNIFTY: { securityId: String(process.env.BANKNIFTY_SECURITY_ID || "25"), segment: "IDX_I", feedSegment: "IDX_I", name: "BANKNIFTY", step: 100 },
  FINNIFTY: { securityId: String(process.env.FINNIFTY_SECURITY_ID || "27"), segment: "IDX_I", feedSegment: "IDX_I", name: "FINNIFTY", step: 50 },
  MIDCPNIFTY: { securityId: String(process.env.MIDCPNIFTY_SECURITY_ID || "442"), segment: "IDX_I", feedSegment: "IDX_I", name: "MIDCPNIFTY", step: 25 },
  SENSEX: { securityId: String(process.env.SENSEX_SECURITY_ID || "51"), segment: "IDX_I", feedSegment: "IDX_I", name: "SENSEX", step: 100 },
};

const state = {
  version: "4.1-STATE-MACHINE-COMPAT",
  indexKey: INDEXES[DEFAULT_INDEX] ? DEFAULT_INDEX : "NIFTY",
  expiry: null,
  spot: null,
  dhanConnected: false,
  depthConnected: false,
  feedLastMessage: null,
  feedLastMessageAgeMs: null,
  feedStale: true,
  lastTick: null,
  marketStatus: null,
  option: null,
  analytics: null,
  chain: { updatedAt: null, expiry: null, rows: [], atm: null, maxPain: null },
  subscriptions: [],
  server: { startedAt: Date.now(), ticks: 0, packets: 0, reconnects: 0, depthPackets: 0 },
};

const clients = new Set();
const pushSubscriptions = new Map();
const ticks = new Map();
const tickHistory = new Map();
const candles = new Map();
const instruments = new Map();
const optionMeta = new Map();
const depthBook = new Map();
const clientPrefs = new Map();

let dhanWs = null;
let depthWs = null;
let reconnectTimer = null;
let depthReconnectTimer = null;
let chainTimer = null;
let instrumentRefreshTimer = null;
let staleTimer = null;
let historyBroadcastTimer = null;
let accessToken = STATIC_TOKEN;
let tokenExpiry = 0;
let authStatus = STATIC_TOKEN ? "STATIC_TOKEN" : "WAITING";
let authLastError = null;
let authLastSuccessAt = null;
let chainBusy = false;

function now() { return Date.now(); }
function log(...a) { console.log(new Date().toISOString(), ...a); }
function clone(x) { return JSON.parse(JSON.stringify(x)); }
function safeNum(v, fallback = null) { const n = Number(v); return Number.isFinite(n) ? n : fallback; }
function key(seg, sec) { return `${seg}:${sec}`; }
function round(v, d = 4) { return v == null || !Number.isFinite(v) ? null : Number(v.toFixed(d)); }
function segmentName(code) { return ({0:"IDX_I",1:"NSE_EQ",2:"NSE_FNO",3:"NSE_CURRENCY",4:"BSE_EQ",5:"MCX_COMM",7:"BSE_CURRENCY",8:"BSE_FNO"})[Number(code)] || String(code); }
function broadcast(message) {
  const s = JSON.stringify(message);
  for (const ws of clients) if (ws.readyState === WebSocket.OPEN) ws.send(s);
}
function broadcastTo(ws, message) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message)); }

async function getAccessToken() {
  if (STATIC_TOKEN) { authStatus = "STATIC_TOKEN"; authLastError = null; return STATIC_TOKEN; }
  if (!CLIENT_ID || !PIN || !TOTP_SECRET) {
    authStatus = "CONFIG_ERROR";
    throw new Error("Set DHAN_ACCESS_TOKEN or DHAN_CLIENT_ID + DHAN_PIN + DHAN_TOTP_SECRET");
  }
  if (accessToken && now() < tokenExpiry - 60000) return accessToken;
  const totp = authenticator.generate(TOTP_SECRET);
  const url = `https://auth.dhan.co/app/generateAccessToken?dhanClientId=${encodeURIComponent(CLIENT_ID)}&pin=${encodeURIComponent(PIN)}&totp=${encodeURIComponent(totp)}`;
  const r = await fetch(url, { method:"POST", headers:{Accept:"application/json","Content-Type":"application/json"} });
  const text = await r.text();
  let j; try { j = JSON.parse(text); } catch { throw new Error(`Dhan auth returned non-JSON: ${text.slice(0,200)}`); }
  if (!r.ok || !j.accessToken) {
    authStatus = `FAILED_${r.status}`;
    authLastError = j.errorMessage || j.message || text.slice(0,200);
    throw new Error(`Dhan auth failed ${r.status}: ${authLastError}`);
  }
  accessToken = j.accessToken;
  authStatus = "AUTHENTICATED";
  authLastError = null;
  authLastSuccessAt = now();
  tokenExpiry = j.expiryTime ? new Date(j.expiryTime).getTime() : now() + 23*3600_000;
  return accessToken;
}

async function dhanPost(path, body) {
  const token = await getAccessToken();
  const headers = {"Content-Type":"application/json",Accept:"application/json","access-token":token};
  if (CLIENT_ID) headers["client-id"] = CLIENT_ID;
  const r = await fetch(`https://api.dhan.co/v2${path}`, {method:"POST",headers,body:JSON.stringify(body)});
  const text = await r.text(); let j; try {j=JSON.parse(text);} catch {j={raw:text};}
  if (!r.ok) throw new Error(`Dhan ${r.status}: ${text.slice(0,400)}`);
  return j;
}

function parseCsvLine(line) {
  const out=[]; let cur="", q=false;
  for(let i=0;i<line.length;i++){
    const ch=line[i];
    if(ch==='"'){if(q&&line[i+1]==='"'){cur+='"';i++;}else q=!q;}
    else if(ch===','&&!q){out.push(cur);cur="";} else cur+=ch;
  }
  out.push(cur); return out;
}

async function loadInstrumentMaster(){
  const r=await fetch("https://images.dhan.co/api-data/api-scrip-master.csv");
  if(!r.ok)throw new Error(`Instrument master ${r.status}`);
  const text=await r.text(); const lines=text.split(/\r?\n/).filter(Boolean);
  const headers=parseCsvLine(lines[0]).map(x=>x.trim()); const idx=n=>headers.indexOf(n);
  const ix={exch:idx("SEM_EXM_EXCH_ID"),seg:idx("SEM_SEGMENT"),id:idx("SEM_SMST_SECURITY_ID"),inst:idx("SEM_INSTRUMENT_NAME"),expiry:idx("SEM_EXPIRY_DATE"),strike:idx("SEM_STRIKE_PRICE"),opt:idx("SEM_OPTION_TYPE"),trading:idx("SEM_TRADING_SYMBOL"),custom:idx("SEM_CUSTOM_SYMBOL"),lot:idx("SEM_LOT_UNITS"),symbol:idx("SM_SYMBOL_NAME"),tick:idx("SEM_TICK_SIZE")};
  instruments.clear();
  const segmentMap={"NSE:I":"IDX_I","NSE:D":"NSE_FNO","NSE:E":"NSE_EQ","BSE:I":"BSE_IDX","BSE:D":"BSE_FNO","BSE:E":"BSE_EQ","MCX:M":"MCX_COMM"};
  for(let i=1;i<lines.length;i++){
    const c=parseCsvLine(lines[i]); if(c.length<8)continue;
    const ex=c[ix.exch],sg=c[ix.seg],es=segmentMap[`${ex}:${sg}`]; if(!es)continue;
    const id=c[ix.id]; if(!id)continue;
    instruments.set(key(es,id),{securityId:String(id),exchangeSegment:es,exchange:ex,segmentCode:sg,instrument:c[ix.inst]||null,expiry:c[ix.expiry]||null,strike:safeNum(c[ix.strike],0),optionType:c[ix.opt]||null,tradingSymbol:c[ix.trading]||null,customSymbol:c[ix.custom]||null,symbol:c[ix.symbol]||null,lotSize:safeNum(c[ix.lot],1),tickSize:safeNum(c[ix.tick],null)});
  }
  log(`Loaded ${instruments.size} Dhan instruments`);
}

function underlyingInfo(){return INDEXES[state.indexKey]||INDEXES.NIFTY;}
async function getExpiryList(){
  const x=underlyingInfo();
  const j=await dhanPost("/optionchain/expirylist",{UnderlyingScrip:Number(x.securityId),UnderlyingSeg:x.segment});
  const dates=Array.isArray(j.data)?j.data:[];
  if(dates.length&&(!state.expiry||!dates.includes(state.expiry)))state.expiry=dates[0];
  return dates;
}
async function getOptionChain(){const x=underlyingInfo();return dhanPost("/optionchain",{UnderlyingScrip:Number(x.securityId),UnderlyingSeg:x.segment,Expiry:state.expiry});}
function bsTime(expiry){return Math.max((new Date(expiry).getTime()-now())/86400000/365,1/(365*24*60));}
function normPdf(x){return Math.exp(-0.5*x*x)/Math.sqrt(2*Math.PI);}
function normCdf(x){const a1=.254829592,a2=-.284496736,a3=1.421413741,a4=-1.453152027,a5=1.061405429,p=.3275911;const sign=x<0?-1:1;const t=1/(1+p*Math.abs(x));const y=1-(((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*Math.exp(-x*x);return .5*(1+sign*y);}
function hiddenGreeks({spot,strike,iv,expiry,optionType,rate=.06}){
  const S=Number(spot),K=Number(strike),sigma=Math.max(Number(iv)/100,.0001),T=bsTime(expiry); if(!(S>0&&K>0))return{};
  const isCall=optionType==="CE"; const d1=(Math.log(S/K)+(rate+sigma*sigma/2)*T)/(sigma*Math.sqrt(T)); const d2=d1-sigma*Math.sqrt(T); const pdf=normPdf(d1);
  const gamma=pdf/(S*sigma*Math.sqrt(T)); const vega=S*pdf*Math.sqrt(T)/100; const delta=isCall?normCdf(d1):normCdf(d1)-1;
  const theta=(-S*pdf*sigma/(2*Math.sqrt(T))-(isCall?rate*K*Math.exp(-rate*T)*normCdf(d2):-rate*K*Math.exp(-rate*T)*normCdf(-d2)))/365;
  const vanna=-pdf*d2/sigma/100, vomma=vega*d1*d2/sigma;
  return {delta,gamma,vega,theta,vanna,vomma};
}
function optionRow(strike,leg,type,expiry,spot){
  if(!leg)return null; const ltp=safeNum(leg.last_price,0),oi=safeNum(leg.oi,0),volume=safeNum(leg.volume,0),prevOI=safeNum(leg.previous_oi,0),prevVol=safeNum(leg.previous_volume,0),iv=safeNum(leg.implied_volatility,0);
  const g=hiddenGreeks({spot,strike,iv,expiry,optionType:type}); const lot=safeNum(instruments.get(key("NSE_FNO",leg.security_id))?.lotSize,1); const gex=(g.gamma||safeNum(leg.greeks?.gamma,0))*oi*lot*spot*spot/1e7;
  return {strike,type,securityId:String(leg.security_id),ltp,oi,previousOI:prevOI,changeOI:oi-prevOI,volume,previousVolume:prevVol,changeVolume:volume-prevVol,iv,bid:safeNum(leg.top_bid_price,0),ask:safeNum(leg.top_ask_price,0),bidQty:safeNum(leg.top_bid_quantity,0),askQty:safeNum(leg.top_ask_quantity,0),averagePrice:safeNum(leg.average_price,0),dhanGreeks:leg.greeks||{},hiddenGreeks:g,gexProxy:round(gex,3),notionalOI:round(oi*lot*strike,2)};
}
function buildAnalytics(rows,spot){
  const ce=rows.filter(x=>x.type==="CE"),pe=rows.filter(x=>x.type==="PE"); const sum=(a,k)=>a.reduce((s,x)=>s+(Number(x[k])||0),0);
  const callOI=sum(ce,"oi"),putOI=sum(pe,"oi"),callVol=sum(ce,"volume"),putVol=sum(pe,"volume"); const pcr=callOI?putOI/callOI:null,pcrVol=callVol?putVol/callVol:null;
  const maxCallWall=ce.reduce((a,b)=>!a||b.oi>a.oi?b:a,null),maxPutWall=pe.reduce((a,b)=>!a||b.oi>a.oi?b:a,null);
  const maxCallChWall=ce.reduce((a,b)=>!a||b.changeOI>a.changeOI?b:a,null),maxPutChWall=pe.reduce((a,b)=>!a||b.changeOI>a.changeOI?b:a,null);
  const atm=rows.reduce((a,b)=>!a||Math.abs(b.strike-spot)<Math.abs(a.strike-spot)?b:a,null)?.strike||null; const strikes=[...new Set(rows.map(x=>x.strike))].sort((a,b)=>a-b);
  let minPain=null,minPainVal=Infinity; for(const k of strikes){let pain=0;for(const x of ce)pain+=Math.max(0,k-x.strike)*x.oi;for(const x of pe)pain+=Math.max(0,x.strike-k)*x.oi;if(pain<minPainVal){minPainVal=pain;minPain=k;}}
  const gexCE=sum(ce,"gexProxy"),gexPE=sum(pe,"gexProxy"),dealerProxy=gexCE-gexPE; const atmRows=rows.filter(x=>x.strike===atm); const ivAtm=atmRows.reduce((s,x)=>s+(x.iv||0),0)/(atmRows.length||1);
  return {atm,pcr:round(pcr,4),pcrVolume:round(pcrVol,4),callOI,putOI,callChangeOI:sum(ce,"changeOI"),putChangeOI:sum(pe,"changeOI"),callVolume:callVol,putVolume:putVol,callChangeVolume:sum(ce,"changeVolume"),putChangeVolume:sum(pe,"changeVolume"),callOIWall:maxCallWall?.strike??null,putOIWall:maxPutWall?.strike??null,callChangeOIWall:maxCallChWall?.strike??null,putChangeOIWall:maxPutChWall?.strike??null,maxPain:minPain,maxPainValue:minPainVal===Infinity?null:minPainVal,atmIV:round(ivAtm,3),totalGEXProxy:round(dealerProxy,3),callGEXProxy:round(gexCE,3),putGEXProxy:round(gexPE,3),dealerHedgePressureProxy:round(-dealerProxy,3)};
}
async function refreshChain(){
  if(chainBusy)return; chainBusy=true;
  try{
    if(!state.expiry)await getExpiryList();
    const j=await getOptionChain(); const spot=safeNum(j?.data?.last_price,state.spot); if(spot)state.spot=spot;
    const oc=j?.data?.oc||{},rows=[];
    for(const [ks,row] of Object.entries(oc)){const strike=Number(ks);if(!Number.isFinite(strike))continue;const ce=optionRow(strike,row.ce,"CE",state.expiry,state.spot),pe=optionRow(strike,row.pe,"PE",state.expiry,state.spot);if(ce)rows.push(ce);if(pe)rows.push(pe);if(ce)optionMeta.set(key("NSE_FNO",ce.securityId),ce);if(pe)optionMeta.set(key("NSE_FNO",pe.securityId),pe);}
    state.analytics=buildAnalytics(rows,state.spot); state.chain={updatedAt:now(),expiry:state.expiry,rows,atm:state.analytics.atm,maxPain:state.analytics.maxPain}; state.option=selectPrimaryOption(rows); await syncMarketSubscriptions(rows);
    broadcast({type:"optionChain",chain:clone(state.chain),analytics:clone(state.analytics)}); broadcast({type:"state",state:publicState()});
  }catch(e){log("Option chain:",e.message);broadcast({type:"error",message:e.message});}finally{chainBusy=false;}
}
function selectPrimaryOption(rows){const side=state.analytics?.pcr!=null&&state.analytics.pcr<.9?"CE":"PE";const pool=rows.filter(x=>x.type===side&&x.ltp>0&&x.ltp>=5&&x.ltp<=30);const sorted=(pool.length?pool:rows.filter(x=>x.type===side&&x.ltp>0)).sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot));return sorted[0]||null;}
async function syncMarketSubscriptions(rows){
  const base=[{ExchangeSegment:underlyingInfo().feedSegment,SecurityId:underlyingInfo().securityId}];
  const near=[...new Map(rows.map(x=>[x.securityId,x])).values()].sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot)).slice(0,600);
  const inst=[...base,...near.map(x=>({ExchangeSegment:"NSE_FNO",SecurityId:x.securityId}))]; state.subscriptions=inst; subscribe(dhanWs,inst,21); syncDepthSubscriptions(near.slice(0,DEPTH_MAX_INSTRUMENTS));
}
function subscribe(ws,insts,requestCode=21){if(!ws||ws.readyState!==WebSocket.OPEN||!insts?.length)return;const unique=[...new Map(insts.map(x=>[key(x.ExchangeSegment,x.SecurityId),x])).values()];for(let i=0;i<unique.length;i+=100){const a=unique.slice(i,i+100);try{ws.send(JSON.stringify({RequestCode:requestCode,InstrumentCount:a.length,InstrumentList:a}));}catch(e){log("subscribe:",e.message);}}}

function decodeOne(buf){
  if(buf.length<8)return null; const dv=new DataView(buf.buffer,buf.byteOffset,buf.byteLength); const code=dv.getUint8(2),seg=dv.getUint8(3),sec=dv.getUint32(4,true),len=dv.getUint16(0,true)||buf.length;
  const f=o=>dv.getFloat32(o,true),i=o=>dv.getInt32(o,true),u=o=>dv.getUint32(o,true),i16=o=>dv.getInt16(o,true),u16=o=>dv.getUint16(o,true);
  const base={code,seg,sec,len};
  if(code===2&&buf.length>=16)return {...base,type:"ticker",ltp:f(8),ltt:u(12)};
  if(code===4&&buf.length>=51)return {...base,type:"quote",ltp:f(8),ltq:u16(12),ltt:u(14),avgPrice:f(18),volume:u(22),sellQty:u(26),buyQty:u(30),open:f(34),close:f(38),high:f(42),low:f(46)};
  if(code===5&&buf.length>=12)return {...base,type:"oi",oi:u(8)};
  if(code===6&&buf.length>=16)return {...base,type:"prev",prevClose:f(8),prevOI:u(12)};
  if(code===7)return {...base,type:"marketStatus",statusCode:buf.length>=10?u16(8):null};
  if(code===8&&buf.length>=163){const depth=[];for(let n=0;n<5;n++){const o=63+n*20;depth.push({bidQty:i(o),askQty:i(o+4),bidOrders:i16(o+8),askOrders:i16(o+10),bid:f(o+12),ask:f(o+16)});}return {...base,type:"full",ltp:f(8),ltq:u16(12),ltt:u(14),avgPrice:f(18),volume:u(22),sellQty:i(26),buyQty:i(30),oi:i(34),oiDayHigh:i(38),oiDayLow:i(42),open:f(46),close:f(50),high:f(54),low:f(58),depth};}
  if(code===41||code===51){const side=code===41?"bid":"ask",levels=[];for(let n=0;n<20;n++){const o=12+n*16;if(o+16>buf.length)break;levels.push({price:dv.getFloat64(o,true),qty:dv.getUint32(o+8,true),orders:dv.getUint32(o+12,true)});}return {...base,type:"depth20",side,levels};}
  if(code===50)return {...base,type:"disconnect",disconnectCode:buf.length>=10?u16(8):null};
  return base;
}
function decodeMessage(data){const b=Buffer.from(data),out=[];let off=0;while(off+8<=b.length){const len=b.readUInt16LE(off)||b.length-off;if(len<8||off+len>b.length&&off+len>b.length+1)break;const end=Math.min(b.length,off+len);const x=decodeOne(b.subarray(off,end));if(x)out.push(x);if(end<=off)break;off=end;}return out;}

function updateDepth(p){
  const k=key(segmentName(p.seg),p.sec); const book=depthBook.get(k)||{bid:[],ask:[],updatedAt:null}; book[p.side]=p.levels||[]; book.updatedAt=now(); depthBook.set(k,book); const t=ticks.get(k); if(t)t.depth20=book;
  state.server.depthPackets++; state.feedLastMessage=now(); state.feedLastMessageAgeMs=0;
  if(state.dhanConnected) broadcastDepthTick(k,book);
}
function pushTickHistory(k,t){let arr=tickHistory.get(k)||[];arr.push(t);if(arr.length>MAX_TICKS_PER_INSTRUMENT)arr=arr.slice(-MAX_TICKS_PER_INSTRUMENT);tickHistory.set(k,arr);}
function updateCandle(k,t){
  if(!Number.isFinite(t.ltp))return null; const tm=Math.floor((t.ltt||now())/CANDLE_TIMEFRAME_MS)*CANDLE_TIMEFRAME_MS; let arr=candles.get(k)||[]; let c=arr[arr.length-1]; let newBar=false;
  if(!c||c.time!==tm){c={time:tm,open:t.ltp,high:t.ltp,low:t.ltp,close:t.ltp,volume:0};arr.push(c);newBar=true;} else {c.high=Math.max(c.high,t.ltp);c.low=Math.min(c.low,t.ltp);c.close=t.ltp;}
  if(Number.isFinite(t.tradeQty))c.volume+=Math.max(0,t.tradeQty); else if(Number.isFinite(t.volumeDelta))c.volume+=Math.max(0,t.volumeDelta);
  if(arr.length>MAX_CANDLES_PER_INSTRUMENT)arr=arr.slice(-MAX_CANDLES_PER_INSTRUMENT);candles.set(k,arr); return {candle:c,newBar};
}
function calcSma(a,n){const x=a.slice(-n).filter(Number.isFinite);return x.length?x.reduce((s,v)=>s+v,0)/x.length:null;}
function calcEma(a,n){const x=a.filter(Number.isFinite);if(!x.length)return null;const k=2/(n+1);let e=x[0];for(let i=1;i<x.length;i++)e=x[i]*k+e*(1-k);return e;}
function rsiSeries(a,n=5){const x=a.filter(Number.isFinite);if(x.length<=n)return [];let gain=0,loss=0;for(let i=1;i<=n;i++){const d=x[i]-x[i-1];gain+=Math.max(d,0);loss+=Math.max(-d,0);}gain/=n;loss/=n;const out=[];out.push(loss===0?100:100-(100/(1+gain/loss)));for(let i=n+1;i<x.length;i++){const d=x[i]-x[i-1];gain=(gain*(n-1)+Math.max(d,0))/n;loss=(loss*(n-1)+Math.max(-d,0))/n;out.push(loss===0?100:100-(100/(1+gain/loss)));}return out;}
function calcRsi(a,n=5){const r=rsiSeries(a,n);return r.length?r[r.length-1]:null;}
function calcDema(a,n=14){const e1=calcEma(a,n);const e2Series=[];let e= null;const k2=2/(n+1);for(const v of a.filter(Number.isFinite)){e=e==null?v:v*k2+e*(1-k2);e2Series.push(e);}const e2=calcEma(e2Series,n);return e1==null?null:2*e1-e2;}
function indicatorSnapshot(k){const arr=candles.get(k)||[], closes=arr.map(x=>x.close), vols=arr.map(x=>x.volume);const rs=rsiSeries(closes,5),rsi=rs.length?rs[rs.length-1]:null,re=calcEma(rs,14),dema=calcDema(closes,14),vs=calcSma(vols,250);return {rsi5:rsi,rsiEma14:re,dema14:dema,volSma250:vs};}

function normalizedTick(k,t){const ind=indicatorSnapshot(k);return {type:"tick",data:{close:t.ltp,ltp:t.ltp,open:t.open??t.ltp,high:t.high??t.ltp,low:t.low??t.ltp,volume:Number(t.tradeQty||0),timestamp:t.ltt||now(),rsi5:ind.rsi5,rsiEma14:ind.rsiEma14,dema14:ind.dema14,volSma250:ind.volSma250,securityId:t.securityId,exchangeSegment:t.exchangeSegment,depth:t.depth20||null}};}
function broadcastIndexTick(k,t){const info=underlyingInfo();if(k!==key(info.feedSegment,info.securityId))return;const msg=normalizedTick(k,t);for(const ws of clients){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg));}}
function broadcastDepthTick(k,book){const info=underlyingInfo();if(k!==key(info.feedSegment,info.securityId))return;for(const ws of clients){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify({type:"depth",data:{securityId:info.securityId,exchangeSegment:info.feedSegment,depth20:book}}));}}
function updateTick(p){
  if(!p||p.sec==null)return; const segName=segmentName(p.seg),k=key(segName,p.sec); let t=ticks.get(k)||{securityId:String(p.sec),exchangeSegment:segName,securityIdNum:p.sec};
  const previousVolume=t.volume; Object.assign(t,{updatedAt:now()}); if(p.ltp!=null)t.ltp=p.ltp;if(p.ltt!=null)t.ltt=p.ltt*1000;if(p.ltq!=null){t.lastTradeQty=p.ltq;t.tradeQty=p.ltq;}if(p.avgPrice!=null)t.avgPrice=p.avgPrice;if(p.volume!=null){if(t.sessionStartVolume==null)t.sessionStartVolume=p.volume;t.volume=p.volume;t.volumeDelta=previousVolume==null?0:Math.max(0,p.volume-previousVolume);}if(p.sellQty!=null)t.sellQuantity=p.sellQty;if(p.buyQty!=null)t.buyQuantity=p.buyQty;
  for(const q of ["open","close","high","low","oi","oiDayHigh","oiDayLow","prevClose","prevOI"])if(p[q]!=null)t[q]=p[q]; if(t.oi!=null){if(t.sessionStartOI==null)t.sessionStartOI=t.oi;t.changeOI=t.oi-t.sessionStartOI;} if(p.depth)t.depth=p.depth;if(p.type==="prev"){t.prevClose=p.prevClose;t.prevOI=p.prevOI;if(t.oi!=null)t.changeOI=t.oi-p.prevOI;}
  if(p.type==="depth20"){updateDepth(p);return;}
  ticks.set(k,t); pushTickHistory(k,{...t}); state.server.ticks++; state.feedLastMessage=now(); state.feedLastMessageAgeMs=0;
  if(segName===underlyingInfo().feedSegment&&String(p.sec)===underlyingInfo().securityId){state.spot=p.ltp??state.spot;state.lastTick=clone(t);}
  const meta=optionMeta.get(k);if(meta)Object.assign(meta,{ltp:t.ltp,volume:t.volume,changeVolume:t.volumeDelta,oi:t.oi,changeOI:t.changeOI,bid:t.depth?.[0]?.bid??meta.bid,ask:t.depth?.[0]?.ask??meta.ask});
  updateCandle(k,t); broadcastIndexTick(k,t);
}

function publicState(){return {...state,ticks:undefined,config:{maxTicksPerInstrument:MAX_TICKS_PER_INSTRUMENT,maxCandlesPerInstrument:MAX_CANDLES_PER_INSTRUMENT,tickStaleMs:TICK_STALE_MS,candleTimeframeMs:CANDLE_TIMEFRAME_MS}};}
function staleCheck(){state.feedLastMessageAgeMs=state.feedLastMessage?now()-state.feedLastMessage:null;state.feedStale=!state.dhanConnected||!state.feedLastMessage||state.feedLastMessageAgeMs>TICK_STALE_MS;}
function syncDepthSubscriptions(rows){if(!depthWs||depthWs.readyState!==WebSocket.OPEN)return;const inst=rows.slice(0,DEPTH_MAX_INSTRUMENTS).map(x=>({ExchangeSegment:"NSE_FNO",SecurityId:x.securityId}));for(let i=0;i<inst.length;i+=50){const a=inst.slice(i,i+50);try{depthWs.send(JSON.stringify({RequestCode:23,InstrumentCount:a.length,InstrumentList:a}));}catch(e){log("depth subscribe:",e.message);}}}
function rowsWithLive(){return state.chain.rows.map(r=>{const t=ticks.get(key("NSE_FNO",r.securityId));const d=depthBook.get(key("NSE_FNO",r.securityId));return t?{...r,ltp:t.ltp,volume:t.volume,changeVolume:t.volumeDelta,oi:t.oi,changeOI:t.changeOI,bid:d?.bid?.[0]?.price??r.bid,ask:d?.ask?.[0]?.price??r.ask,bidQty:d?.bid?.[0]?.qty??r.bidQty,askQty:d?.ask?.[0]?.qty??r.askQty,depth20:d||null}:r;});}

function connectFeed(){
  clearTimeout(reconnectTimer);try{dhanWs?.close();}catch{}
  getAccessToken().then(token=>{
    const url=`wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;
    dhanWs=new WebSocket(url);
    dhanWs.on("open",()=>{state.dhanConnected=true;state.server.reconnects++;state.feedLastMessage=now();state.feedLastMessageAgeMs=0;state.feedStale=false;log("Dhan live feed connected");subscribe(dhanWs,state.subscriptions.length?state.subscriptions:[{ExchangeSegment:underlyingInfo().feedSegment,SecurityId:underlyingInfo().securityId}],21);broadcast({type:"state",state:publicState()});});
    dhanWs.on("message",data=>{for(const p of decodeMessage(data)){state.server.packets++;updateTick(p);if(p.type==="marketStatus")state.marketStatus=p.statusCode;}});
    dhanWs.on("close",(code,reason)=>{state.dhanConnected=false;staleCheck();log(`Dhan feed closed ${code} ${reason?.toString()||""}`);broadcast({type:"state",state:publicState()});reconnectTimer=setTimeout(connectFeed,5000);});
    dhanWs.on("error",e=>log("Dhan feed error",e.message));
  }).catch(e=>{state.dhanConnected=false;authLastError=e.message;staleCheck();log("Dhan auth failed",e.message);broadcast({type:"error",message:e.message});reconnectTimer=setTimeout(connectFeed,10000);});
}
function connectDepth(){
  clearTimeout(depthReconnectTimer);try{depthWs?.close();}catch{}
  getAccessToken().then(token=>{
    depthWs=new WebSocket(`wss://depth-api-feed.dhan.co/twentydepth?token=${encodeURIComponent(token)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`);
    depthWs.on("open",()=>{state.depthConnected=true;syncDepthSubscriptions([...state.chain.rows].sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot)).slice(0,DEPTH_MAX_INSTRUMENTS));broadcast({type:"state",state:publicState()});});
    depthWs.on("message",data=>{for(const p of decodeMessage(data)){if(p.type==="depth20")updateDepth(p);}});
    depthWs.on("close",()=>{state.depthConnected=false;broadcast({type:"state",state:publicState()});depthReconnectTimer=setTimeout(connectDepth,7000);});
    depthWs.on("error",e=>log("Depth feed error",e.message));
  }).catch(e=>log("Depth auth",e.message));
}

app.get("/",(_q,res)=>{
  res.status(200).type("html").send(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="cache-control" content="no-store"><title>Bharati Unique Backend V4.1</title><style>body{font-family:system-ui,sans-serif;background:#0b1220;color:#eef2ff;padding:24px;line-height:1.5}.card{background:#111b2e;padding:22px;border-radius:16px;max-width:760px;margin:auto}h1{margin:0 0 8px}.good{color:#4ade80}.warn{color:#fbbf24}.bad{color:#fb7185}.muted{color:#94a3b8}.row{margin:10px 0}code{background:#1e293b;padding:2px 6px;border-radius:6px}a{color:#7dd3fc;margin-right:8px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.metric{background:#0d1728;padding:12px;border-radius:12px}.metric small{display:block;color:#94a3b8}.metric b{font-size:17px}@media(max-width:600px){.grid{grid-template-columns:1fr}}</style></head><body><div class="card"><h1>Bharati Unique Backend V4.1</h1><div id="online" class="good">● ONLINE</div><p>Universal market-data gateway for the RSI DEMA VOLUME state-machine PWA.</p><p>Version: <code>${state.version}</code></p><div class="grid"><div class="metric">Dhan feed<br><b id="dhan">CHECKING...</b></div><div class="metric">L20 depth<br><b id="depth">CHECKING...</b></div><div class="metric">Feed state<br><b id="stale">CHECKING...</b></div><div class="metric">Last message<br><b id="last">—</b></div></div><p class="muted" id="stats">Checking live status...</p><p><a href="/health">Health</a><a href="/api/health">API Health JSON</a><a href="/api/state">State</a><a href="/api/analytics">Analytics</a><a href="/api/option-chain">Option Chain</a><a href="/api/ticks">Ticks</a></p><script>async function refresh(){try{const r=await fetch('/api/health',{cache:'no-store'}),j=await r.json();const d=document.getElementById('dhan'),dep=document.getElementById('depth'),st=document.getElementById('stale');d.textContent=j.dhanConnected?'CONNECTED':'CONNECTING';d.className=j.dhanConnected?'good':'warn';dep.textContent=j.depthConnected?'CONNECTED':'CONNECTING';dep.className=j.depthConnected?'good':'warn';st.textContent=j.feedStale?'STALE':'LIVE';st.className=j.feedStale?'bad':'good';document.getElementById('last').textContent=j.feedLastMessage?new Date(j.feedLastMessage).toLocaleTimeString():'—';document.getElementById('stats').textContent='Ticks: '+j.ticks+' · Packets: '+j.packets+' · Depth packets: '+j.depthPackets+' · Subscriptions: '+j.subscriptions+' · Chain rows: '+j.chainRows+' · Age: '+(j.feedLastMessageAgeMs==null?'—':j.feedLastMessageAgeMs+' ms');}catch(e){document.getElementById('online').textContent='● HEALTH ERROR';document.getElementById('online').className='bad';}}refresh();setInterval(refresh,3000);</script></div></body></html>`);
});
app.get("/health",(_q,res)=>res.status(200).type("text").send(`OK\nBharati Unique Backend V4.1\nDhan feed: ${state.dhanConnected?"CONNECTED":"CONNECTING"}\nDepth feed: ${state.depthConnected?"CONNECTED":"CONNECTING"}\nFeed stale: ${state.feedStale}\n`));
app.get("/api/health",(_q,res)=>{staleCheck();res.json({ok:true,version:state.version,dhanConnected:state.dhanConnected,depthConnected:state.depthConnected,feedLastMessage:state.feedLastMessage,feedLastMessageAgeMs:state.feedLastMessageAgeMs,feedStale:state.feedStale,authStatus,authLastSuccessAt,authLastError:authLastError?String(authLastError).slice(0,200):null,ticks:state.server.ticks,packets:state.server.packets,depthPackets:state.server.depthPackets,subscriptions:state.subscriptions.length,chainRows:state.chain.rows.length,lastTick:state.lastTick,time:now()});});
app.get("/api/config",(_q,res)=>res.json({ok:true,version:state.version,indexes:INDEXES,vapidPublicKey:VAPID_PUBLIC_KEY,features:["tick","quote","oi","volume","depth20","option-chain","greeks","hidden-greeks","pcr","max-pain","historical","websocket","pwa-compatible"]}));
app.get("/api/state",(_q,res)=>res.json(publicState()));
app.get("/api/tick",(q,res)=>{const seg=q.query.segment||"NSE_FNO",sec=String(q.query.securityId||"");const t=ticks.get(key(seg,sec));res.json({ok:!!t,data:t||null});});
app.get("/api/ticks",(_q,res)=>res.json({ok:true,data:[...ticks.values()]}));
app.get("/api/option-chain",(_q,res)=>res.json({ok:true,expiry:state.expiry,spot:state.spot,rows:rowsWithLive(),analytics:state.analytics,updatedAt:state.chain.updatedAt}));
app.get("/api/analytics",(_q,res)=>res.json({ok:true,spot:state.spot,analytics:state.analytics}));
app.get("/api/history",(q,res)=>{const seg=q.query.segment||"IDX_I",sec=String(q.query.securityId||underlyingInfo().securityId);res.json({ok:true,data:candles.get(key(seg,sec))||[]});});
app.get("/api/tick-history",(q,res)=>{const seg=q.query.segment||"IDX_I",sec=String(q.query.securityId||underlyingInfo().securityId);res.json({ok:true,data:tickHistory.get(key(seg,sec))||[]});});
app.get("/api/depth",(q,res)=>{const seg=q.query.segment||"NSE_FNO",sec=String(q.query.securityId||"");res.json({ok:true,data:depthBook.get(key(seg,sec))||null});});
app.get("/api/instruments",(q,res)=>{const search=String(q.query.search||"").toUpperCase(),limit=Math.min(Number(q.query.limit||100),1000);const arr=[...instruments.values()].filter(x=>!search||JSON.stringify(x).toUpperCase().includes(search)).slice(0,limit);res.json({ok:true,count:arr.length,data:arr});});
app.post("/api/index",async(req,res)=>{const k=String(req.body?.index||"NIFTY").toUpperCase();if(!INDEXES[k])return res.status(400).json({ok:false,error:`Unknown index. Use ${Object.keys(INDEXES).join(", ")}`});state.indexKey=k;state.expiry=null;state.chain={updatedAt:null,expiry:null,rows:[],atm:null,maxPain:null};state.analytics=null;await refreshChain();broadcast({type:"state",state:publicState()});res.json({ok:true,state:publicState()});});
app.post("/api/expiry",async(_q,res)=>{try{const dates=await getExpiryList();res.json({ok:true,dates,selected:state.expiry});}catch(e){res.status(500).json({ok:false,error:e.message});}});
app.post("/api/expiry/select",async(req,res)=>{const x=String(req.body?.expiry||"");if(!x)return res.status(400).json({ok:false,error:"expiry required"});state.expiry=x;await refreshChain();res.json({ok:true,state:publicState()});});
app.post("/api/subscribe",(req,res)=>{const list=Array.isArray(req.body?.instruments)?req.body.instruments:[];const clean=list.map(x=>({ExchangeSegment:String(x.ExchangeSegment||x.exchangeSegment),SecurityId:String(x.SecurityId||x.securityId)})).filter(x=>x.ExchangeSegment&&x.SecurityId);subscribe(dhanWs,clean,21);state.subscriptions=[...new Map([...state.subscriptions,...clean].map(x=>[key(x.ExchangeSegment,x.SecurityId),x])).values()];res.json({ok:true,count:state.subscriptions.length});});
app.post("/api/push/subscribe",(req,res)=>{const s=req.body;if(!s?.endpoint)return res.status(400).json({ok:false,error:"Invalid subscription"});pushSubscriptions.set(s.endpoint,s);res.json({ok:true});});
app.post("/api/push/test",async(_q,res)=>{if(!VAPID_PUBLIC_KEY||!VAPID_PRIVATE_KEY)return res.status(503).json({ok:false,error:"VAPID keys are not configured"});let sent=0;for(const[k,s]of pushSubscriptions){try{await webpush.sendNotification(s,JSON.stringify({title:"Bharati V4 Test",body:"Live backend push is working."}));sent++;}catch(e){if([404,410].includes(e.statusCode))pushSubscriptions.delete(k);}}res.json({ok:true,devices:pushSubscriptions.size,sent});});

const server=http.createServer(app); const wss=new WebSocketServer({server,path:"/ws"});
wss.on("connection",ws=>{
  clients.add(ws); clientPrefs.set(ws,{symbol:state.indexKey,timeframe:"1m"});
  broadcastTo(ws,{type:"hello",version:state.version,features:["tick","quote","oi","volume","depth20","chain","analytics","rsi5","rsiEma14","dema14","volSma250"]});
  broadcastTo(ws,{type:"state",state:publicState()});
  if(state.lastTick)broadcastTo(ws,normalizedTick(key(underlyingInfo().feedSegment,underlyingInfo().securityId),state.lastTick));
  ws.on("message",raw=>{
    let m;try{m=JSON.parse(raw.toString());}catch{return;}
    if(m?.type==="subscribe"){
      const sym=String(m.symbol||"NIFTY").toUpperCase(); if(INDEXES[sym]){state.indexKey=sym;state.expiry=null;clientPrefs.set(ws,{symbol:sym,timeframe:String(m.timeframe||"1m")});const x=INDEXES[sym];subscribe(dhanWs,[{ExchangeSegment:x.feedSegment,SecurityId:x.securityId}],21);if(state.lastTick&&String(state.lastTick.securityId)===x.securityId)broadcastTo(ws,normalizedTick(key(x.feedSegment,x.securityId),state.lastTick));}
      return;
    }
    if(m?.type==="ping")broadcastTo(ws,{type:"pong",time:now()});
  });
  ws.on("close",()=>{clients.delete(ws);clientPrefs.delete(ws);});
});

async function boot(){
  try{await loadInstrumentMaster();}catch(e){log("Instrument master:",e.message);}
  try{await getExpiryList();await refreshChain();}catch(e){log("Chain startup:",e.message);}
  connectFeed(); connectDepth();
  chainTimer=setInterval(refreshChain,OPTION_CHAIN_REFRESH_MS);
  instrumentRefreshTimer=setInterval(async()=>{try{await loadInstrumentMaster();}catch(e){log("Instrument refresh:",e.message);}},6*3600_000);
  staleTimer=setInterval(()=>{staleCheck();broadcast({type:"state",state:publicState()});},3000);
  historyBroadcastTimer=setInterval(()=>{if(state.lastTick)broadcast({type:"state",state:publicState()});},LIVE_HISTORY_REFRESH_MS);
}
server.listen(PORT,"0.0.0.0",()=>{log(`Bharati Unique Backend V4.1 listening on 0.0.0.0:${PORT}`);boot();});
