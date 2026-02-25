import{r as h,j as t,c as be,a as fe,u as we,L as ke}from"./index-B_OPaOyb.js";/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Z=(...r)=>r.filter((c,d,p)=>!!c&&c.trim()!==""&&p.indexOf(c)===d).join(" ").trim();/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const je=r=>r.replace(/([a-z0-9])([A-Z])/g,"$1-$2").toLowerCase();/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const ve=r=>r.replace(/^([A-Z])|[\s-_]+(\w)/g,(c,d,p)=>p?p.toUpperCase():d.toLowerCase());/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const V=r=>{const c=ve(r);return c.charAt(0).toUpperCase()+c.slice(1)};/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */var Ne={xmlns:"http://www.w3.org/2000/svg",width:24,height:24,viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:2,strokeLinecap:"round",strokeLinejoin:"round"};/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Me=r=>{for(const c in r)if(c.startsWith("aria-")||c==="role"||c==="title")return!0;return!1};/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const $e=h.forwardRef(({color:r="currentColor",size:c=24,strokeWidth:d=2,absoluteStrokeWidth:p,className:g="",children:b,iconNode:y,...v},N)=>h.createElement("svg",{ref:N,...Ne,width:c,height:c,stroke:r,strokeWidth:p?Number(d)*24/Number(c):d,className:Z("lucide",g),...!b&&!Me(v)&&{"aria-hidden":"true"},...v},[...y.map(([$,M])=>h.createElement($,M)),...Array.isArray(b)?b:[b]]));/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const l=(r,c)=>{const d=h.forwardRef(({className:p,...g},b)=>h.createElement($e,{ref:b,iconNode:c,className:Z(`lucide-${je(V(r))}`,`lucide-${r}`,p),...g}));return d.displayName=V(r),d};/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Se=[["rect",{width:"20",height:"5",x:"2",y:"3",rx:"1",key:"1wp1u1"}],["path",{d:"M4 8v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8",key:"1s80jp"}],["path",{d:"M10 12h4",key:"a56b0p"}]],K=l("archive",Se);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ce=[["path",{d:"M12 7v14",key:"1akyts"}],["path",{d:"M3 18a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h5a4 4 0 0 1 4 4 4 4 0 0 1 4-4h5a1 1 0 0 1 1 1v13a1 1 0 0 1-1 1h-6a3 3 0 0 0-3 3 3 3 0 0 0-3-3z",key:"ruj8y"}]],Te=l("book-open",Ce);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const _e=[["path",{d:"M3 3v16a2 2 0 0 0 2 2h16",key:"c24i48"}],["path",{d:"M18 17V9",key:"2bz60n"}],["path",{d:"M13 17V5",key:"1frdt8"}],["path",{d:"M8 17v-3",key:"17ska0"}]],Q=l("chart-column",_e);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ee=[["path",{d:"m9 18 6-6-6-6",key:"mthhwq"}]],W=l("chevron-right",Ee);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Le=[["rect",{width:"18",height:"18",x:"3",y:"3",rx:"2",key:"afitv7"}],["path",{d:"M12 3v18",key:"108xh3"}]],B=l("columns-2",Le);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const ze=[["rect",{width:"14",height:"14",x:"8",y:"8",rx:"2",ry:"2",key:"17jyea"}],["path",{d:"M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2",key:"zix9uf"}]],Fe=l("copy",ze);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ie=[["ellipse",{cx:"12",cy:"5",rx:"9",ry:"3",key:"msslwz"}],["path",{d:"M3 5V19A9 3 0 0 0 21 19V5",key:"1wlel7"}],["path",{d:"M3 12A9 3 0 0 0 21 12",key:"mv7ke4"}]],L=l("database",Ie);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Oe=[["circle",{cx:"12",cy:"12",r:"1",key:"41hilf"}],["circle",{cx:"19",cy:"12",r:"1",key:"1wjl8i"}],["circle",{cx:"5",cy:"12",r:"1",key:"1pcz8c"}]],Ae=l("ellipsis",Oe);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const qe=[["path",{d:"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z",key:"1oefj6"}],["path",{d:"M14 2v5a1 1 0 0 0 1 1h5",key:"wfsgrz"}],["path",{d:"M10 12.5 8 15l2 2.5",key:"1tg20x"}],["path",{d:"m14 12.5 2 2.5-2 2.5",key:"yinavb"}]],_=l("file-code",qe);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const He=[["path",{d:"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z",key:"1oefj6"}],["path",{d:"M14 2v5a1 1 0 0 0 1 1h5",key:"wfsgrz"}],["path",{d:"M10 9H8",key:"b1mrlr"}],["path",{d:"M16 13H8",key:"t4e002"}],["path",{d:"M16 17H8",key:"z1uh3a"}]],De=l("file-text",He);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Pe=[["path",{d:"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z",key:"1oefj6"}],["path",{d:"M14 2v5a1 1 0 0 0 1 1h5",key:"wfsgrz"}]],G=l("file",Pe);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Re=[["path",{d:"m6 14 1.5-2.9A2 2 0 0 1 9.24 10H20a2 2 0 0 1 1.94 2.5l-1.54 6a2 2 0 0 1-1.95 1.5H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h3.9a2 2 0 0 1 1.69.9l.81 1.2a2 2 0 0 0 1.67.9H18a2 2 0 0 1 2 2v2",key:"usdka0"}]],U=l("folder-open",Re);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ve=[["path",{d:"M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z",key:"1kt360"}]],J=l("folder",Ve);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const We=[["path",{d:"M15 6a9 9 0 0 0-9 9V3",key:"1cii5b"}],["circle",{cx:"18",cy:"6",r:"3",key:"1h7g24"}],["circle",{cx:"6",cy:"18",r:"3",key:"fqmcym"}]],X=l("git-branch",We);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Be=[["circle",{cx:"18",cy:"18",r:"3",key:"1xkwt0"}],["circle",{cx:"6",cy:"6",r:"3",key:"1lh9wr"}],["path",{d:"M13 6h3a2 2 0 0 1 2 2v7",key:"1yeb86"}],["line",{x1:"6",x2:"6",y1:"9",y2:"21",key:"rroup"}]],Y=l("git-pull-request",Be);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ge=[["path",{d:"M10 16h.01",key:"1bzywj"}],["path",{d:"M2.212 11.577a2 2 0 0 0-.212.896V18a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-5.527a2 2 0 0 0-.212-.896L18.55 5.11A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z",key:"18tbho"}],["path",{d:"M21.946 12.013H2.054",key:"zqlbp7"}],["path",{d:"M6 16h.01",key:"1pmjb7"}]],Ue=l("hard-drive",Ge);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Je=[["path",{d:"M15 21v-8a1 1 0 0 0-1-1h-4a1 1 0 0 0-1 1v8",key:"5wwlr5"}],["path",{d:"M3 10a2 2 0 0 1 .709-1.528l7-6a2 2 0 0 1 2.582 0l7 6A2 2 0 0 1 21 10v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z",key:"r6nss1"}]],Ze=l("house",Je);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ke=[["rect",{width:"18",height:"18",x:"3",y:"3",rx:"2",ry:"2",key:"1m3agn"}],["circle",{cx:"9",cy:"9",r:"2",key:"af1f0g"}],["path",{d:"m21 15-3.086-3.086a2 2 0 0 0-2.828 0L6 21",key:"1xmnt7"}]],Qe=l("image",Ke);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Xe=[["path",{d:"M3 5h.01",key:"18ugdj"}],["path",{d:"M3 12h.01",key:"nlz23k"}],["path",{d:"M3 19h.01",key:"noohij"}],["path",{d:"M8 5h13",key:"1pao27"}],["path",{d:"M8 12h13",key:"1za7za"}],["path",{d:"M8 19h13",key:"m83p4d"}]],Ye=l("list",Xe);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const et=[["path",{d:"M15 3h6v6",key:"1q9fwt"}],["path",{d:"m21 3-7 7",key:"1l2asr"}],["path",{d:"m3 21 7-7",key:"tjx5ai"}],["path",{d:"M9 21H3v-6",key:"wtvkvv"}]],tt=l("maximize-2",et);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const at=[["path",{d:"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z",key:"10ikf1"}]],st=l("play",at);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const nt=[["path",{d:"m21 21-4.34-4.34",key:"14j7rj"}],["circle",{cx:"11",cy:"11",r:"8",key:"4ej97u"}]],rt=l("search",nt);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const ot=[["rect",{width:"20",height:"8",x:"2",y:"2",rx:"2",ry:"2",key:"ngkwjq"}],["rect",{width:"20",height:"8",x:"2",y:"14",rx:"2",ry:"2",key:"iecqi9"}],["line",{x1:"6",x2:"6.01",y1:"6",y2:"6",key:"16zg32"}],["line",{x1:"6",x2:"6.01",y1:"18",y2:"18",key:"nzw8ys"}]],A=l("server",ot);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const it=[["path",{d:"M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915",key:"1i5ecw"}],["circle",{cx:"12",cy:"12",r:"3",key:"1v7zrd"}]],ct=l("settings",it);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const lt=[["path",{d:"M12 3v18",key:"108xh3"}],["rect",{width:"18",height:"18",x:"3",y:"3",rx:"2",key:"afitv7"}],["path",{d:"M3 9h18",key:"1pudct"}],["path",{d:"M3 15h18",key:"5xshup"}]],dt=l("table",lt);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const pt=[["path",{d:"M12 19h8",key:"baeox8"}],["path",{d:"m4 17 6-6-6-6",key:"1yngyt"}]],ee=l("terminal",pt);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const ht=[["path",{d:"M12 4v16",key:"1654pz"}],["path",{d:"M4 7V5a1 1 0 0 1 1-1h14a1 1 0 0 1 1 1v2",key:"e0r10z"}],["path",{d:"M9 20h6",key:"s66wpe"}]],ut=l("type",ht);function te(r){var c,d,p="";if(typeof r=="string"||typeof r=="number")p+=r;else if(typeof r=="object")if(Array.isArray(r)){var g=r.length;for(c=0;c<g;c++)r[c]&&(d=te(r[c]))&&(p&&(p+=" "),p+=d)}else for(d in r)r[d]&&(p&&(p+=" "),p+=d);return p}function E(){for(var r,c,d=0,p="",g=arguments.length;d<g;d++)(r=arguments[d])&&(c=te(r))&&(p&&(p+=" "),p+=c);return p}const xt=[{name:"Home",icon:Ze,path:"/"},{name:"Database",icon:L,path:"/data"},{name:"Lineage",icon:X,path:"/lineage"},{name:"Visualize",icon:Q,path:"/visualize"},{name:"Storage",icon:Ue,path:"/storage"},{name:"Workflows",icon:K,path:"/workflows"},{name:"Notebooks",icon:Te,path:"/notebooks"},{name:"CI/CD",icon:Y,path:"/cicd"},{name:"Agent",icon:ee,path:"/agent"},{name:"Compute",icon:A,path:"/compute"}],yt=()=>{const{pathname:r}=be(),c=fe(),[d]=we(),p=d.get("file")||null,[g,b]=h.useState(!0);h.useEffect(()=>{const e=()=>b(a=>!a);return window.addEventListener("openclaw:toggle-sidebar",e),()=>window.removeEventListener("openclaw:toggle-sidebar",e)},[]);const[y,v]=h.useState("trino"),[N,$]=h.useState([]),[M,q]=h.useState([]),[ae,S]=h.useState(new Set),[z,C]=h.useState(new Set),[k,F]=h.useState(null),se=h.useRef(null),[I,ne]=h.useState([]),[re,H]=h.useState(!1),[oe,ie]=h.useState(new Set(["."])),ce=(e,a)=>a?["py"].includes(a)||["ts","tsx","js","jsx"].includes(a)?_:["sql"].includes(a)?L:["md","txt","csv"].includes(a)?De:["yml","yaml","json","toml"].includes(a)?_:["sh","bash"].includes(a)?ee:["png","jpg","jpeg","gif","svg","webp"].includes(a)?Qe:["html","css"].includes(a)?_:["dockerfile"].includes(e.toLowerCase())?A:G:G;h.useEffect(()=>{r==="/"&&le()},[r]);async function le(){if(!(I.length>0)){H(!0);try{const a=await(await fetch("/api/files?maxDepth=4")).json();ne(a.tree||[])}catch(e){console.error("Failed to fetch file tree",e)}finally{H(!1)}}}const de=e=>{ie(a=>{const s=new Set(a);return s.has(e)?s.delete(e):s.add(e),s})},D=(e,a=0)=>e.map((s,o,n)=>{const i=s.type==="directory",u=oe.has(s.path),x=i?u?U:J:ce(s.name,s.extension),f=m=>m?m<1024?`${m} B`:m<1024*1024?`${(m/1024).toFixed(1)} KB`:`${(m/(1024*1024)).toFixed(1)} MB`:"";return t.jsxs("div",{className:"relative",children:[a>0&&Array.from({length:a}).map((m,j)=>t.jsx("div",{className:"absolute w-[1px] bg-obsidian-border h-full",style:{left:`${j*12+11}px`}},j)),t.jsxs("div",{className:E("flex items-center gap-1.5 cursor-pointer hover:bg-obsidian-panel-hover text-obsidian-muted select-none text-[13px] h-[22px]",!i&&p===s.path&&"bg-gradient-to-r from-obsidian-info/20 to-transparent text-white shadow-[inset_2px_0_0_0_#3794ff]",!i&&p!==s.path&&"text-obsidian-muted"),onClick:()=>{i?de(s.path):c(`/?file=${encodeURIComponent(s.path)}`)},style:{paddingLeft:`${a*12+8}px`},children:[t.jsxs("div",{className:"w-4 h-4 flex items-center justify-center flex-shrink-0",children:[i&&t.jsx(W,{className:`w-3.5 h-3.5 text-obsidian-muted transition-transform ${u?"rotate-90":""}`}),!i&&t.jsx("div",{className:"w-3.5"})]}),t.jsx(x,{className:`w-4 h-4 flex-shrink-0 ${i?"text-[#6895a8]":s.extension==="py"?"text-[#4e94c0]":["ts","tsx"].includes(s.extension)?"text-[#4b8ec2]":s.extension==="sql"?"text-[#82aaff]":["yml","yaml"].includes(s.extension)?"text-[#8a6ea0]":s.extension==="md"?"text-[#6ea8b0]":["sh","bash"].includes(s.extension)?"text-[#6ea870]":"text-obsidian-muted"}`}),t.jsx("span",{className:"truncate",children:s.name}),!i&&s.size&&t.jsx("span",{className:"ml-auto text-[10px] text-obsidian-muted opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap pr-2",children:f(s.size)})]}),i&&u&&s.children&&t.jsx("div",{children:D(s.children,a+1)})]},s.path)});h.useEffect(()=>{r==="/data"&&y==="trino"&&P()},[r,y]);async function T(e){try{return(await(await fetch("/api/trino",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({query:e})})).json()).data||[]}catch(a){return console.error(a),[]}}async function P(){if(N.length>0)return;const a=(await T("SHOW CATALOGS")).map(s=>({id:s.Catalog,name:s.Catalog,type:"database",children:[],loaded:!1}));$(a)}async function pe(){if(!(M.length>0))try{const s=((await(await fetch("/api/postgres",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"explore",type:"databases"})})).json()).data||[]).map(o=>({id:`pg:${o.name}`,name:o.name,type:"database",engine:"postgres",children:[],loaded:!1}));q(s)}catch(e){console.error("Failed to fetch PG databases",e)}}async function he(e){const a=new Set(z);if(a.has(e.id)){a.delete(e.id),C(a);return}if(a.add(e.id),C(a),!e.loaded){S(n=>new Set(n).add(e.id));let s=[];try{if(e.type==="database")s=((await(await fetch("/api/postgres",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"explore",type:"schemas",database:e.name})})).json()).data||[]).map(u=>({id:`${e.id}.${u.name}`,name:u.name,type:"schema",engine:"postgres",parentId:e.id,children:[],loaded:!1}));else if(e.type==="schema")s=((await(await fetch("/api/postgres",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"explore",type:"tables",schema:e.name})})).json()).data||[]).map(u=>({id:`${e.id}.${u.name}`,name:u.name,type:"table",engine:"postgres",parentId:e.id,children:[],loaded:!1}));else if(e.type==="table"){const n=e.id.split("."),i=n[n.length-2],u=n[n.length-1];s=((await(await fetch("/api/postgres",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"explore",type:"columns",schema:i,table:u})})).json()).data||[]).map(m=>({id:`${e.id}.${m.name}`,name:m.name,dataType:m.data_type,type:"column",engine:"postgres",parentId:e.id,loaded:!0}))}}catch(n){console.error("Failed to load PG children",n)}const o=n=>n.map(i=>i.id===e.id?{...i,children:s,loaded:!0}:i.children?{...i,children:o(i.children)}:i);q(n=>o(n)),S(n=>{const i=new Set(n);return i.delete(e.id),i})}}h.useEffect(()=>{r==="/data"&&(y==="trino"?P():pe())},[r,y]);async function ue(e){const a=new Set(z);if(a.has(e.id)){a.delete(e.id),C(a);return}if(a.add(e.id),C(a),!e.loaded){S(n=>new Set(n).add(e.id));let s=[];try{if(e.type==="database"){const n=`SHOW SCHEMAS FROM "${e.name}"`;s=(await T(n)).map(u=>({id:`${e.id}.${u.Schema}`,name:u.Schema,type:"schema",parentId:e.id,children:[],loaded:!1}))}else if(e.type==="schema"){const[n,i]=e.id.split("."),u=`SHOW TABLES FROM "${n}"."${i}"`;s=(await T(u)).map(f=>({id:`${e.id}.${f.Table}`,name:f.Table,type:"table",parentId:e.id,children:[],loaded:!1}))}else if(e.type==="table"){const n=e.id.split("."),i=n[0],u=n[1],x=n.slice(2).join("."),f=`SHOW COLUMNS FROM "${i}"."${u}"."${x}"`;s=(await T(f)).map(j=>({id:`${e.id}.${j.Column}`,name:j.Column,dataType:j.Type,type:"column",parentId:e.id,loaded:!0}))}}catch(n){console.error("Failed to load children",n)}const o=n=>n.map(i=>i.id===e.id?{...i,children:s,loaded:!0}:i.children?{...i,children:o(i.children)}:i);$(n=>o(n)),S(n=>{const i=new Set(n);return i.delete(e.id),i})}}const xe=e=>{const a=e.toLowerCase();return a.includes("int")||a.includes("bigint")||a.includes("smallint")||a.includes("tinyint")?{bg:"bg-obsidian-info/15",text:"text-[#5b9bd5]",label:a}:a.includes("double")||a.includes("float")||a.includes("decimal")||a.includes("real")||a.includes("numeric")?{bg:"bg-obsidian-number/15",text:"text-obsidian-number",label:a}:a.includes("varchar")||a.includes("char")||a.includes("text")||a.includes("string")?{bg:"bg-obsidian-string/15",text:"text-obsidian-string",label:a}:a.includes("timestamp")||a.includes("date")||a.includes("time")?{bg:"bg-obsidian-date/15",text:"text-obsidian-date",label:a}:a.includes("bool")?{bg:"bg-obsidian-boolean/15",text:"text-obsidian-boolean",label:a}:a.includes("json")||a.includes("map")||a.includes("array")||a.includes("row")?{bg:"bg-obsidian-object/15",text:"text-obsidian-object",label:a}:a.includes("binary")||a.includes("blob")||a.includes("bytea")||a.includes("varbinary")?{bg:"bg-obsidian-binary/15",text:"text-obsidian-binary",label:a}:{bg:"bg-obsidian-muted/10",text:"text-obsidian-muted",label:a}},R=e=>{navigator.clipboard.writeText(e).catch(()=>{})},ye=(e,a)=>{const s=[];if(e.type==="table"){const o=a?(()=>{const n=e.id.replace(/^pg:/,"").split(".");return n.length>=2?`"${n[n.length-2]}"."${n[n.length-1]}"`:`"${e.name}"`})():(()=>{const n=e.id.split(".");return n.length>=3?`"${n[0]}"."${n[1]}"."${n[2]}"`:`"${e.name}"`})();s.push({label:"SELECT TOP 100",icon:t.jsx(st,{className:"w-3.5 h-3.5 text-[#6aab73]"}),action:()=>{const n=new CustomEvent("openclaw:run-query",{detail:{query:`SELECT * FROM ${o} LIMIT 100`,engine:a?"postgres":"trino"}});window.dispatchEvent(n)}}),s.push({label:"Generate SELECT",icon:t.jsx(Ye,{className:"w-3.5 h-3.5 text-obsidian-info"}),action:()=>{const n=new CustomEvent("openclaw:insert-query",{detail:{query:`SELECT
    *
FROM ${o}
LIMIT 100;`}});window.dispatchEvent(n)}}),s.push({label:"SHOW COLUMNS",icon:t.jsx(B,{className:"w-3.5 h-3.5 text-[#b07cd8]"}),action:()=>{if(!a){const n=new CustomEvent("openclaw:run-query",{detail:{query:`SHOW COLUMNS FROM ${o}`,engine:"trino"}});window.dispatchEvent(n)}}}),s.push({label:"",action:()=>{},separator:!0})}return s.push({label:"Copy Name",icon:t.jsx(Fe,{className:"w-3.5 h-3.5 text-obsidian-muted"}),action:()=>R(e.name)}),e.type==="column"&&e.dataType&&s.push({label:`Copy Type: ${e.dataType}`,icon:t.jsx(ut,{className:"w-3.5 h-3.5 text-obsidian-muted"}),action:()=>R(e.dataType)}),s},me=(e,a,s)=>{e.preventDefault(),e.stopPropagation(),F({x:e.clientX,y:e.clientY,items:ye(a,s)})},ge=(e,a)=>{if(e.type!=="table")return;const s=a?(()=>{const n=e.id.replace(/^pg:/,"").split(".");return n.length>=2?`"${n[n.length-2]}"."${n[n.length-1]}"`:`"${e.name}"`})():(()=>{const n=e.id.split(".");return n.length>=3?`"${n[0]}"."${n[1]}"."${n[2]}"`:`"${e.name}"`})(),o=new CustomEvent("openclaw:run-query",{detail:{query:`SELECT * FROM ${s} LIMIT 100`,engine:a?"postgres":"trino"}});window.dispatchEvent(o)};h.useEffect(()=>{const e=()=>F(null);if(k)return window.addEventListener("click",e),()=>window.removeEventListener("click",e)},[k]);const O=(e,a=0,s=!1)=>e.map(o=>{const n=z.has(o.id),i=ae.has(o.id),u=o.type==="database"?L:o.type==="schema"?J:o.type==="table"?dt:B;return t.jsxs("div",{children:[t.jsxs("div",{className:"flex items-center gap-1.5 py-[2px] cursor-pointer select-none text-[12px] group transition-colors rounded-sm",style:{color:"rgba(255,255,255,0.55)",paddingLeft:a>0?`${a*12+8}px`:"8px"},onMouseEnter:x=>x.currentTarget.style.background="rgba(255,255,255,0.04)",onMouseLeave:x=>x.currentTarget.style.background="transparent",onClick:()=>s?he(o):ue(o),onDoubleClick:()=>ge(o,s),onContextMenu:x=>me(x,o,s),children:[t.jsx("div",{className:"w-4 h-4 flex items-center justify-center flex-shrink-0",children:o.children&&o.children.length>0?t.jsx(W,{className:E("w-3 h-3 transition-transform",n&&"rotate-90"),style:{color:"rgba(255,255,255,0.3)"}}):t.jsx("div",{className:"w-4"})}),t.jsx(u,{className:"w-3.5 h-3.5 flex-shrink-0 transition-colors",style:{color:o.type==="database"?s?"#22d3ee":"#818cf8":o.type==="schema"?"rgba(255,255,255,0.35)":o.type==="table"?"#60a5fa":"rgba(255,255,255,0.25)"}}),t.jsx("span",{className:E("truncate",o.type==="column"?"opacity-70":""),children:o.name}),o.dataType&&(()=>{const x=xe(o.dataType);return t.jsx("span",{className:E("ml-auto text-[9px] px-1.5 py-[1px] rounded-[3px] font-medium font-mono tracking-wide transition-opacity",x.bg,x.text),children:x.label})})(),i&&t.jsx("span",{className:"ml-auto text-[9px] text-obsidian-muted animate-pulse",children:"..."})]}),n&&o.children&&t.jsx("div",{children:O(o.children,a+1,s)})]},o.id)});return t.jsxs("aside",{className:"h-full shrink-0 z-40 relative flex select-none bg-black/20 backdrop-blur-xl",children:[t.jsxs("div",{className:"w-[56px] flex flex-col items-center py-3 shrink-0 bg-transparent backdrop-blur-md z-10",style:{borderRight:"1px solid rgba(255,255,255,0.02)"},children:[xt.map(e=>{const a=e.icon,s=r===e.path||e.path!=="/"&&r.startsWith(e.path);return t.jsx(ke,{to:e.path,title:e.name,children:t.jsxs("div",{className:"w-[44px] h-[44px] flex items-center justify-center rounded-xl mb-1.5 cursor-pointer transition-all duration-200 relative group",style:{background:s?"rgba(255,255,255,0.06)":"transparent",boxShadow:s?"inset 0 1px 1px rgba(255,255,255,0.05), inset 0 -1px 1px rgba(0,0,0,0.2)":"none"},onMouseEnter:o=>{s||(o.currentTarget.style.background="rgba(255,255,255,0.08)")},onMouseLeave:o=>{s||(o.currentTarget.style.background="transparent")},children:[t.jsx(a,{className:"transition-colors",style:{width:22,height:22,color:s?"rgba(255,255,255,0.9)":"rgba(255,255,255,0.45)",strokeWidth:s?1.5:1.25}}),s&&t.jsx("div",{className:"absolute left-[1px] top-[10px] bottom-[10px] w-[1px] rounded-full",style:{background:"linear-gradient(180deg, rgba(255,255,255,0.0), rgba(255,255,255,0.4), rgba(255,255,255,0.0))"}}),t.jsx("div",{className:"absolute left-full ml-3 px-2.5 py-1 rounded-md text-[11px] whitespace-nowrap pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity z-50 font-medium active:scale-95",style:{background:"#18181c",border:"1px solid rgba(255,255,255,0.08)",color:"rgba(255,255,255,0.8)",boxShadow:"0 4px 12px rgba(0,0,0,0.4)"},children:e.name})]})},e.path)}),t.jsx("div",{className:"mt-auto pb-4",children:t.jsxs("div",{className:"w-[44px] h-[44px] flex items-center justify-center rounded-xl cursor-pointer transition-all duration-200 group relative",onMouseEnter:e=>{e.currentTarget.style.background="rgba(255,255,255,0.08)"},onMouseLeave:e=>{e.currentTarget.style.background="transparent"},children:[t.jsx(ct,{style:{width:22,height:22,color:"rgba(255,255,255,0.45)",strokeWidth:1.25},className:"group-hover:rotate-45 transition-transform duration-300"}),t.jsx("div",{className:"absolute left-full ml-3 px-2.5 py-1 rounded-md text-[11px] whitespace-nowrap pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity z-50 font-medium active:scale-95",style:{background:"#18181c",border:"1px solid rgba(255,255,255,0.08)",color:"rgba(255,255,255,0.8)",boxShadow:"0 4px 12px rgba(0,0,0,0.4)"},children:"Settings"})]})})]}),r!=="/data"&&r!=="/notebooks"&&r!=="/cicd"&&g&&t.jsxs("div",{className:"w-[260px] flex flex-col h-full text-[12px] font-sans select-none bg-transparent",children:[t.jsxs("div",{className:"h-10 flex items-center px-4 justify-between shrink-0 border-b border-white/5",children:[t.jsxs("div",{className:"flex items-center gap-2",children:[r==="/"?t.jsx(U,{className:"w-3.5 h-3.5 text-sky-400 opacity-80"}):r==="/workflows"?t.jsx(K,{className:"w-3.5 h-3.5 text-sky-400 opacity-80"}):r==="/lineage"?t.jsx(X,{className:"w-3.5 h-3.5 text-sky-400 opacity-80"}):r==="/visualize"?t.jsx(Q,{className:"w-3.5 h-3.5 text-sky-400 opacity-80"}):r==="/compute"?t.jsx(A,{className:"w-3.5 h-3.5 text-sky-400 opacity-80"}):r==="/cicd"?t.jsx(Y,{className:"w-3.5 h-3.5 text-sky-400 opacity-80"}):t.jsx(L,{className:"w-3.5 h-3.5 text-sky-400 opacity-80"}),t.jsx("span",{className:"text-xs font-semibold text-white/90 tracking-wide",children:r==="/"||r==="/data"?"Explorer":r==="/workflows"?"Structure":r==="/lineage"?"Lineage":r==="/visualize"?"Dashboards":r==="/compute"?"Infrastructure":"Explorer"})]}),t.jsxs("div",{className:"flex gap-1 shrink-0",children:[t.jsx("button",{className:"p-1.5 hover:bg-white/10 rounded-md text-white/40 hover:text-white/80 transition-all active:scale-95",title:"Search",children:t.jsx(rt,{className:"w-3.5 h-3.5"})}),t.jsx("button",{className:"p-1.5 hover:bg-white/10 rounded-md text-white/40 hover:text-white/80 transition-all active:scale-95",title:"Expand",children:t.jsx(tt,{className:"w-3.5 h-3.5"})})]})]}),t.jsx("div",{className:"flex-1 overflow-y-auto p-1 py-2 custom-scrollbar",children:r==="/data"?t.jsxs(t.Fragment,{children:[t.jsxs("div",{className:"flex items-center gap-1 px-2 mb-3",children:[t.jsx("button",{onClick:()=>v("trino"),className:"flex-1 px-2 py-1 rounded-md text-[10px] font-medium transition-all",style:{background:y==="trino"?"rgba(99,102,241,0.15)":"transparent",color:y==="trino"?"#818cf8":"rgba(255,255,255,0.25)",border:y==="trino"?"1px solid rgba(99,102,241,0.3)":"1px solid transparent"},children:"Trino"}),t.jsx("button",{onClick:()=>v("postgres"),className:"flex-1 px-2 py-1 rounded-md text-[10px] font-medium transition-all",style:{background:y==="postgres"?"rgba(34,211,238,0.12)":"transparent",color:y==="postgres"?"#22d3ee":"rgba(255,255,255,0.25)",border:y==="postgres"?"1px solid rgba(34,211,238,0.25)":"1px solid transparent"},children:"PostgreSQL"})]}),y==="trino"?t.jsxs(t.Fragment,{children:[N.length===0&&t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px] animate-pulse",children:"Fetching Catalogs..."}),O(N)]}):t.jsxs(t.Fragment,{children:[M.length===0&&t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px] animate-pulse",children:"Fetching Databases..."}),O(M,0,!0)]})]}):r==="/"?t.jsx(t.Fragment,{children:re?t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px] animate-pulse",children:"Loading project structure..."}):I.length===0?t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px]",children:"No files found"}):D(I)}):t.jsx("div",{className:"px-4 py-2 text-obsidian-muted italic",children:r==="/workflows"&&"DAGs Explorer..."})}),t.jsxs("div",{style:{height:210,borderTop:"1px solid rgba(255,255,255,0.05)",flexShrink:0},className:"flex flex-col",children:[t.jsxs("div",{className:"h-7 flex items-center px-3 gap-2 shrink-0",style:{borderBottom:"1px solid rgba(255,255,255,0.05)"},children:[t.jsx("span",{style:{fontSize:10,fontWeight:600,letterSpacing:"0.06em",textTransform:"uppercase",color:"rgba(255,255,255,0.3)"},children:"Services"}),t.jsx("div",{className:"ml-auto",children:t.jsx("div",{className:"btn-icon w-5 h-5",style:{color:"rgba(255,255,255,0.4)"},children:t.jsx(Ae,{style:{width:12,height:12}})})})]}),t.jsxs("div",{className:"flex-1 overflow-y-auto p-2 space-y-0.5",children:[t.jsx(w,{name:"Airflow (API)",status:"running"}),t.jsx(w,{name:"Trino (Coordinator)",status:"running"}),t.jsx(w,{name:"Spark Master",status:"running"}),t.jsx(w,{name:"MinIO (S3)",status:"running"}),t.jsx(w,{name:"Marquez (API)",status:"running"}),t.jsx(w,{name:"Superset",status:"running"}),t.jsx(w,{name:"Gitea",status:"running"})]})]})]}),k&&t.jsx("div",{ref:se,className:"fixed z-[9999] rounded-xl shadow-2xl py-1 min-w-[180px]",style:{left:k.x,top:k.y,background:"#0d0d12",border:"1px solid rgba(255,255,255,0.09)"},onClick:e=>e.stopPropagation(),children:k.items.map((e,a)=>e.separator?t.jsx("div",{className:"h-[1px] my-1",style:{background:"rgba(255,255,255,0.06)"}},a):t.jsxs("button",{className:"w-full px-3 py-1.5 text-left text-[11px] flex items-center gap-2 transition-colors rounded-md mx-1",style:{color:"rgba(255,255,255,0.65)",width:"calc(100% - 8px)"},onMouseEnter:s=>s.currentTarget.style.background="rgba(99,102,241,0.15)",onMouseLeave:s=>s.currentTarget.style.background="transparent",onClick:()=>{e.action(),F(null)},children:[e.icon,e.label]},a))})]})},w=({name:r,status:c})=>t.jsxs("div",{className:"flex items-center py-[3px] px-2 rounded-md cursor-pointer transition-colors group active:scale-95",style:{color:"rgba(255,255,255,0.45)"},onMouseEnter:d=>d.currentTarget.style.background="rgba(255,255,255,0.03)",onMouseLeave:d=>d.currentTarget.style.background="transparent",children:[t.jsx("div",{className:"w-1.5 h-1.5 rounded-full mr-2 flex-shrink-0",style:{background:c==="running"?"#4caf50":"#e57373"}}),t.jsx("span",{style:{fontSize:11},children:r})]}),gt=()=>t.jsx(h.Suspense,{fallback:t.jsx("div",{className:"flex h-full",style:{background:"#09090b"}}),children:t.jsx(yt,{})});export{K as A,Te as B,B as C,L as D,Ae as E,_ as F,Y as G,Ue as H,Qe as I,Ye as L,tt as M,st as P,gt as S,ee as T,ct as a,E as b,l as c,rt as d,dt as e,Fe as f,Q as g,W as h,X as i,A as j,J as k,De as l,G as m,U as n};
