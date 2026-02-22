import{r as h,j as t,c as be,a as we,u as ke,L as je}from"./index-CcXpoZej.js";/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const X=(...r)=>r.filter((c,l,d)=>!!c&&c.trim()!==""&&d.indexOf(c)===l).join(" ").trim();/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const ve=r=>r.replace(/([a-z0-9])([A-Z])/g,"$1-$2").toLowerCase();/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ne=r=>r.replace(/^([A-Z])|[\s-_]+(\w)/g,(c,l,d)=>d?d.toUpperCase():l.toLowerCase());/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const J=r=>{const c=Ne(r);return c.charAt(0).toUpperCase()+c.slice(1)};/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */var Me={xmlns:"http://www.w3.org/2000/svg",width:24,height:24,viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:2,strokeLinecap:"round",strokeLinejoin:"round"};/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const $e=r=>{for(const c in r)if(c.startsWith("aria-")||c==="role"||c==="title")return!0;return!1};/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Se=h.forwardRef(({color:r="currentColor",size:c=24,strokeWidth:l=2,absoluteStrokeWidth:d,className:y="",children:f,iconNode:m,...v},N)=>h.createElement("svg",{ref:N,...Me,width:c,height:c,stroke:r,strokeWidth:d?Number(l)*24/Number(c):l,className:X("lucide",y),...!f&&!$e(v)&&{"aria-hidden":"true"},...v},[...m.map(([$,M])=>h.createElement($,M)),...Array.isArray(f)?f:[f]]));/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const p=(r,c)=>{const l=h.forwardRef(({className:d,...y},f)=>h.createElement(Se,{ref:f,iconNode:c,className:X(`lucide-${ve(J(r))}`,`lucide-${r}`,d),...y}));return l.displayName=J(r),l};/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ce=[["rect",{width:"20",height:"5",x:"2",y:"3",rx:"1",key:"1wp1u1"}],["path",{d:"M4 8v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8",key:"1s80jp"}],["path",{d:"M10 12h4",key:"a56b0p"}]],Te=p("archive",Ce);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const _e=[["path",{d:"M12 7v14",key:"1akyts"}],["path",{d:"M3 18a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h5a4 4 0 0 1 4 4 4 4 0 0 1 4-4h5a1 1 0 0 1 1 1v13a1 1 0 0 1-1 1h-6a3 3 0 0 0-3 3 3 3 0 0 0-3-3z",key:"ruj8y"}]],Ee=p("book-open",_e);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Le=[["path",{d:"M3 3v16a2 2 0 0 0 2 2h16",key:"c24i48"}],["path",{d:"M18 17V9",key:"2bz60n"}],["path",{d:"M13 17V5",key:"1frdt8"}],["path",{d:"M8 17v-3",key:"17ska0"}]],ze=p("chart-column",Le);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Fe=[["path",{d:"m9 18 6-6-6-6",key:"mthhwq"}]],U=p("chevron-right",Fe);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ie=[["rect",{width:"18",height:"18",x:"3",y:"3",rx:"2",key:"afitv7"}],["path",{d:"M12 3v18",key:"108xh3"}]],Z=p("columns-2",Ie);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ae=[["rect",{width:"14",height:"14",x:"8",y:"8",rx:"2",ry:"2",key:"17jyea"}],["path",{d:"M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2",key:"zix9uf"}]],Oe=p("copy",Ae);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const qe=[["ellipse",{cx:"12",cy:"5",rx:"9",ry:"3",key:"msslwz"}],["path",{d:"M3 5V19A9 3 0 0 0 21 19V5",key:"1wlel7"}],["path",{d:"M3 12A9 3 0 0 0 21 12",key:"mv7ke4"}]],q=p("database",qe);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const He=[["circle",{cx:"12",cy:"12",r:"1",key:"41hilf"}],["circle",{cx:"19",cy:"12",r:"1",key:"1wjl8i"}],["circle",{cx:"5",cy:"12",r:"1",key:"1pcz8c"}]],Re=p("ellipsis",He);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const De=[["path",{d:"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z",key:"1oefj6"}],["path",{d:"M14 2v5a1 1 0 0 0 1 1h5",key:"wfsgrz"}],["path",{d:"M10 12.5 8 15l2 2.5",key:"1tg20x"}],["path",{d:"m14 12.5 2 2.5-2 2.5",key:"yinavb"}]],_=p("file-code",De);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Pe=[["path",{d:"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z",key:"1oefj6"}],["path",{d:"M14 2v5a1 1 0 0 0 1 1h5",key:"wfsgrz"}],["path",{d:"M10 9H8",key:"b1mrlr"}],["path",{d:"M16 13H8",key:"t4e002"}],["path",{d:"M16 17H8",key:"z1uh3a"}]],Ve=p("file-text",Pe);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const We=[["path",{d:"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z",key:"1oefj6"}],["path",{d:"M14 2v5a1 1 0 0 0 1 1h5",key:"wfsgrz"}]],K=p("file",We);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ge=[["path",{d:"m6 14 1.5-2.9A2 2 0 0 1 9.24 10H20a2 2 0 0 1 1.94 2.5l-1.54 6a2 2 0 0 1-1.95 1.5H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h3.9a2 2 0 0 1 1.69.9l.81 1.2a2 2 0 0 0 1.67.9H18a2 2 0 0 1 2 2v2",key:"usdka0"}]],Be=p("folder-open",Ge);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Je=[["path",{d:"M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z",key:"1kt360"}]],Q=p("folder",Je);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ue=[["path",{d:"M15 6a9 9 0 0 0-9 9V3",key:"1cii5b"}],["circle",{cx:"18",cy:"6",r:"3",key:"1h7g24"}],["circle",{cx:"6",cy:"18",r:"3",key:"fqmcym"}]],Ze=p("git-branch",Ue);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ke=[["circle",{cx:"18",cy:"18",r:"3",key:"1xkwt0"}],["circle",{cx:"6",cy:"6",r:"3",key:"1lh9wr"}],["path",{d:"M13 6h3a2 2 0 0 1 2 2v7",key:"1yeb86"}],["line",{x1:"6",x2:"6",y1:"9",y2:"21",key:"rroup"}]],Y=p("git-pull-request",Ke);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Qe=[["path",{d:"M10 16h.01",key:"1bzywj"}],["path",{d:"M2.212 11.577a2 2 0 0 0-.212.896V18a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-5.527a2 2 0 0 0-.212-.896L18.55 5.11A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z",key:"18tbho"}],["path",{d:"M21.946 12.013H2.054",key:"zqlbp7"}],["path",{d:"M6 16h.01",key:"1pmjb7"}]],Xe=p("hard-drive",Qe);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const Ye=[["path",{d:"M15 21v-8a1 1 0 0 0-1-1h-4a1 1 0 0 0-1 1v8",key:"5wwlr5"}],["path",{d:"M3 10a2 2 0 0 1 .709-1.528l7-6a2 2 0 0 1 2.582 0l7 6A2 2 0 0 1 21 10v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z",key:"r6nss1"}]],et=p("house",Ye);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const tt=[["rect",{width:"18",height:"18",x:"3",y:"3",rx:"2",ry:"2",key:"1m3agn"}],["circle",{cx:"9",cy:"9",r:"2",key:"af1f0g"}],["path",{d:"m21 15-3.086-3.086a2 2 0 0 0-2.828 0L6 21",key:"1xmnt7"}]],at=p("image",tt);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const st=[["path",{d:"M3 5h.01",key:"18ugdj"}],["path",{d:"M3 12h.01",key:"nlz23k"}],["path",{d:"M3 19h.01",key:"noohij"}],["path",{d:"M8 5h13",key:"1pao27"}],["path",{d:"M8 12h13",key:"1za7za"}],["path",{d:"M8 19h13",key:"m83p4d"}]],nt=p("list",st);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const rt=[["path",{d:"M15 3h6v6",key:"1q9fwt"}],["path",{d:"m21 3-7 7",key:"1l2asr"}],["path",{d:"m3 21 7-7",key:"tjx5ai"}],["path",{d:"M9 21H3v-6",key:"wtvkvv"}]],ot=p("maximize-2",rt);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const it=[["path",{d:"M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z",key:"10ikf1"}]],ct=p("play",it);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const lt=[["path",{d:"m21 21-4.34-4.34",key:"14j7rj"}],["circle",{cx:"11",cy:"11",r:"8",key:"4ej97u"}]],dt=p("search",lt);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const pt=[["rect",{width:"20",height:"8",x:"2",y:"2",rx:"2",ry:"2",key:"ngkwjq"}],["rect",{width:"20",height:"8",x:"2",y:"14",rx:"2",ry:"2",key:"iecqi9"}],["line",{x1:"6",x2:"6.01",y1:"6",y2:"6",key:"16zg32"}],["line",{x1:"6",x2:"6.01",y1:"18",y2:"18",key:"nzw8ys"}]],ee=p("server",pt);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const ht=[["path",{d:"M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915",key:"1i5ecw"}],["circle",{cx:"12",cy:"12",r:"3",key:"1v7zrd"}]],ut=p("settings",ht);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const xt=[["path",{d:"M12 3v18",key:"108xh3"}],["rect",{width:"18",height:"18",x:"3",y:"3",rx:"2",key:"afitv7"}],["path",{d:"M3 9h18",key:"1pudct"}],["path",{d:"M3 15h18",key:"5xshup"}]],yt=p("table",xt);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const mt=[["path",{d:"M12 19h8",key:"baeox8"}],["path",{d:"m4 17 6-6-6-6",key:"1yngyt"}]],H=p("terminal",mt);/**
 * @license lucide-react v0.564.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const gt=[["path",{d:"M12 4v16",key:"1654pz"}],["path",{d:"M4 7V5a1 1 0 0 1 1-1h14a1 1 0 0 1 1 1v2",key:"e0r10z"}],["path",{d:"M9 20h6",key:"s66wpe"}]],ft=p("type",gt);function te(r){var c,l,d="";if(typeof r=="string"||typeof r=="number")d+=r;else if(typeof r=="object")if(Array.isArray(r)){var y=r.length;for(c=0;c<y;c++)r[c]&&(l=te(r[c]))&&(d&&(d+=" "),d+=l)}else for(l in r)r[l]&&(d&&(d+=" "),d+=l);return d}function E(){for(var r,c,l=0,d="",y=arguments.length;l<y;l++)(r=arguments[l])&&(c=te(r))&&(d&&(d+=" "),d+=c);return d}async function bt(r,c=1e4){const l=new AbortController,d=window.setTimeout(()=>l.abort(),c);try{const y=await fetch(r,{signal:l.signal});if(!y.ok)throw new Error(`${y.status} ${y.statusText}`);return await y.json()}finally{window.clearTimeout(d)}}const wt=[{name:"Home",icon:et,path:"/"},{name:"Database",icon:q,path:"/data"},{name:"Lineage",icon:Ze,path:"/lineage"},{name:"Visualize",icon:ze,path:"/visualize"},{name:"Storage",icon:Xe,path:"/storage"},{name:"Workflows",icon:Te,path:"/workflows"},{name:"Notebooks",icon:Ee,path:"/notebooks"},{name:"CI/CD",icon:Y,path:"/cicd"},{name:"Agent",icon:H,path:"/agent"},{name:"Compute",icon:ee,path:"/compute"},{name:"Docker CLI",icon:H,path:"/docker-cli"}],kt=()=>{const{pathname:r}=be(),c=we(),[l]=ke(),d=l.get("file")||null,[y,f]=h.useState(!0);h.useEffect(()=>{const e=()=>f(a=>!a);return window.addEventListener("openclaw:toggle-sidebar",e),()=>window.removeEventListener("openclaw:toggle-sidebar",e)},[]);const[m,v]=h.useState("trino"),[N,$]=h.useState([]),[M,R]=h.useState([]),[ae,S]=h.useState(new Set),[L,C]=h.useState(new Set),[k,z]=h.useState(null),se=h.useRef(null),[D,P]=h.useState([]),[F,I]=h.useState(!1),[A,ne]=h.useState([]),[re,V]=h.useState(!1),[oe,ie]=h.useState(new Set(["."])),ce=(e,a)=>a?["py"].includes(a)||["ts","tsx","js","jsx"].includes(a)?_:["sql"].includes(a)?q:["md","txt","csv"].includes(a)?Ve:["yml","yaml","json","toml"].includes(a)?_:["sh","bash"].includes(a)?H:["png","jpg","jpeg","gif","svg","webp"].includes(a)?at:["html","css"].includes(a)?_:["dockerfile"].includes(e.toLowerCase())?ee:K:K;h.useEffect(()=>{r==="/cicd"&&le()},[r]),h.useEffect(()=>{if(r!=="/cicd"||!F)return;const e=window.setTimeout(()=>I(!1),12e3);return()=>window.clearTimeout(e)},[r,F]);async function le(){I(!0);try{const e=await bt("/api/gitea/repos?limit=50",1e4);P(e.repos||[])}catch(e){console.error("Failed to fetch Gitea repos",e),P([])}finally{I(!1)}}h.useEffect(()=>{r==="/"&&de()},[r]);async function de(){if(!(A.length>0)){V(!0);try{const a=await(await fetch("/api/files?maxDepth=4")).json();ne(a.tree||[])}catch(e){console.error("Failed to fetch file tree",e)}finally{V(!1)}}}const pe=e=>{ie(a=>{const s=new Set(a);return s.has(e)?s.delete(e):s.add(e),s})},W=(e,a=0)=>e.map((s,o,n)=>{const i=s.type==="directory",u=oe.has(s.path),x=i?u?Be:Q:ce(s.name,s.extension),b=g=>g?g<1024?`${g} B`:g<1024*1024?`${(g/1024).toFixed(1)} KB`:`${(g/(1024*1024)).toFixed(1)} MB`:"";return t.jsxs("div",{className:"relative",children:[a>0&&Array.from({length:a}).map((g,j)=>t.jsx("div",{className:"absolute w-[1px] bg-obsidian-border h-full",style:{left:`${j*12+11}px`}},j)),t.jsxs("div",{className:E("flex items-center gap-1.5 cursor-pointer hover:bg-obsidian-panel-hover text-obsidian-muted select-none text-[13px] h-[22px]",!i&&d===s.path&&"bg-gradient-to-r from-obsidian-info/20 to-transparent text-white shadow-[inset_2px_0_0_0_#3794ff]",!i&&d!==s.path&&"text-obsidian-muted"),onClick:()=>{i?pe(s.path):c(`/?file=${encodeURIComponent(s.path)}`)},style:{paddingLeft:`${a*12+8}px`},children:[t.jsxs("div",{className:"w-4 h-4 flex items-center justify-center flex-shrink-0",children:[i&&t.jsx(U,{className:`w-3.5 h-3.5 text-obsidian-muted transition-transform ${u?"rotate-90":""}`}),!i&&t.jsx("div",{className:"w-3.5"})]}),t.jsx(x,{className:`w-4 h-4 flex-shrink-0 ${i?"text-[#6895a8]":s.extension==="py"?"text-[#4e94c0]":["ts","tsx"].includes(s.extension)?"text-[#4b8ec2]":s.extension==="sql"?"text-[#82aaff]":["yml","yaml"].includes(s.extension)?"text-[#8a6ea0]":s.extension==="md"?"text-[#6ea8b0]":["sh","bash"].includes(s.extension)?"text-[#6ea870]":"text-obsidian-muted"}`}),t.jsx("span",{className:"truncate",children:s.name}),!i&&s.size&&t.jsx("span",{className:"ml-auto text-[10px] text-obsidian-muted opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap pr-2",children:b(s.size)})]}),i&&u&&s.children&&t.jsx("div",{children:W(s.children,a+1)})]},s.path)});h.useEffect(()=>{r==="/data"&&m==="trino"&&G()},[r,m]);async function T(e){try{return(await(await fetch("/api/trino",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({query:e})})).json()).data||[]}catch(a){return console.error(a),[]}}async function G(){if(N.length>0)return;const a=(await T("SHOW CATALOGS")).map(s=>({id:s.Catalog,name:s.Catalog,type:"database",children:[],loaded:!1}));$(a)}async function he(){if(!(M.length>0))try{const s=((await(await fetch("/api/postgres",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"explore",type:"databases"})})).json()).data||[]).map(o=>({id:`pg:${o.name}`,name:o.name,type:"database",engine:"postgres",children:[],loaded:!1}));R(s)}catch(e){console.error("Failed to fetch PG databases",e)}}async function ue(e){const a=new Set(L);if(a.has(e.id)){a.delete(e.id),C(a);return}if(a.add(e.id),C(a),!e.loaded){S(n=>new Set(n).add(e.id));let s=[];try{if(e.type==="database")s=((await(await fetch("/api/postgres",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"explore",type:"schemas",database:e.name})})).json()).data||[]).map(u=>({id:`${e.id}.${u.name}`,name:u.name,type:"schema",engine:"postgres",parentId:e.id,children:[],loaded:!1}));else if(e.type==="schema")s=((await(await fetch("/api/postgres",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"explore",type:"tables",schema:e.name})})).json()).data||[]).map(u=>({id:`${e.id}.${u.name}`,name:u.name,type:"table",engine:"postgres",parentId:e.id,children:[],loaded:!1}));else if(e.type==="table"){const n=e.id.split("."),i=n[n.length-2],u=n[n.length-1];s=((await(await fetch("/api/postgres",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action:"explore",type:"columns",schema:i,table:u})})).json()).data||[]).map(g=>({id:`${e.id}.${g.name}`,name:g.name,dataType:g.data_type,type:"column",engine:"postgres",parentId:e.id,loaded:!0}))}}catch(n){console.error("Failed to load PG children",n)}const o=n=>n.map(i=>i.id===e.id?{...i,children:s,loaded:!0}:i.children?{...i,children:o(i.children)}:i);R(n=>o(n)),S(n=>{const i=new Set(n);return i.delete(e.id),i})}}h.useEffect(()=>{r==="/data"&&(m==="trino"?G():he())},[r,m]);async function xe(e){const a=new Set(L);if(a.has(e.id)){a.delete(e.id),C(a);return}if(a.add(e.id),C(a),!e.loaded){S(n=>new Set(n).add(e.id));let s=[];try{if(e.type==="database"){const n=`SHOW SCHEMAS FROM "${e.name}"`;s=(await T(n)).map(u=>({id:`${e.id}.${u.Schema}`,name:u.Schema,type:"schema",parentId:e.id,children:[],loaded:!1}))}else if(e.type==="schema"){const[n,i]=e.id.split("."),u=`SHOW TABLES FROM "${n}"."${i}"`;s=(await T(u)).map(b=>({id:`${e.id}.${b.Table}`,name:b.Table,type:"table",parentId:e.id,children:[],loaded:!1}))}else if(e.type==="table"){const n=e.id.split("."),i=n[0],u=n[1],x=n.slice(2).join("."),b=`SHOW COLUMNS FROM "${i}"."${u}"."${x}"`;s=(await T(b)).map(j=>({id:`${e.id}.${j.Column}`,name:j.Column,dataType:j.Type,type:"column",parentId:e.id,loaded:!0}))}}catch(n){console.error("Failed to load children",n)}const o=n=>n.map(i=>i.id===e.id?{...i,children:s,loaded:!0}:i.children?{...i,children:o(i.children)}:i);$(n=>o(n)),S(n=>{const i=new Set(n);return i.delete(e.id),i})}}const ye=e=>{const a=e.toLowerCase();return a.includes("int")||a.includes("bigint")||a.includes("smallint")||a.includes("tinyint")?{bg:"bg-obsidian-info/15",text:"text-[#5b9bd5]",label:a}:a.includes("double")||a.includes("float")||a.includes("decimal")||a.includes("real")||a.includes("numeric")?{bg:"bg-obsidian-number/15",text:"text-obsidian-number",label:a}:a.includes("varchar")||a.includes("char")||a.includes("text")||a.includes("string")?{bg:"bg-obsidian-string/15",text:"text-obsidian-string",label:a}:a.includes("timestamp")||a.includes("date")||a.includes("time")?{bg:"bg-obsidian-date/15",text:"text-obsidian-date",label:a}:a.includes("bool")?{bg:"bg-obsidian-boolean/15",text:"text-obsidian-boolean",label:a}:a.includes("json")||a.includes("map")||a.includes("array")||a.includes("row")?{bg:"bg-obsidian-object/15",text:"text-obsidian-object",label:a}:a.includes("binary")||a.includes("blob")||a.includes("bytea")||a.includes("varbinary")?{bg:"bg-obsidian-binary/15",text:"text-obsidian-binary",label:a}:{bg:"bg-obsidian-muted/10",text:"text-obsidian-muted",label:a}},B=e=>{navigator.clipboard.writeText(e).catch(()=>{})},me=(e,a)=>{const s=[];if(e.type==="table"){const o=a?(()=>{const n=e.id.replace(/^pg:/,"").split(".");return n.length>=2?`"${n[n.length-2]}"."${n[n.length-1]}"`:`"${e.name}"`})():(()=>{const n=e.id.split(".");return n.length>=3?`"${n[0]}"."${n[1]}"."${n[2]}"`:`"${e.name}"`})();s.push({label:"SELECT TOP 100",icon:t.jsx(ct,{className:"w-3.5 h-3.5 text-[#6aab73]"}),action:()=>{const n=new CustomEvent("openclaw:run-query",{detail:{query:`SELECT * FROM ${o} LIMIT 100`,engine:a?"postgres":"trino"}});window.dispatchEvent(n)}}),s.push({label:"Generate SELECT",icon:t.jsx(nt,{className:"w-3.5 h-3.5 text-obsidian-info"}),action:()=>{const n=new CustomEvent("openclaw:insert-query",{detail:{query:`SELECT
    *
FROM ${o}
LIMIT 100;`}});window.dispatchEvent(n)}}),s.push({label:"SHOW COLUMNS",icon:t.jsx(Z,{className:"w-3.5 h-3.5 text-[#b07cd8]"}),action:()=>{if(!a){const n=new CustomEvent("openclaw:run-query",{detail:{query:`SHOW COLUMNS FROM ${o}`,engine:"trino"}});window.dispatchEvent(n)}}}),s.push({label:"",action:()=>{},separator:!0})}return s.push({label:"Copy Name",icon:t.jsx(Oe,{className:"w-3.5 h-3.5 text-obsidian-muted"}),action:()=>B(e.name)}),e.type==="column"&&e.dataType&&s.push({label:`Copy Type: ${e.dataType}`,icon:t.jsx(ft,{className:"w-3.5 h-3.5 text-obsidian-muted"}),action:()=>B(e.dataType)}),s},ge=(e,a,s)=>{e.preventDefault(),e.stopPropagation(),z({x:e.clientX,y:e.clientY,items:me(a,s)})},fe=(e,a)=>{if(e.type!=="table")return;const s=a?(()=>{const n=e.id.replace(/^pg:/,"").split(".");return n.length>=2?`"${n[n.length-2]}"."${n[n.length-1]}"`:`"${e.name}"`})():(()=>{const n=e.id.split(".");return n.length>=3?`"${n[0]}"."${n[1]}"."${n[2]}"`:`"${e.name}"`})(),o=new CustomEvent("openclaw:run-query",{detail:{query:`SELECT * FROM ${s} LIMIT 100`,engine:a?"postgres":"trino"}});window.dispatchEvent(o)};h.useEffect(()=>{const e=()=>z(null);if(k)return window.addEventListener("click",e),()=>window.removeEventListener("click",e)},[k]);const O=(e,a=0,s=!1)=>e.map(o=>{const n=L.has(o.id),i=ae.has(o.id),u=o.type==="database"?q:o.type==="schema"?Q:o.type==="table"?yt:Z;return t.jsxs("div",{children:[t.jsxs("div",{className:"flex items-center gap-1.5 py-[2px] cursor-pointer select-none text-[12px] group transition-colors rounded-sm",style:{color:"rgba(255,255,255,0.55)",paddingLeft:a>0?`${a*12+8}px`:"8px"},onMouseEnter:x=>x.currentTarget.style.background="rgba(255,255,255,0.04)",onMouseLeave:x=>x.currentTarget.style.background="transparent",onClick:()=>s?ue(o):xe(o),onDoubleClick:()=>fe(o,s),onContextMenu:x=>ge(x,o,s),children:[t.jsx("div",{className:"w-4 h-4 flex items-center justify-center flex-shrink-0",children:o.children&&o.children.length>0?t.jsx(U,{className:E("w-3 h-3 transition-transform",n&&"rotate-90"),style:{color:"rgba(255,255,255,0.3)"}}):t.jsx("div",{className:"w-4"})}),t.jsx(u,{className:"w-3.5 h-3.5 flex-shrink-0 transition-colors",style:{color:o.type==="database"?s?"#22d3ee":"#818cf8":o.type==="schema"?"rgba(255,255,255,0.35)":o.type==="table"?"#60a5fa":"rgba(255,255,255,0.25)"}}),t.jsx("span",{className:E("truncate",o.type==="column"?"opacity-70":""),children:o.name}),o.dataType&&(()=>{const x=ye(o.dataType);return t.jsx("span",{className:E("ml-auto text-[9px] px-1.5 py-[1px] rounded-[3px] font-medium font-mono tracking-wide transition-opacity",x.bg,x.text),children:x.label})})(),i&&t.jsx("span",{className:"ml-auto text-[9px] text-obsidian-muted animate-pulse",children:"..."})]}),n&&o.children&&t.jsx("div",{children:O(o.children,a+1,s)})]},o.id)});return t.jsxs("aside",{className:"h-full shrink-0 z-40 relative flex select-none bg-black/20 backdrop-blur-xl border-r border-white/5",children:[t.jsxs("div",{className:"w-[52px] flex flex-col items-center py-2 shrink-0 bg-black/40 border-r border-white/5 backdrop-blur-md",children:[wt.map(e=>{const a=e.icon,s=r===e.path||e.path!=="/"&&r.startsWith(e.path);return t.jsx(je,{to:e.path,title:e.name,children:t.jsxs("div",{className:"w-10 h-10 flex items-center justify-center rounded-lg mb-0.5 cursor-pointer transition-all relative group",style:{background:s?"rgba(99,102,241,0.18)":"transparent"},onMouseEnter:o=>{s||(o.currentTarget.style.background="rgba(255,255,255,0.06)")},onMouseLeave:o=>{s||(o.currentTarget.style.background="transparent")},children:[t.jsx(a,{className:"transition-colors",style:{width:20,height:20,color:s?"#a5b4fc":"rgba(255,255,255,0.4)",strokeWidth:1.25}}),s&&t.jsx("div",{className:"absolute left-0 top-[8px] bottom-[8px] w-[2.5px] rounded-r-full",style:{background:"linear-gradient(180deg, #818cf8, #67e8f9)"}}),t.jsx("div",{className:"absolute left-full ml-3 px-2.5 py-1 rounded-md text-[11px] whitespace-nowrap pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity z-50 font-medium active:scale-95",style:{background:"#18181c",border:"1px solid rgba(255,255,255,0.08)",color:"rgba(255,255,255,0.8)",boxShadow:"0 4px 12px rgba(0,0,0,0.4)"},children:e.name})]})},e.path)}),t.jsx("div",{className:"mt-auto pb-2",children:t.jsxs("div",{className:"w-10 h-10 flex items-center justify-center rounded-lg cursor-pointer transition-all group relative",onMouseEnter:e=>{e.currentTarget.style.background="rgba(255,255,255,0.06)"},onMouseLeave:e=>{e.currentTarget.style.background="transparent"},children:[t.jsx(ut,{style:{width:20,height:20,color:"rgba(255,255,255,0.35)",strokeWidth:1.25}}),t.jsx("div",{className:"absolute left-full ml-3 px-2.5 py-1 rounded-md text-[11px] whitespace-nowrap pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity z-50 font-medium active:scale-95",style:{background:"#18181c",border:"1px solid rgba(255,255,255,0.08)",color:"rgba(255,255,255,0.8)",boxShadow:"0 4px 12px rgba(0,0,0,0.4)"},children:"Settings"})]})})]}),r!=="/data"&&r!=="/notebooks"&&y&&t.jsxs("div",{className:"w-[220px] flex flex-col h-full text-[12px] font-sans select-none bg-transparent",children:[t.jsxs("div",{className:"h-10 flex items-center px-4 justify-between shrink-0 border-b border-white/5",children:[t.jsx("span",{className:"font-semibold tracking-tight",style:{fontSize:11,color:"rgba(255,255,255,0.45)",textTransform:"uppercase",letterSpacing:"0.06em"},children:r==="/data"?"Explorer":r==="/workflows"?"Structure":r==="/lineage"?"Lineage":r==="/visualize"?"Dashboards":r==="/compute"?"Infrastructure":r==="/cicd"?"Repositories":"Explorer"}),t.jsxs("div",{className:"flex gap-0.5",children:[t.jsx("div",{className:"btn-icon w-5 h-5",style:{color:"rgba(255,255,255,0.4)"},children:t.jsx(dt,{style:{width:12,height:12}})}),t.jsx("div",{className:"btn-icon w-5 h-5",style:{color:"rgba(255,255,255,0.4)"},children:t.jsx(ot,{style:{width:12,height:12}})})]})]}),t.jsx("div",{className:"flex-1 overflow-y-auto p-1 py-2",children:r==="/data"?t.jsxs(t.Fragment,{children:[t.jsxs("div",{className:"flex items-center gap-1 px-2 mb-3",children:[t.jsx("button",{onClick:()=>v("trino"),className:"flex-1 px-2 py-1 rounded-md text-[10px] font-medium transition-all",style:{background:m==="trino"?"rgba(99,102,241,0.15)":"transparent",color:m==="trino"?"#818cf8":"rgba(255,255,255,0.25)",border:m==="trino"?"1px solid rgba(99,102,241,0.3)":"1px solid transparent"},children:"Trino"}),t.jsx("button",{onClick:()=>v("postgres"),className:"flex-1 px-2 py-1 rounded-md text-[10px] font-medium transition-all",style:{background:m==="postgres"?"rgba(34,211,238,0.12)":"transparent",color:m==="postgres"?"#22d3ee":"rgba(255,255,255,0.25)",border:m==="postgres"?"1px solid rgba(34,211,238,0.25)":"1px solid transparent"},children:"PostgreSQL"})]}),m==="trino"?t.jsxs(t.Fragment,{children:[N.length===0&&t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px] animate-pulse",children:"Fetching Catalogs..."}),O(N)]}):t.jsxs(t.Fragment,{children:[M.length===0&&t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px] animate-pulse",children:"Fetching Databases..."}),O(M,0,!0)]})]}):r==="/cicd"?t.jsx(t.Fragment,{children:F?t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px] animate-pulse",children:"Loading repos..."}):D.length===0?t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px]",children:"No repos found"}):D.map(e=>t.jsxs("div",{className:"flex items-center gap-1.5 py-[2px] px-2 cursor-pointer hover:bg-obsidian-panel-hover text-foreground select-none text-[13px] transition-all active:scale-95",style:{paddingLeft:"8px"},onClick:()=>{window.dispatchEvent(new CustomEvent("openclaw:select-repo",{detail:{repo:e.full_name}}))},children:[t.jsx(Y,{className:"w-4 h-4 flex-shrink-0",style:{color:"#a78bfa"}}),t.jsx("span",{className:"truncate",children:e.name}),e.language&&t.jsx("span",{className:"ml-auto text-[9px] text-obsidian-muted",children:e.language})]},e.id))}):r==="/"?t.jsx(t.Fragment,{children:re?t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px] animate-pulse",children:"Loading project structure..."}):A.length===0?t.jsx("div",{className:"ml-4 text-obsidian-muted text-[10px]",children:"No files found"}):W(A)}):t.jsx("div",{className:"px-4 py-2 text-obsidian-muted italic",children:r==="/workflows"&&"DAGs Explorer..."})}),t.jsxs("div",{style:{height:210,borderTop:"1px solid rgba(255,255,255,0.05)",flexShrink:0},className:"flex flex-col",children:[t.jsxs("div",{className:"h-7 flex items-center px-3 gap-2 shrink-0",style:{borderBottom:"1px solid rgba(255,255,255,0.05)"},children:[t.jsx("span",{style:{fontSize:10,fontWeight:600,letterSpacing:"0.06em",textTransform:"uppercase",color:"rgba(255,255,255,0.3)"},children:"Services"}),t.jsx("div",{className:"ml-auto",children:t.jsx("div",{className:"btn-icon w-5 h-5",style:{color:"rgba(255,255,255,0.4)"},children:t.jsx(Re,{style:{width:12,height:12}})})})]}),t.jsxs("div",{className:"flex-1 overflow-y-auto p-2 space-y-0.5",children:[t.jsx(w,{name:"Airflow (API)",status:"running"}),t.jsx(w,{name:"Trino (Coordinator)",status:"running"}),t.jsx(w,{name:"Spark Master",status:"running"}),t.jsx(w,{name:"MinIO (S3)",status:"running"}),t.jsx(w,{name:"Marquez (API)",status:"running"}),t.jsx(w,{name:"Superset",status:"running"}),t.jsx(w,{name:"Gitea",status:"running"})]})]})]}),k&&t.jsx("div",{ref:se,className:"fixed z-[9999] rounded-xl shadow-2xl py-1 min-w-[180px]",style:{left:k.x,top:k.y,background:"#0d0d12",border:"1px solid rgba(255,255,255,0.09)"},onClick:e=>e.stopPropagation(),children:k.items.map((e,a)=>e.separator?t.jsx("div",{className:"h-[1px] my-1",style:{background:"rgba(255,255,255,0.06)"}},a):t.jsxs("button",{className:"w-full px-3 py-1.5 text-left text-[11px] flex items-center gap-2 transition-colors rounded-md mx-1",style:{color:"rgba(255,255,255,0.65)",width:"calc(100% - 8px)"},onMouseEnter:s=>s.currentTarget.style.background="rgba(99,102,241,0.15)",onMouseLeave:s=>s.currentTarget.style.background="transparent",onClick:()=>{e.action(),z(null)},children:[e.icon,e.label]},a))})]})},w=({name:r,status:c})=>t.jsxs("div",{className:"flex items-center py-[3px] px-2 rounded-md cursor-pointer transition-colors group active:scale-95",style:{color:"rgba(255,255,255,0.45)"},onMouseEnter:l=>l.currentTarget.style.background="rgba(255,255,255,0.03)",onMouseLeave:l=>l.currentTarget.style.background="transparent",children:[t.jsx("div",{className:"w-1.5 h-1.5 rounded-full mr-2 flex-shrink-0",style:{background:c==="running"?"#4caf50":"#e57373"}}),t.jsx("span",{style:{fontSize:11},children:r})]}),vt=()=>t.jsx(h.Suspense,{fallback:t.jsx("div",{className:"flex h-full",style:{background:"#09090b"}}),children:t.jsx(kt,{})});export{Te as A,Ee as B,Z as C,q as D,Re as E,_ as F,Y as G,Xe as H,at as I,nt as L,ot as M,ct as P,vt as S,H as T,ut as a,dt as b,p as c,yt as d,E as e,Oe as f,ze as g,U as h,Ze as i,ee as j,Q as k,Ve as l,K as m,Be as n};
