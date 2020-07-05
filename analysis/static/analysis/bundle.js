var app=function(){"use strict";function e(){}function t(e){return e()}function n(){return Object.create(null)}function s(e){e.forEach(t)}function l(e){return"function"==typeof e}function i(e,t){return e!=e?t==t:e!==t||e&&"object"==typeof e||"function"==typeof e}function r(e,t){e.appendChild(t)}function a(e,t,n){e.insertBefore(t,n||null)}function o(e){e.parentNode.removeChild(e)}function c(e,t){for(let n=0;n<e.length;n+=1)e[n]&&e[n].d(t)}function f(e){return document.createElement(e)}function d(e){return document.createTextNode(e)}function u(){return d(" ")}function p(){return d("")}function m(e,t,n,s){return e.addEventListener(t,n,s),()=>e.removeEventListener(t,n,s)}function h(e,t,n){null==n?e.removeAttribute(t):e.getAttribute(t)!==n&&e.setAttribute(t,n)}function g(e,t){t=""+t,e.data!==t&&(e.data=t)}function v(e,t){(null!=t||e.value)&&(e.value=t)}function b(e,t){for(let n=0;n<e.options.length;n+=1){const s=e.options[n];if(s.__value===t)return void(s.selected=!0)}}function _(e){const t=e.querySelector(":checked")||e.options[0];return t&&t.__value}let w;function j(e){w=e}function y(e){(function(){if(!w)throw new Error("Function called outside component initialization");return w})().$$.on_mount.push(e)}const O=[],x=[],k=[],$=[],q=Promise.resolve();let C=!1;function E(e){k.push(e)}function A(){const e=new Set;do{for(;O.length;){const e=O.shift();j(e),L(e.$$)}for(;x.length;)x.pop()();for(let t=0;t<k.length;t+=1){const n=k[t];e.has(n)||(n(),e.add(n))}k.length=0}while(O.length);for(;$.length;)$.pop()();C=!1}function L(e){null!==e.fragment&&(e.update(e.dirty),s(e.before_update),e.fragment&&e.fragment.p(e.dirty,e.ctx),e.dirty=null,e.after_update.forEach(E))}const S=new Set;const N="undefined"!=typeof window?window:global;function T(e,t){e.$$.dirty||(O.push(e),C||(C=!0,q.then(A)),e.$$.dirty=n()),e.$$.dirty[t]=!0}function M(i,r,a,o,c,f){const d=w;j(i);const u=r.props||{},p=i.$$={fragment:null,ctx:null,props:f,update:e,not_equal:c,bound:n(),on_mount:[],on_destroy:[],before_update:[],after_update:[],context:new Map(d?d.$$.context:[]),callbacks:n(),dirty:null};let m=!1;var h,g,v;p.ctx=a?a(i,u,(e,t,n=t)=>(p.ctx&&c(p.ctx[e],p.ctx[e]=n)&&(p.bound[e]&&p.bound[e](n),m&&T(i,e)),t)):u,p.update(),m=!0,s(p.before_update),p.fragment=!!o&&o(p.ctx),r.target&&(r.hydrate?p.fragment&&p.fragment.l((v=r.target,Array.from(v.childNodes))):p.fragment&&p.fragment.c(),r.intro&&((h=i.$$.fragment)&&h.i&&(S.delete(h),h.i(g))),function(e,n,i){const{fragment:r,on_mount:a,on_destroy:o,after_update:c}=e.$$;r&&r.m(n,i),E(()=>{const n=a.map(t).filter(l);o?o.push(...n):s(n),e.$$.on_mount=[]}),c.forEach(E)}(i,r.target,r.anchor),A()),j(d)}class R{$destroy(){!function(e,t){const n=e.$$;null!==n.fragment&&(s(n.on_destroy),n.fragment&&n.fragment.d(t),n.on_destroy=n.fragment=null,n.ctx={})}(this,1),this.$destroy=e}$on(e,t){const n=this.$$.callbacks[e]||(this.$$.callbacks[e]=[]);return n.push(t),()=>{const e=n.indexOf(t);-1!==e&&n.splice(e,1)}}$set(){}}const z=5e3;async function F(e){let t;try{t=await e.json()}catch(t){return{ok:!1,error:"Server error encountered",status:e.status}}return t.ok=e.ok,t}async function P(e){let t;try{t=await fetch(e)}catch{return{ok:!1,error:"Kantele encountered a network error",status:!1}}return await F(t)}const{Object:D}=N;function I(e,t,n){const s=D.create(e);return s.ffilep=t[n],s}function U(e,t,n){const s=D.create(e);return s.resfile=t[n],s}function W(e,t,n){const s=D.create(e);return s.libfn=t[n],s}function H(e,t,n){const s=D.create(e);return s.filep=t[n],s}function B(e,t,n){const s=D.create(e);return s.flag=t[n],s}function Y(e,t,n){const s=D.create(e);return s.ch=t[n].ch,s.sample=t[n].sample,s}function K(e,t,n){const s=D.create(e);return s.isoq=t[n],s.each_value_5=t,s.isoq_index=n,s}function V(e,t,n){const s=D.create(e);return s.fn=t[n],s.each_value_8=t,s.fn_index=n,s}function J(e,t,n){const s=D.create(e);return s.sf=t[n],s}function X(e,t,n){const s=D.create(e);return s.ds=t[n],s.each_value_7=t,s.ds_index=n,s}function Z(e,t,n){const s=D.create(e);return s.wfv=t[n],s}function G(e,t,n){const s=D.create(e);return s.wfid=t[n],s}function Q(e,t,n){const s=D.create(e);return s.message=t[n],s}function ee(e,t,n){const s=D.create(e);return s.link=t[n],s}function te(e,t,n){const s=D.create(e);return s.error=t[n],s}function ne(e){let t,n=Object.entries(e.notif.errors).filter(Xe).map(Ze),s=[];for(let t=0;t<n.length;t+=1)s[t]=se(te(e,n,t));return{c(){t=f("div");for(let e=0;e<s.length;e+=1)s[e].c();h(t,"class","notification is-danger is-light")},m(e,n){a(e,t,n);for(let e=0;e<s.length;e+=1)s[e].m(t,null)},p(e,l){if(e.Object||e.notif){let i;for(n=Object.entries(l.notif.errors).filter(Xe).map(Ze),i=0;i<n.length;i+=1){const r=te(l,n,i);s[i]?s[i].p(e,r):(s[i]=se(r),s[i].c(),s[i].m(t,null))}for(;i<s.length;i+=1)s[i].d(1);s.length=n.length}},d(e){e&&o(t),c(s,e)}}}function se(e){let t,n,s=e.error+"";return{c(){t=f("div"),n=d(s)},m(e,s){a(e,t,s),r(t,n)},p(e,t){(e.Object||e.notif)&&s!==(s=t.error+"")&&g(n,s)},d(e){e&&o(t)}}}function le(e){let t,n=Object.entries(e.notif.links).filter(Ge).map(Qe),s=[];for(let t=0;t<n.length;t+=1)s[t]=ie(ee(e,n,t));return{c(){t=f("div");for(let e=0;e<s.length;e+=1)s[e].c();h(t,"class","notification is-danger is-light errormsg svelte-5nys59")},m(e,n){a(e,t,n);for(let e=0;e<s.length;e+=1)s[e].m(t,null)},p(e,l){if(e.Object||e.notif){let i;for(n=Object.entries(l.notif.links).filter(Ge).map(Qe),i=0;i<n.length;i+=1){const r=ee(l,n,i);s[i]?s[i].p(e,r):(s[i]=ie(r),s[i].c(),s[i].m(t,null))}for(;i<s.length;i+=1)s[i].d(1);s.length=n.length}},d(e){e&&o(t),c(s,e)}}}function ie(e){let t,n,s,l,i;return{c(){t=f("div"),n=d("Click here: "),s=f("a"),l=d("here"),h(s,"target","_blank"),h(s,"href",i=e.link)},m(e,i){a(e,t,i),r(t,n),r(t,s),r(s,l)},p(e,t){(e.Object||e.notif)&&i!==(i=t.link)&&h(s,"href",i)},d(e){e&&o(t)}}}function re(e){let t,n=Object.entries(e.notif.messages).filter(et).map(tt),s=[];for(let t=0;t<n.length;t+=1)s[t]=ae(Q(e,n,t));return{c(){t=f("div");for(let e=0;e<s.length;e+=1)s[e].c();h(t,"class","notification is-success is-light errormsg svelte-5nys59")},m(e,n){a(e,t,n);for(let e=0;e<s.length;e+=1)s[e].m(t,null)},p(e,l){if(e.Object||e.notif){let i;for(n=Object.entries(l.notif.messages).filter(et).map(tt),i=0;i<n.length;i+=1){const r=Q(l,n,i);s[i]?s[i].p(e,r):(s[i]=ae(r),s[i].c(),s[i].m(t,null))}for(;i<s.length;i+=1)s[i].d(1);s.length=n.length}},d(e){e&&o(t),c(s,e)}}}function ae(e){let t,n,s=e.message+"";return{c(){t=f("div"),n=d(s)},m(e,s){a(e,t,s),r(t,n)},p(e,t){(e.Object||e.notif)&&s!==(s=t.message+"")&&g(n,s)},d(e){e&&o(t)}}}function oe(e){let t,n,s,l,i=e.allwfs[e.wfid].name+"";return{c(){t=f("option"),n=d(i),s=u(),t.__value=l=e.wfid,t.value=t.__value},m(e,l){a(e,t,l),r(t,n),r(t,s)},p(e,s){(e.allwfs||e.wforder)&&i!==(i=s.allwfs[s.wfid].name+"")&&g(n,i),e.wforder&&l!==(l=s.wfid)&&(t.__value=l),t.value=t.__value},d(e){e&&o(t)}}}function ce(e){let t,n=e.allwfs[e.config.wfid].versions,s=[];for(let t=0;t<n.length;t+=1)s[t]=de(Z(e,n,t));return{c(){for(let e=0;e<s.length;e+=1)s[e].c();t=p()},m(e,n){for(let t=0;t<s.length;t+=1)s[t].m(e,n);a(e,t,n)},p(e,l){if(e.allwfs||e.config){let i;for(n=l.allwfs[l.config.wfid].versions,i=0;i<n.length;i+=1){const r=Z(l,n,i);s[i]?s[i].p(e,r):(s[i]=de(r),s[i].c(),s[i].m(t.parentNode,t))}for(;i<s.length;i+=1)s[i].d(1);s.length=n.length}},d(e){c(s,e),e&&o(t)}}}function fe(e){let t;return{c(){(t=f("span")).textContent="LATEST:"},m(e,n){a(e,t,n)},d(e){e&&o(t)}}}function de(e){let t,n,s,l,i,c,p,m=e.wfv.date+"",h=e.wfv.name+"",v=e.wfv.latest&&fe();return{c(){t=f("option"),v&&v.c(),n=u(),s=d(m),l=d(" -- "),i=d(h),c=u(),t.__value=p=e.wfv,t.value=t.__value},m(e,o){a(e,t,o),v&&v.m(t,null),r(t,n),r(t,s),r(t,l),r(t,i),r(t,c)},p(e,l){l.wfv.latest?v||((v=fe()).c(),v.m(t,n)):v&&(v.d(1),v=null),(e.allwfs||e.config)&&m!==(m=l.wfv.date+"")&&g(s,m),(e.allwfs||e.config)&&h!==(h=l.wfv.name+"")&&g(i,h),(e.allwfs||e.config)&&p!==(p=l.wfv)&&(t.__value=p),t.value=t.__value},d(e){e&&o(t),v&&v.d()}}}function ue(e){let t,n,s,l,i,b,_,w,j,y,O,x,k,$,q,C,E,A,L,S,N,T,M,R,z,F,P,D=e.allwfs[e.config.wfid].wftype+"",I=e.config.analysisname+"",U=Object.keys(e.isoquants).length,W=!("flags"in e.wf&&Object.keys(e.wf.flags).length),Y=Object.values(e.dsets),K=[];for(let t=0;t<Y.length;t+=1)K[t]=ke(X(e,Y,t));let V=U&&$e(e),J=Object.entries(e.wf.flags),Z=[];for(let t=0;t<J.length;t+=1)Z[t]=Ne(B(e,J,t));let G=W&&Te(),Q=e.wf.fileparams,ee=[];for(let t=0;t<Q.length;t+=1)ee[t]=Pe(H(e,Q,t));let te=e.wf.fixedfileparams.length&&De(e);function ne(e,t){return t.runButtonActive?He:t.postingAnalysis?We:Ue}let se=ne(0,e),le=se(e);return{c(){t=f("div"),n=f("input"),s=u(),l=f("div"),i=d("Full name will be "),b=f("code"),_=d(D),w=d("_"),j=d(I),y=d("\n    This will be the folder name for the output and prefixed to the output filenames"),O=u(),(x=f("div")).textContent="Datasets",k=u();for(let e=0;e<K.length;e+=1)K[e].c();$=u(),V&&V.c(),q=u(),C=f("div"),(E=f("div")).textContent="Workflow parameters",A=u();for(let e=0;e<Z.length;e+=1)Z[e].c();L=u(),G&&G.c(),S=u(),N=f("div"),(T=f("div")).textContent="Input files",M=u();for(let e=0;e<ee.length;e+=1)ee[e].c();R=u(),te&&te.c(),z=u(),le.c(),F=p(),h(n,"type","text"),h(n,"class","input"),h(n,"placeholder","Please enter analysis name"),h(t,"class","field"),h(x,"class","title is-5"),h(E,"class","title is-5"),h(C,"class","box"),h(T,"class","title is-5"),h(N,"class","box"),P=m(n,"input",e.input_input_handler)},m(o,c){a(o,t,c),r(t,n),v(n,e.config.analysisname),r(t,s),r(t,l),r(l,i),r(l,b),r(b,_),r(b,w),r(b,j),r(l,y),a(o,O,c),a(o,x,c),a(o,k,c);for(let e=0;e<K.length;e+=1)K[e].m(o,c);a(o,$,c),V&&V.m(o,c),a(o,q,c),a(o,C,c),r(C,E),r(C,A);for(let e=0;e<Z.length;e+=1)Z[e].m(C,null);r(C,L),G&&G.m(C,null),a(o,S,c),a(o,N,c),r(N,T),r(N,M);for(let e=0;e<ee.length;e+=1)ee[e].m(N,null);a(o,R,c),te&&te.m(o,c),a(o,z,c),le.m(o,c),a(o,F,c)},p(e,t){if(e.config&&n.value!==t.config.analysisname&&v(n,t.config.analysisname),(e.allwfs||e.config)&&D!==(D=t.allwfs[t.config.wfid].wftype+"")&&g(_,D),e.config&&I!==(I=t.config.analysisname+"")&&g(j,I),e.Object||e.dsets||e.matchedFr||e.frregex||e.matchFractions||e.updateIsoquant){let n;for(Y=Object.values(t.dsets),n=0;n<Y.length;n+=1){const s=X(t,Y,n);K[n]?K[n].p(e,s):(K[n]=ke(s),K[n].c(),K[n].m($.parentNode,$))}for(;n<K.length;n+=1)K[n].d(1);K.length=Y.length}if((e.Object||e.isoquants)&&(U=Object.keys(t.isoquants).length),U?V?V.p(e,t):((V=$e(t)).c(),V.m(q.parentNode,q)):V&&(V.d(1),V=null),e.Object||e.wf||e.config){let n;for(J=Object.entries(t.wf.flags),n=0;n<J.length;n+=1){const s=B(t,J,n);Z[n]?Z[n].p(e,s):(Z[n]=Ne(s),Z[n].c(),Z[n].m(C,L))}for(;n<Z.length;n+=1)Z[n].d(1);Z.length=J.length}if((e.wf||e.Object)&&(W=!("flags"in t.wf&&Object.keys(t.wf.flags).length)),W?G||((G=Te()).c(),G.m(C,null)):G&&(G.d(1),G=null),e.config||e.wf){let n;for(Q=t.wf.fileparams,n=0;n<Q.length;n+=1){const s=H(t,Q,n);ee[n]?ee[n].p(e,s):(ee[n]=Pe(s),ee[n].c(),ee[n].m(N,null))}for(;n<ee.length;n+=1)ee[n].d(1);ee.length=Q.length}t.wf.fixedfileparams.length?te?te.p(e,t):((te=De(t)).c(),te.m(z.parentNode,z)):te&&(te.d(1),te=null),se===(se=ne(0,t))&&le?le.p(e,t):(le.d(1),(le=se(t))&&(le.c(),le.m(F.parentNode,F)))},d(e){e&&o(t),e&&o(O),e&&o(x),e&&o(k),c(K,e),e&&o($),V&&V.d(e),e&&o(q),e&&o(C),c(Z,e),G&&G.d(),e&&o(S),e&&o(N),c(ee,e),e&&o(R),te&&te.d(e),e&&o(z),le.d(e),e&&o(F),P()}}}function pe(e){let t,n,s,l,i,m,v,b,_,w,j,y,O,x,k,$,q,C,E,A,L,S,N,T,M,R,z,F=e.ds.proj+"",P=e.ds.exp+"",D=e.ds.run+"",I=e.ds.details.qtype+"",U=e.ds.details.instruments.join(", ")+"",W=!e.ds.prefrac&&he(e),H=!e.ds.filesaresets&&ge(e);function B(e,t){return t.ds.prefrac?t.ds.hr?be:ve:_e}let Y=B(0,e),K=Y(e),V=Object.entries(e.ds.details.nrstoredfiles),X=[];for(let t=0;t<V.length;t+=1)X[t]=we(J(e,V,t));let Z=e.ds.details.nrstoredfiles.refined_mzML&&je(),G=e.ds.prefrac&&ye(e),Q=e.ds.filesaresets&&Oe(e);return{c(){t=f("div"),n=f("div"),W&&W.c(),s=u(),H&&H.c(),l=u(),i=f("div"),m=f("span"),v=d(F),b=d(" // "),_=d(P),w=d(" // "),j=d(D),y=d(" //"),O=u(),K.c(),x=u(),k=f("div"),$=f("span"),q=d(I),C=u();for(let e=0;e<X.length;e+=1)X[e].c();E=u(),A=f("span"),L=d("// "),S=d(U),N=u(),Z&&Z.c(),T=u(),M=f("div"),G&&G.c(),R=u(),Q&&Q.c(),z=p(),h(i,"class","subtitle is-6 has-text-primary"),h(k,"class","subtitle is-6"),h(n,"class","column"),h(M,"class","column"),h(t,"class","columns")},m(e,o){a(e,t,o),r(t,n),W&&W.m(n,null),r(n,s),H&&H.m(n,null),r(n,l),r(n,i),r(i,m),r(m,v),r(m,b),r(m,_),r(m,w),r(m,j),r(m,y),r(i,O),K.m(i,null),r(n,x),r(n,k),r(k,$),r($,q),r(k,C);for(let e=0;e<X.length;e+=1)X[e].m(k,null);r(k,E),r(k,A),r(A,L),r(A,S),r(n,N),Z&&Z.m(n,null),r(t,T),r(t,M),G&&G.m(M,null),a(e,R,o),Q&&Q.m(e,o),a(e,z,o)},p(e,t){if(t.ds.prefrac?W&&(W.d(1),W=null):W?W.p(e,t):((W=he(t)).c(),W.m(n,s)),t.ds.filesaresets?H&&(H.d(1),H=null):H?H.p(e,t):((H=ge(t)).c(),H.m(n,l)),(e.Object||e.dsets)&&F!==(F=t.ds.proj+"")&&g(v,F),(e.Object||e.dsets)&&P!==(P=t.ds.exp+"")&&g(_,P),(e.Object||e.dsets)&&D!==(D=t.ds.run+"")&&g(j,D),Y===(Y=B(0,t))&&K?K.p(e,t):(K.d(1),(K=Y(t))&&(K.c(),K.m(i,null))),(e.Object||e.dsets)&&I!==(I=t.ds.details.qtype+"")&&g(q,I),e.Object||e.dsets){let n;for(V=Object.entries(t.ds.details.nrstoredfiles),n=0;n<V.length;n+=1){const s=J(t,V,n);X[n]?X[n].p(e,s):(X[n]=we(s),X[n].c(),X[n].m(k,E))}for(;n<X.length;n+=1)X[n].d(1);X.length=V.length}(e.Object||e.dsets)&&U!==(U=t.ds.details.instruments.join(", ")+"")&&g(S,U),t.ds.details.nrstoredfiles.refined_mzML?Z||((Z=je()).c(),Z.m(n,null)):Z&&(Z.d(1),Z=null),t.ds.prefrac?G?G.p(e,t):((G=ye(t)).c(),G.m(M,null)):G&&(G.d(1),G=null),t.ds.filesaresets?Q?Q.p(e,t):((Q=Oe(t)).c(),Q.m(z.parentNode,z)):Q&&(Q.d(1),Q=null)},d(e){e&&o(t),W&&W.d(),H&&H.d(),K.d(),c(X,e),Z&&Z.d(),G&&G.d(),e&&o(R),Q&&Q.d(e),e&&o(z)}}}function me(e){let t,n,s,l,i,c,u,p,m=e.ds.proj+"",v=e.ds.run+"",b=e.ds.details.qtype+"",_=e.ds.details.instruments.join(",")+"";return{c(){t=f("span"),n=d(m),s=d(" // Labelcheck // "),l=d(v),i=d(" // "),c=d(b),u=d(" // "),p=d(_),h(t,"class","has-text-primary")},m(e,o){a(e,t,o),r(t,n),r(t,s),r(t,l),r(t,i),r(t,c),r(t,u),r(t,p)},p(e,t){(e.Object||e.dsets)&&m!==(m=t.ds.proj+"")&&g(n,m),(e.Object||e.dsets)&&v!==(v=t.ds.run+"")&&g(l,v),(e.Object||e.dsets)&&b!==(b=t.ds.details.qtype+"")&&g(c,b),(e.Object||e.dsets)&&_!==(_=t.ds.details.instruments.join(",")+"")&&g(p,_)},d(e){e&&o(t)}}}function he(e){let t,n,s,l;function i(){e.input_change_handler.call(t,e)}return{c(){t=f("input"),n=u(),(s=f("label")).textContent="Each file is a different sample",h(t,"type","checkbox"),h(s,"class","checkbox"),l=m(t,"change",i)},m(l,i){a(l,t,i),t.checked=e.ds.filesaresets,a(l,n,i),a(l,s,i)},p(n,s){e=s,(n.Object||n.dsets)&&(t.checked=e.ds.filesaresets)},d(e){e&&o(t),e&&o(n),e&&o(s),l()}}}function ge(e){let t,n,l;function i(){e.input_input_handler_1.call(n,e)}return{c(){t=f("div"),h(n=f("input"),"type","text"),h(n,"class","input"),h(n,"placeholder","Name of set"),h(t,"class","field"),l=[m(n,"input",i),m(n,"change",e.updateIsoquant)]},m(s,l){a(s,t,l),r(t,n),v(n,e.ds.setname)},p(t,s){e=s,(t.Object||t.dsets)&&n.value!==e.ds.setname&&v(n,e.ds.setname)},d(e){e&&o(t),s(l)}}}function ve(e){let t,n,s=e.ds.prefrac+"";return{c(){t=f("span"),n=d(s)},m(e,s){a(e,t,s),r(t,n)},p(e,t){(e.Object||e.dsets)&&s!==(s=t.ds.prefrac+"")&&g(n,s)},d(e){e&&o(t)}}}function be(e){let t,n,s=e.ds.hr+"";return{c(){t=f("span"),n=d(s)},m(e,s){a(e,t,s),r(t,n)},p(e,t){(e.Object||e.dsets)&&s!==(s=t.ds.hr+"")&&g(n,s)},d(e){e&&o(t)}}}function _e(e){let t,n,s=e.ds.dtype+"";return{c(){t=f("span"),n=d(s)},m(e,s){a(e,t,s),r(t,n)},p(e,t){(e.Object||e.dsets)&&s!==(s=t.ds.dtype+"")&&g(n,s)},d(e){e&&o(t)}}}function we(e){let t,n,s,l,i,c,p=e.sf[1]+"",m=e.sf[0]+"";return{c(){t=f("span"),n=d("// "),s=d(p),l=u(),i=d(m),c=d(" files")},m(e,o){a(e,t,o),r(t,n),r(t,s),r(t,l),r(t,i),r(t,c)},p(e,t){(e.Object||e.dsets)&&p!==(p=t.sf[1]+"")&&g(s,p),(e.Object||e.dsets)&&m!==(m=t.sf[0]+"")&&g(i,m)},d(e){e&&o(t)}}}function je(e){let t;return{c(){(t=f("div")).innerHTML="<strong>Enforcing use of refined mzML(s)</strong>",h(t,"class","subtitle is-6")},m(e,n){a(e,t,n)},d(e){e&&o(t)}}}function ye(e){let t,n,l,i,c,p,b,_,w,j=e.matchedFr[e.ds.id]+"";function y(){e.input_input_handler_2.call(i,e)}function O(...t){return e.change_handler_1(e,...t)}return{c(){t=f("div"),(n=f("label")).textContent="Regex for fraction detection",l=u(),i=f("input"),c=u(),p=f("span"),b=d(j),_=d(" fractions matched"),h(n,"class","label"),h(i,"type","text"),h(i,"class","input"),h(t,"class","field"),w=[m(i,"input",y),m(i,"change",O)]},m(s,o){a(s,t,o),r(t,n),r(t,l),r(t,i),v(i,e.frregex[e.ds.id]),a(s,c,o),a(s,p,o),r(p,b),r(p,_)},p(t,n){e=n,(t.frregex||t.Object||t.dsets)&&i.value!==e.frregex[e.ds.id]&&v(i,e.frregex[e.ds.id]),(t.matchedFr||t.Object||t.dsets)&&j!==(j=e.matchedFr[e.ds.id]+"")&&g(b,j)},d(e){e&&o(t),e&&o(c),e&&o(p),s(w)}}}function Oe(e){let t,n=e.ds.files,s=[];for(let t=0;t<n.length;t+=1)s[t]=xe(V(e,n,t));return{c(){for(let e=0;e<s.length;e+=1)s[e].c();t=p()},m(e,n){for(let t=0;t<s.length;t+=1)s[t].m(e,n);a(e,t,n)},p(e,l){if(e.Object||e.dsets){let i;for(n=l.ds.files,i=0;i<n.length;i+=1){const r=V(l,n,i);s[i]?s[i].p(e,r):(s[i]=xe(r),s[i].c(),s[i].m(t.parentNode,t))}for(;i<s.length;i+=1)s[i].d(1);s.length=n.length}},d(e){c(s,e),e&&o(t)}}}function xe(e){let t,n,s,l,i,c,p,b,_=e.fn.name+"";function w(){e.input_input_handler_3.call(c,e)}return{c(){t=f("div"),n=f("div"),s=d(_),l=u(),i=f("div"),c=f("input"),h(n,"class","column"),h(c,"type","text"),h(c,"class","input"),h(c,"placeholder",p=e.fn.sample),h(i,"class","column"),h(t,"class","columns"),b=m(c,"input",w)},m(o,f){a(o,t,f),r(t,n),r(n,s),r(t,l),r(t,i),r(i,c),v(c,e.fn.setname)},p(t,n){e=n,(t.Object||t.dsets)&&_!==(_=e.fn.name+"")&&g(s,_),(t.Object||t.dsets)&&c.value!==e.fn.setname&&v(c,e.fn.setname),(t.Object||t.dsets)&&p!==(p=e.fn.sample)&&h(c,"placeholder",p)},d(e){e&&o(t),b()}}}function ke(e){let t,n;function s(e,t){return(null==n||e.Object||e.dsets)&&(n=!("labelcheck"!==t.ds.dtype.toLowerCase())),n?me:pe}let l=s(null,e),i=l(e);return{c(){t=f("div"),i.c(),h(t,"class","box")},m(e,n){a(e,t,n),i.m(t,null)},p(e,n){l===(l=s(e,n))&&i?i.p(e,n):(i.d(1),(i=l(n))&&(i.c(),i.m(t,null)))},d(e){e&&o(t),i.d()}}}function $e(e){let t,n,s,l,i=1===Object.keys(e.isoquants).length,d=i&&qe(e),p=Object.entries(e.isoquants),m=[];for(let t=0;t<p.length;t+=1)m[t]=Se(K(e,p,t));return{c(){t=f("div"),(n=f("div")).textContent="Isobaric quantification",s=u(),d&&d.c(),l=u();for(let e=0;e<m.length;e+=1)m[e].c();h(n,"class","title is-5"),h(t,"class","box")},m(e,i){a(e,t,i),r(t,n),r(t,s),d&&d.m(t,null),r(t,l);for(let e=0;e<m.length;e+=1)m[e].m(t,null)},p(e,n){if((e.Object||e.isoquants)&&(i=1===Object.keys(n.isoquants).length),i?d?d.p(e,n):((d=qe(n)).c(),d.m(t,l)):d&&(d.d(1),d=null),e.sortChannels||e.Object||e.isoquants||e.mediansweep){let s;for(p=Object.entries(n.isoquants),s=0;s<p.length;s+=1){const l=K(n,p,s);m[s]?m[s].p(e,l):(m[s]=Se(l),m[s].c(),m[s].m(t,null))}for(;s<m.length;s+=1)m[s].d(1);m.length=p.length}},d(e){e&&o(t),d&&d.d(),c(m,e)}}}function qe(e){let t,n,s,l,i;return{c(){t=f("div"),n=f("input"),s=u(),(l=f("label")).innerHTML='Use median sweeping (no predefined denominators)\n        <span class="icon is-small"><a title="Pick median denominator per PSM, only for single-set analyses"><i class="fa fa-question-circle"></i></a></span>',h(n,"type","checkbox"),h(l,"class","checkbox"),h(t,"class","field"),i=m(n,"change",e.input_change_handler_1)},m(i,o){a(i,t,o),r(t,n),n.checked=e.mediansweep,r(t,s),r(t,l)},p(e,t){e.mediansweep&&(n.checked=t.mediansweep)},d(e){e&&o(t),i()}}}function Ce(e){let t;return{c(){(t=f("th")).innerHTML="<del>Denominator</del>"},m(e,n){a(e,t,n)},d(e){e&&o(t)}}}function Ee(e){let t;return{c(){(t=f("th")).textContent="Denominator"},m(e,n){a(e,t,n)},d(e){e&&o(t)}}}function Ae(e){let t,n;function s(){e.input_change_handler_2.call(t,e)}return{c(){h(t=f("input"),"type","checkbox"),n=m(t,"change",s)},m(n,s){a(n,t,s),t.checked=e.isoq[1].denoms[e.ch]},p(n,s){e=s,(n.Object||n.isoquants||n.sortChannels)&&(t.checked=e.isoq[1].denoms[e.ch])},d(e){e&&o(t),n()}}}function Le(e){let t,n,s,l,i,c,p,b,_,w,j,y,O,x=e.ch+"",k=e.sample+"",$=!e.mediansweep&&Ae(e);function q(){e.input_input_handler_4.call(j,e)}return{c(){t=f("tr"),n=f("td"),$&&$.c(),s=u(),l=f("td"),i=d(x),c=u(),p=f("td"),b=d(k),_=u(),w=f("td"),j=f("input"),y=u(),h(j,"type","text"),h(j,"class","input"),h(j,"placeholder","Sample group or empty (e.g. CTRL, TREAT)"),O=m(j,"input",q)},m(o,f){a(o,t,f),r(t,n),$&&$.m(n,null),r(t,s),r(t,l),r(l,i),r(t,c),r(t,p),r(p,b),r(t,_),r(t,w),r(w,j),v(j,e.isoq[1].samplegroups[e.ch]),r(t,y)},p(t,s){(e=s).mediansweep?$&&($.d(1),$=null):$?$.p(t,e):(($=Ae(e)).c(),$.m(n,null)),(t.Object||t.isoquants)&&x!==(x=e.ch+"")&&g(i,x),(t.Object||t.isoquants)&&k!==(k=e.sample+"")&&g(b,k),(t.Object||t.isoquants||t.sortChannels)&&j.value!==e.isoq[1].samplegroups[e.ch]&&v(j,e.isoq[1].samplegroups[e.ch])},d(e){e&&o(t),$&&$.d(),O()}}}function Se(e){let t,n,s,l,i,p,m,v,b,_,w,j,y,O,x,k,$,q,C=e.isoq[0]+"";function E(e,t){return t.mediansweep?Ce:Ee}let A=E(0,e),L=A(e),S=Ye(e.isoq[1].channels),N=[];for(let t=0;t<S.length;t+=1)N[t]=Le(Y(e,S,t));return{c(){t=f("div"),n=d("Set: "),s=d(C),l=u(),i=f("div"),p=f("div"),m=f("table"),v=f("thead"),b=f("tr"),L.c(),_=u(),(w=f("th")).textContent="Channel",j=u(),(y=f("th")).textContent="Sample name",O=u(),(x=f("th")).innerHTML='Sample group \n                <span class="icon is-small"><a title="For DEqMS and PCA plots"><i class="fa fa-question-circle"></i></a></span>\n                LEAVE EMPTY FOR INTERNAL STANDARDS!',k=u(),$=f("tbody");for(let e=0;e<N.length;e+=1)N[e].c();q=u(),h(t,"class","has-text-primary title is-6"),h(m,"class","table is-striped is-narrow"),h(p,"class","column is-three-quarters"),h(i,"class","columns")},m(e,o){a(e,t,o),r(t,n),r(t,s),a(e,l,o),a(e,i,o),r(i,p),r(p,m),r(m,v),r(v,b),L.m(b,null),r(b,_),r(b,w),r(b,j),r(b,y),r(b,O),r(b,x),r(m,k),r(m,$);for(let e=0;e<N.length;e+=1)N[e].m($,null);r(i,q)},p(e,t){if((e.Object||e.isoquants)&&C!==(C=t.isoq[0]+"")&&g(s,C),A!==(A=E(0,t))&&(L.d(1),(L=A(t))&&(L.c(),L.m(b,_))),e.Object||e.isoquants||e.sortChannels||e.mediansweep){let n;for(S=Ye(t.isoq[1].channels),n=0;n<S.length;n+=1){const s=Y(t,S,n);N[n]?N[n].p(e,s):(N[n]=Le(s),N[n].c(),N[n].m($,null))}for(;n<N.length;n+=1)N[n].d(1);N.length=S.length}},d(e){e&&o(t),e&&o(l),e&&o(i),L.d(),c(N,e)}}}function Ne(e){let t,n,s,l,i,c,p,v,b,_,w=e.flag[1]+"",j=e.flag[0]+"";return{c(){t=f("div"),n=f("input"),l=u(),i=f("label"),c=d(w),p=d(": "),v=f("code"),b=d(j),e.$$binding_groups[0].push(n),n.__value=s=e.flag[0],n.value=n.__value,h(n,"type","checkbox"),h(i,"class","checkbox"),_=m(n,"change",e.input_change_handler_3)},m(s,o){a(s,t,o),r(t,n),n.checked=~e.config.flags.indexOf(n.__value),r(t,l),r(t,i),r(i,c),r(t,p),r(t,v),r(v,b)},p(e,t){e.config&&(n.checked=~t.config.flags.indexOf(n.__value)),(e.Object||e.wf)&&s!==(s=t.flag[0])&&(n.__value=s),n.value=n.__value,(e.Object||e.wf)&&w!==(w=t.flag[1]+"")&&g(c,w),(e.Object||e.wf)&&j!==(j=t.flag[0]+"")&&g(b,j)},d(s){s&&o(t),e.$$binding_groups[0].splice(e.$$binding_groups[0].indexOf(n),1),_()}}}function Te(e){let t;return{c(){(t=f("div")).textContent="No parameters for this workflow"},m(e,n){a(e,t,n)},d(e){e&&o(t)}}}function Me(e){let t,n=e.wf.libfiles[e.filep.ftype],s=[];for(let t=0;t<n.length;t+=1)s[t]=Re(W(e,n,t));return{c(){for(let e=0;e<s.length;e+=1)s[e].c();t=p()},m(e,n){for(let t=0;t<s.length;t+=1)s[t].m(e,n);a(e,t,n)},p(e,l){if(e.wf){let i;for(n=l.wf.libfiles[l.filep.ftype],i=0;i<n.length;i+=1){const r=W(l,n,i);s[i]?s[i].p(e,r):(s[i]=Re(r),s[i].c(),s[i].m(t.parentNode,t))}for(;i<s.length;i+=1)s[i].d(1);s.length=n.length}},d(e){c(s,e),e&&o(t)}}}function Re(e){let t,n,s,l,i,c=e.libfn.name+"",u=e.libfn.desc+"";return{c(){t=f("option"),n=d(c),s=d(" -- "),l=d(u),t.__value=i=e.libfn.id,t.value=t.__value},m(e,i){a(e,t,i),r(t,n),r(t,s),r(t,l)},p(e,s){e.wf&&c!==(c=s.libfn.name+"")&&g(n,c),e.wf&&u!==(u=s.libfn.desc+"")&&g(l,u),e.wf&&i!==(i=s.libfn.id)&&(t.__value=i),t.value=t.__value},d(e){e&&o(t)}}}function ze(e){let t,n=e.wf.prev_resultfiles,s=[];for(let t=0;t<n.length;t+=1)s[t]=Fe(U(e,n,t));return{c(){for(let e=0;e<s.length;e+=1)s[e].c();t=p()},m(e,n){for(let t=0;t<s.length;t+=1)s[t].m(e,n);a(e,t,n)},p(e,l){if(e.wf){let i;for(n=l.wf.prev_resultfiles,i=0;i<n.length;i+=1){const r=U(l,n,i);s[i]?s[i].p(e,r):(s[i]=Fe(r),s[i].c(),s[i].m(t.parentNode,t))}for(;i<s.length;i+=1)s[i].d(1);s.length=n.length}},d(e){c(s,e),e&&o(t)}}}function Fe(e){let t,n,s,l,i,c,u,p=e.resfile.analysisname+"",m=e.resfile.analysisdate+"",h=e.resfile.name+"";return{c(){t=f("option"),n=d(p),s=d(" -- "),l=d(m),i=d(" -- "),c=d(h),t.__value=u=e.resfile.id,t.value=t.__value},m(e,o){a(e,t,o),r(t,n),r(t,s),r(t,l),r(t,i),r(t,c)},p(e,s){e.wf&&p!==(p=s.resfile.analysisname+"")&&g(n,p),e.wf&&m!==(m=s.resfile.analysisdate+"")&&g(l,m),e.wf&&h!==(h=s.resfile.name+"")&&g(c,h),e.wf&&u!==(u=s.resfile.id)&&(t.__value=u),t.value=t.__value},d(e){e&&o(t)}}}function Pe(e){let t,n,s,l,i,c,v,_,w,j,y,O=e.filep.name+"",x=e.filep.ftype in e.wf.libfiles&&Me(e),k=e.filep.allow_resultfile&&ze(e);function $(){e.select_change_handler.call(c,e)}return{c(){t=f("div"),n=f("label"),s=d(O),l=u(),i=f("div"),c=f("select"),(v=f("option")).textContent="Please select one",(_=f("option")).textContent="Do not use this parameter",x&&x.c(),w=p(),k&&k.c(),j=u(),h(n,"class","label"),v.disabled=!0,v.__value="",v.value=v.__value,_.__value="",_.value=_.__value,void 0===e.config.fileparams[e.filep.nf]&&E($),h(i,"class","select"),h(t,"class","field"),y=m(c,"change",$)},m(o,f){a(o,t,f),r(t,n),r(n,s),r(t,l),r(t,i),r(i,c),r(c,v),r(c,_),x&&x.m(c,null),r(c,w),k&&k.m(c,null),b(c,e.config.fileparams[e.filep.nf]),r(t,j)},p(t,n){e=n,t.wf&&O!==(O=e.filep.name+"")&&g(s,O),e.filep.ftype in e.wf.libfiles?x?x.p(t,e):((x=Me(e)).c(),x.m(c,w)):x&&(x.d(1),x=null),e.filep.allow_resultfile?k?k.p(t,e):((k=ze(e)).c(),k.m(c,null)):k&&(k.d(1),k=null),(t.config||t.wf)&&b(c,e.config.fileparams[e.filep.nf])},d(e){e&&o(t),x&&x.d(),k&&k.d(),y()}}}function De(e){let t,n,s,l=e.wf.fixedfileparams,i=[];for(let t=0;t<l.length;t+=1)i[t]=Ie(I(e,l,t));return{c(){t=f("div"),(n=f("div")).textContent="Predefined files",s=u();for(let e=0;e<i.length;e+=1)i[e].c();h(n,"class","title is-5"),h(t,"class","box")},m(e,l){a(e,t,l),r(t,n),r(t,s);for(let e=0;e<i.length;e+=1)i[e].m(t,null)},p(e,n){if(e.wf){let s;for(l=n.wf.fixedfileparams,s=0;s<l.length;s+=1){const r=I(n,l,s);i[s]?i[s].p(e,r):(i[s]=Ie(r),i[s].c(),i[s].m(t,null))}for(;s<i.length;s+=1)i[s].d(1);i.length=l.length}},d(e){e&&o(t),c(i,e)}}}function Ie(e){let t,n,s,l,i,c,p,m,v,b,_,w,j,y=e.ffilep.name+"",O=e.ffilep.fn+"",x=e.ffilep.desc+"";return{c(){t=f("div"),n=f("label"),s=d(y),l=u(),i=f("div"),c=f("select"),(p=f("option")).textContent="Fixed selection",m=f("option"),v=d(O),b=d(" -- "),_=d(x),j=u(),h(n,"class","label"),p.disabled=!0,p.__value="",p.value=p.__value,m.__value=w=e.ffilep.fn+" -- "+e.ffilep.desc,m.value=m.__value,h(i,"class","select"),h(t,"class","field")},m(e,o){a(e,t,o),r(t,n),r(n,s),r(t,l),r(t,i),r(i,c),r(c,p),r(c,m),r(m,v),r(m,b),r(m,_),r(t,j)},p(e,t){e.wf&&y!==(y=t.ffilep.name+"")&&g(s,y),e.wf&&O!==(O=t.ffilep.fn+"")&&g(v,O),e.wf&&x!==(x=t.ffilep.desc+"")&&g(_,x),e.wf&&w!==(w=t.ffilep.fn+" -- "+t.ffilep.desc)&&(m.__value=w),m.value=m.__value},d(e){e&&o(t)}}}function Ue(t){let n;return{c(){(n=f("a")).textContent="Run analysis",h(n,"class","button is-primary"),h(n,"disabled","")},m(e,t){a(e,n,t)},p:e,d(e){e&&o(n)}}}function We(t){let n;return{c(){(n=f("a")).textContent="Run analysis",h(n,"class","button is-primary is-loading")},m(e,t){a(e,n,t)},p:e,d(e){e&&o(n)}}}function He(t){let n,s;return{c(){(n=f("a")).textContent="Run analysis",h(n,"class","button is-primary"),s=m(n,"click",t.runAnalysis)},m(e,t){a(e,n,t)},p:e,d(e){e&&o(n),s()}}}function Be(t){let n,l,i,d,p,g,v,_,w,j,y,O,x,k,$,q,C,A,L,S,N,T,M,R,z,F,P,D=Object.values(t.notif.errors).some(Je),I=Object.values(t.notif.links).some(Ve),U=Object.values(t.notif.messages).some(Ke),W=D&&ne(t),H=I&&le(t),B=U&&re(t),Y=t.wforder,K=[];for(let e=0;e<Y.length;e+=1)K[e]=oe(G(t,Y,e));let V=t.config.wfid&&ce(t),J=t.wf&&ue(t);return{c(){n=f("div"),W&&W.c(),l=u(),H&&H.c(),i=u(),B&&B.c(),d=u(),p=f("div"),(g=f("div")).textContent="Analysis",v=u(),_=f("div"),(w=f("div")).innerHTML='<label class="label">Workflow:</label>',j=u(),y=f("div"),O=f("div"),x=f("div"),k=f("select"),($=f("option")).textContent="Select workflow";for(let e=0;e<K.length;e+=1)K[e].c();C=u(),(A=f("div")).innerHTML='<label class="label">Workflow version:</label>',L=u(),S=f("div"),N=f("div"),T=f("div"),M=f("select"),(R=f("option")).textContent="Select workflow version",V&&V.c(),F=u(),J&&J.c(),h(n,"class","errormsg svelte-5nys59"),h(g,"class","title is-5"),h(w,"class","field-label is-normal"),$.disabled=!0,$.__value=q=!1,$.value=$.__value,void 0===t.config.wfid&&E(()=>t.select0_change_handler.call(k)),h(x,"class","select"),h(O,"class","field"),h(y,"class","field-body"),h(A,"class","field-label is-normal"),R.disabled=!0,R.__value=z=!1,R.value=R.__value,void 0===t.config.wfversion&&E(()=>t.select1_change_handler.call(M)),h(T,"class","select"),h(N,"class","field"),h(S,"class","field-body"),h(_,"class","field is-horizontal"),h(p,"class","content"),P=[m(k,"change",t.select0_change_handler),m(k,"change",t.change_handler),m(M,"change",t.select1_change_handler),m(T,"change",t.fetchWorkflow)]},m(e,s){a(e,n,s),W&&W.m(n,null),r(n,l),H&&H.m(n,null),r(n,i),B&&B.m(n,null),a(e,d,s),a(e,p,s),r(p,g),r(p,v),r(p,_),r(_,w),r(_,j),r(_,y),r(y,O),r(O,x),r(x,k),r(k,$);for(let e=0;e<K.length;e+=1)K[e].m(k,null);b(k,t.config.wfid),r(_,C),r(_,A),r(_,L),r(_,S),r(S,N),r(N,T),r(T,M),r(M,R),V&&V.m(M,null),b(M,t.config.wfversion),r(p,F),J&&J.m(p,null)},p(e,t){if((e.Object||e.notif)&&(D=Object.values(t.notif.errors).some(Je)),D?W?W.p(e,t):((W=ne(t)).c(),W.m(n,l)):W&&(W.d(1),W=null),(e.Object||e.notif)&&(I=Object.values(t.notif.links).some(Ve)),I?H?H.p(e,t):((H=le(t)).c(),H.m(n,i)):H&&(H.d(1),H=null),(e.Object||e.notif)&&(U=Object.values(t.notif.messages).some(Ke)),U?B?B.p(e,t):((B=re(t)).c(),B.m(n,null)):B&&(B.d(1),B=null),e.wforder||e.allwfs){let n;for(Y=t.wforder,n=0;n<Y.length;n+=1){const s=G(t,Y,n);K[n]?K[n].p(e,s):(K[n]=oe(s),K[n].c(),K[n].m(k,null))}for(;n<K.length;n+=1)K[n].d(1);K.length=Y.length}e.config&&b(k,t.config.wfid),t.config.wfid?V?V.p(e,t):((V=ce(t)).c(),V.m(M,null)):V&&(V.d(1),V=null),e.config&&b(M,t.config.wfversion),t.wf?J?J.p(e,t):((J=ue(t)).c(),J.m(p,null)):J&&(J.d(1),J=null)},i:e,o:e,d(e){e&&o(n),W&&W.d(),H&&H.d(),B&&B.d(),e&&o(d),e&&o(p),c(K,e),V&&V.d(),J&&J.d(),s(P)}}}function Ye(e){return Object.entries(e).sort((e,t)=>e[0].replace("N","A")>t[0].replace("N","A")).map(e=>({ch:e[0],sample:e[1]}))}const Ke=e=>1===e,Ve=e=>1===e,Je=e=>1===e,Xe=e=>1==e[1],Ze=e=>e[0],Ge=e=>1==e[1],Qe=e=>e[0],et=e=>1==e[1],tt=e=>e[0];function nt(e,t,n){let s={errors:{},messages:{},links:{}},l=!0,i=!1,r={},a=!1,o=[],c={},f={},d=!1,u={wfid:!1,wfversion:!1,analysisname:"",flags:[],fileparams:{},v1:!1,v2:!1,version_dep:{v1:{instype:!1,dtype:!1}}},p={},m={};function h(e){let t=new Set;for(let n of e.files){const s=n.name.match(RegExp(m[e.id]));s?(n.fr=s[1],t.add(s[1])):n.fr="NA"}n("matchedFr",p[e.id]=t.size,p)}y(async()=>{n("frregex",m=Object.fromEntries(dsids.map(e=>[e,".*fr([0-9]+).*mzML$"]))),async function(){let e=new URL("/analysis/workflows",document.location);const t=await P(e);if("error"in t){const e=`While fetching workflows, encountered: ${t.error}`;n("notif",s.errors[e]=1,s),setTimeout(function(e){n("notif",s.errors[e]=0,s)},z,e)}else n("allwfs",r=t.allwfs),n("wforder",o=t.order)}()});const g=[[]];return{notif:s,runButtonActive:l,postingAnalysis:i,allwfs:r,wf:a,wforder:o,dsets:c,isoquants:f,mediansweep:d,config:u,matchedFr:p,frregex:m,runAnalysis:async function(){if(!function(){n("notif",s={errors:{},messages:{},links:{}});const e=RegExp("^[a-z0-9_-]+$","i");return u.analysisname?e.test(u.analysisname)||n("notif",s.errors["Analysisname may only contain a-z 0-9 _ -"]=1,s):n("notif",s.errors["Analysisname must be filled in"]=1,s),u.wfid||n("notif",s.errors["You must select a workflow"]=1,s),u.wfversion||n("notif",s.errors["You must select a workflow version"]=1,s),Object.values(c).forEach(t=>{!u.v1||"labelcheck"===u.version_dep.v1.dtype.toLowerCase()||t.filesaresets||t.setname?t.filesaresets?t.files.filter(e=>!e.setname).length&&n("notif",s.errors[`File ${fn.name} needs to have a setname`]=1,s):t.setname&&!e.test(t.setname)&&n("notif",s.errors[`Dataset ${t.proj} - ${t.exp} - ${t.run} needs to have another set name: only a-z 0-9 _ are allowed`]=1,s):n("notif",s.errors[`Dataset ${t.proj} - ${t.exp} - ${t.run} needs to have a set name`]=1,s)}),Object.entries(f).forEach(([t,l])=>{Object.entries(l.samplegroups).forEach(([l,i])=>{i&&!e.test(i)&&n("notif",s.errors[`Incorrect sample group name for set ${t}, channel ${l}, only A-Z a-z 0-9 _ are allowed`]=1,s)})}),0===Object.keys(s.errors).length}())return!1;n("runButtonActive",l=!1),n("postingAnalysis",i=!0),n("notif",s.messages["Validated data"]=1,s);let e=Object.fromEntries(Object.entries(u.fileparams).filter(([e,t])=>t));a.fixedfileparams.forEach(t=>{e[t.nf]=t.id});let t={};Object.values(c).filter(e=>e.filesaresets).forEach(e=>{Object.assign(t,Object.fromEntries(e.files.map(e=>[e.id,e.setname])))}),Object.values(c).filter(e=>!e.filesaresets).forEach(e=>{Object.assign(t,Object.fromEntries(e.files.map(t=>[t.id,e.setname])))});const o=Object.fromEntries(Object.values(c).flatMap(e=>e.files.map(e=>[e.id,e.fr])));n("notif",s.messages[`${Object.keys(t).length} set(s) found`]=1,s),n("notif",s.messages[`Using ${Object.keys(c).length} dataset(s)`]=1,s),s.messages[`${Object.keys(e).length} other inputfiles found`];let p={setnames:t,dsids:Object.keys(c),fractions:o,files:e,wfid:u.wfid,nfwfvid:u.wfversion.id,analysisname:`${r[u.wfid].wftype}_${u.analysisname}`,strips:{},params:{flags:u.flags}};u.v1&&(p.params.inst=["--instrument",u.version_dep.v1.instype],p.params.quant="labelfree"===u.version_dep.v1.qtype?[]:["--isobaric",u.version_dep.v1.qtype]),Object.values(c).forEach(e=>{p.strips[e.id]=e.hr?e.hr:!!e.prefrac&&"unknown_plate"});let m=Object.fromEntries(Object.entries(f).map(([e,t])=>[e,Object.entries(t.denoms).filter(([e,t])=>t).map(([e,t])=>e)]));(m=Object.entries(m).filter(([e,t])=>t.length>0).map(([e,t])=>`${e}:${t.join(":")}`).join(" ")).length>0&&!d&&(p.params.denoms=["--denoms",m]);let h=Object.entries(f).flatMap(([e,t])=>Object.entries(t.channels).map(([n,s])=>[n,e,s,t.samplegroups[n]]).sort((e,t)=>e[0].replace("N","A")>t[0].replace("N","A")));p.sampletable=h.map(e=>e.slice(0,3).concat(e[3]?e[3]:"X__POOL")),n("notif",s.messages[`Posting analysis job for ${this.analysisname}`]=1,s);const g=await async function(e,t){let n;try{n=await fetch(e,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(t)})}catch{return{ok:!1,error:"Kantele encountered a network error",status:!1}}return await F(n)}("/analysis/run/",p);g.error?(n("notif",s.errors[g.error]=1,s),"link"in g&&n("notif",s.links[g.link]=1,s)):window.location.href="/?tab=searches",n("postingAnalysis",i=!1),n("runButtonActive",l=!0)},fetchWorkflow:async function(){let e=new URL("/analysis/workflow",document.location);const t={dsids:dsids.join(","),wfvid:u.wfversion.id};e.search=new URLSearchParams(t).toString();const l=await P(e);if("error"in l){const e=`While fetching workflow versions, encountered: ${l.error}`;n("notif",s.errors[e]=1,s),setTimeout(function(e){n("notif",s.errors[e]=0,s)},z,e)}else n("wf",a=l.wf),n("config",u.v1=1===a.analysisapi,u),n("config",u.v2=2===a.analysisapi,u);!async function(){let e=new URL("/analysis/dsets/",document.location);const t={dsids:dsids.join(",")};e.search=new URLSearchParams(t).toString();const l=await P(e);if(l.error){const e=l.errmsg;n("notif",s.errors[e]=1,s),setTimeout(function(e){n("notif",s.errors[e]=0,s)},z,e)}else{n("dsets",c=l.dsets),Object.entries(c).filter(e=>e[1].prefrac).forEach(e=>h(c[e[0]])),Object.entries(c).forEach(e=>{e.filesaresets=!1,e.setname=""});const e=new Set(Object.values(c).map(e=>e.dtype.toLowerCase()));n("config",u.version_dep.v1.dtype=e.size>1?"mixed":e.keys().next().value,u);const t=new Set(Object.values(c).map(e=>e.details.qtypeshort));u.v1&&t.size>1?n("notif",s.errors["Mixed quant types detected, cannot use those in single run, use more advanced pipeline version"]=1,s):n("config",u.version_dep.v1.qtype=t.keys().next().value,u);const i=new Set(Object.values(c).flatMap(e=>e.details.instrument_types).map(e=>e.toLowerCase()));u.v1&&i.size>1?n("notif",s.errors["Mixed instrument types detected, cannot use those in single run, use more advanced pipeline version"]=1,s):n("config",u.version_dep.v1.instype=i.keys().next().value,u)}}()},matchFractions:h,updateIsoquant:function(){Object.values(c).forEach(e=>{e.setname in f||n("isoquants",f[e.setname]={channels:e.details.channels,samplegroups:Object.fromEntries(Object.keys(e.details.channels).map(e=>[e,""])),denoms:Object.fromEntries(Object.keys(e.details.channels).map(e=>[e,!1]))},f)});const e=new Set(Object.values(c).map(e=>e.setname));Object.keys(f).filter(t=>!e.has(t)).forEach(e=>{delete f[e]})},select0_change_handler:function(){u.wfid=_(this),n("config",u),n("wforder",o)},change_handler:e=>n("wf",a=n("config",u.wfversion=!1,u)),select1_change_handler:function(){u.wfversion=_(this),n("config",u),n("wforder",o)},input_input_handler:function(){u.analysisname=this.value,n("config",u),n("wforder",o)},input_change_handler:function({ds:e}){e.filesaresets=this.checked,n("Object",Object),n("dsets",c)},input_input_handler_1:function({ds:e}){e.setname=this.value,n("Object",Object),n("dsets",c)},input_input_handler_2:function({ds:e}){m[e.id]=this.value,n("frregex",m),n("Object",Object),n("dsets",c)},change_handler_1:({ds:e},t)=>h(e),input_input_handler_3:function({fn:e}){e.setname=this.value,n("Object",Object),n("dsets",c)},input_change_handler_1:function(){d=this.checked,n("mediansweep",d)},input_change_handler_2:function({isoq:e,ch:t}){e[1].denoms[t]=this.checked,n("Object",Object),n("isoquants",f),n("sortChannels",Ye)},input_input_handler_4:function({isoq:e,ch:t}){e[1].samplegroups[t]=this.value,n("Object",Object),n("isoquants",f),n("sortChannels",Ye)},input_change_handler_3:function(){u.flags=function(e){const t=[];for(let n=0;n<e.length;n+=1)e[n].checked&&t.push(e[n].__value);return t}(g[0]),n("config",u),n("wforder",o)},select_change_handler:function({filep:e}){u.fileparams[e.nf]=_(this),n("config",u),n("wf",a),n("wforder",o)},$$binding_groups:g}}return new class extends R{constructor(e){super(),M(this,e,nt,Be,i,{})}}({target:document.querySelector("#apps")})}();
//# sourceMappingURL=bundle.js.map
