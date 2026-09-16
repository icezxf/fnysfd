package dashboard

// loginHTML 登录页面（极简风格）
const loginHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>登录 - FNYSFD</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Helvetica Neue",Arial,sans-serif;background:#f4f4f5;display:flex;align-items:center;justify-content:center;color:#18181b}
.wrap{width:360px;background:#fff;padding:40px;border-radius:6px;box-shadow:0 1px 3px rgba(0,0,0,.08),0 4px 12px rgba(0,0,0,.04)}
.brand{text-align:center;margin-bottom:32px}
.brand h1{font-size:20px;font-weight:600;color:#18181b;letter-spacing:.5px}
.brand .sub{font-size:14px;color:#71717a;margin-top:6px}
.brand-divider{width:32px;height:1px;background:#2563eb;margin:12px auto}
.form-group{margin-bottom:20px}
.form-group input{width:100%;height:36px;border:none;border-bottom:1px solid #e4e4e7;background:transparent;font-size:14px;color:#18181b;font-family:inherit;padding:0 2px;transition:all .15s ease}
.form-group input:focus{outline:none;border-bottom-color:#2563eb;box-shadow:inset 0 -1px 0 #2563eb}
.form-group input::placeholder{color:#a1a1aa}
.btn{width:100%;height:36px;background:#2563eb;color:#fff;border:none;border-radius:4px;font-size:14px;font-weight:500;cursor:pointer;font-family:inherit;transition:all .15s ease}
.btn:hover{background:#1d4ed8;transform:translateY(-1px);box-shadow:0 2px 4px rgba(0,0,0,.06)}
.err{color:#dc2626;text-align:center;margin-top:12px;font-size:13px;display:none}
@media (max-width:768px){
.wrap{width:90%;padding:24px}
.brand{margin-bottom:24px}
}
</style>
</head>
<body>
<div class="wrap">
<div class="brand">
<h1>FNYSFD</h1>
<div class="sub">飞牛影视反代</div>
<div class="brand-divider"></div>
</div>
<form id="loginForm">
<div class="form-group">
<input type="text" id="username" autocomplete="username" placeholder="用户名">
</div>
<div class="form-group">
<input type="password" id="password" autocomplete="current-password" placeholder="密码">
</div>
<button type="submit" class="btn" id="loginBtn">登录</button>
</form>
<div class="err" id="errMsg"></div>
</div>
<script>
(function(){
var form=document.getElementById('loginForm');
function showErr(m){var e=document.getElementById('errMsg');e.textContent=m;e.style.display='block';setTimeout(function(){e.style.display='none'},3000)}
form.onsubmit=function(ev){
ev.preventDefault();
var u=document.getElementById('username').value.trim();
var p=document.getElementById('password').value;
if(!u||!p){showErr('请输入用户名和密码');return}
fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest'},body:JSON.stringify({username:u,password:p})})
.then(function(r){
if(!r.ok){showErr('登录请求失败（HTTP '+r.status+'）');return null}
return r.json();
})
.then(function(d){
if(!d)return;
if(d.code===200){location.href='/'}
else{showErr(d.message||'登录失败')}
})
.catch(function(){showErr('网络错误，请重试')})
}
})();
</script>
</body>
</html>`

// indexHTML 主界面
const indexHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FNYSFD 管理面板</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
--primary:#2563eb;
--primary-hover:#1d4ed8;
--success:#16a34a;
--warning:#ea580c;
--danger:#dc2626;
--purple:#9333ea;
--bg:#ffffff;
--sidebar-bg:#18181b;
--text:#18181b;
--text-muted:#71717a;
--text-light:#a1a1aa;
--border:#e4e4e7;
}
html,body{height:100%}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Helvetica Neue",Arial,sans-serif;background:var(--bg);color:var(--text);font-size:14px;line-height:1.5}

::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:#d4d4d8;border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:#a1a1aa}

.sidebar{position:fixed;left:0;top:0;bottom:0;width:200px;background:var(--sidebar-bg);display:flex;flex-direction:column;z-index:100}
.sidebar-logo{padding:20px 16px;color:#fff;font-size:16px;font-weight:600;letter-spacing:.5px;border-bottom:1px solid #27272a}
.sidebar-nav{flex:1;padding:8px 0}
.nav-item{display:block;padding:10px 16px;color:#a1a1aa;font-size:14px;cursor:pointer;user-select:none;border-left:2px solid transparent;transition:all .15s ease;margin:2px 0}
.nav-item:hover{color:#fff;background:rgba(255,255,255,.03)}
.nav-item.active{color:#fff;border-left-color:var(--primary);background:rgba(255,255,255,.05)}
.sidebar-footer{padding:8px 0}
.nav-item.danger:hover{color:#f87171}

.main{margin-left:200px;min-height:100vh}
.topbar{display:flex;align-items:center;justify-content:space-between;padding:24px 32px 0}
.topbar .title{font-size:20px;font-weight:600;color:var(--text)}
.topbar .right{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--text-muted)}
.status-dot{width:8px;height:8px;border-radius:50%;background:var(--success);display:inline-block;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.content{padding:24px 32px 32px}

.card{background:#fff;border:1px solid var(--border);border-radius:6px;padding:20px;margin-bottom:16px;transition:all .15s ease}
.card-title{font-size:16px;font-weight:600;color:var(--text);margin-bottom:16px}

.stats-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:16px;margin-bottom:16px}
.stat-card{background:#fff;border:1px solid var(--border);border-top:3px solid var(--border);border-radius:6px;padding:20px;transition:all .15s ease}
.stat-card:hover{border-color:#d4d4d8}
.stat-card.stat-blue{border-top-color:#2563eb}
.stat-card.stat-green{border-top-color:#16a34a}
.stat-card.stat-purple{border-top-color:#9333ea}
.stat-card.stat-orange{border-top-color:#ea580c}
.stat-card .num{font-size:28px;font-weight:700;color:#18181b;line-height:1.2;font-variant-numeric:tabular-nums}
.stat-card .lbl{font-size:13px;color:#71717a;margin-top:4px}

.sys-row{display:flex;align-items:center;height:40px;border-bottom:1px dashed var(--border)}
.sys-row:last-child{border-bottom:none}
.sys-row .k{font-size:13px;color:#71717a;width:140px;flex-shrink:0}
.sys-row .v{font-size:14px;color:#18181b;flex:1;word-break:break-all;font-variant-numeric:tabular-nums}
.card-actions{display:flex;justify-content:flex-end;margin-top:16px}

.form-group-title{font-size:15px;font-weight:600;color:var(--text);margin-bottom:16px;border-left:3px solid var(--primary);padding-left:8px}
.form-item{margin-bottom:16px}
.form-item>label{display:block;font-size:13px;color:#71717a;margin-bottom:6px}
.form-item>label.switch{display:inline-flex;margin-bottom:0}
.form-item input[type="text"],.form-item input[type="number"],.form-item input[type="password"],.form-item select{width:100%;height:32px;border:1px solid var(--border);border-radius:4px;padding:0 8px;font-size:14px;color:#18181b;background:#fff;font-family:inherit;transition:all .15s ease}
.form-item input[type="text"]:focus,.form-item input[type="number"]:focus,.form-item input[type="password"]:focus,.form-item select:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px rgba(37,99,235,.1)}
.form-item .hint{font-size:12px;color:#a1a1aa;margin-top:4px}
.form-actions{display:flex;justify-content:flex-end;margin-top:16px}
.form-actions .btn{border-radius:6px}

.btn{height:36px;padding:0 16px;border:none;border-radius:4px;font-size:14px;font-weight:500;cursor:pointer;font-family:inherit;transition:all .15s ease}
.btn-primary{background:var(--primary);color:#fff}
.btn-primary:hover{background:var(--primary-hover);transform:translateY(-1px);box-shadow:0 2px 4px rgba(0,0,0,.06)}
.btn-text{background:transparent;border:none;color:var(--text-muted);font-size:14px;cursor:pointer;padding:0;font-family:inherit;transition:all .15s ease}
.btn-text:hover{color:var(--text)}
.btn-text.danger{color:var(--danger)}
.btn-text.danger:hover{color:#b91c1c;font-weight:600}

.notice{background:#fff7ed;border:1px solid #fed7aa;border-radius:6px;padding:12px;font-size:13px;color:#9a3412;margin-bottom:16px;line-height:1.6}
.notice code{font-family:"SF Mono",Monaco,"Cascadia Code","Roboto Mono",Consolas,monospace;background:#fff;color:#9a3412;padding:1px 5px;border-radius:3px;border:1px solid #fed7aa}
.path-item{display:flex;align-items:center;justify-content:space-between;background:#fff;border:1px solid var(--border);border-radius:6px;padding:12px 16px;margin-bottom:8px;gap:12px;transition:all .15s ease}
.path-item:hover{background:#fafafa}
.path-item .paths{font-family:"SF Mono",Monaco,"Cascadia Code","Roboto Mono",Consolas,monospace;font-size:13px;color:var(--text);flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.path-item .paths .arrow{color:var(--text-muted);margin:0 8px}
.path-empty{text-align:center;padding:24px;color:var(--text-light);font-size:13px}
.add-form{display:flex;gap:12px;align-items:flex-end}
.add-form .form-item{flex:1;margin-bottom:0}

.log-toolbar{display:flex;align-items:center;gap:16px;margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid var(--border)}
.log-toolbar .spacer{flex:1}
.switch,.switch-list .switch{position:relative;display:inline-flex;align-items:center;gap:8px;font-size:13px;color:var(--text-muted);cursor:pointer;user-select:none;margin-bottom:0}
.switch input,.switch-list .switch input{position:absolute;opacity:0;width:0;height:0;margin:0}
.switch .track,.switch-list .switch .track{width:32px;height:18px;background:#d4d4d8;border-radius:9px;position:relative;transition:background .15s ease;flex-shrink:0}
.switch .track::after,.switch-list .switch .track::after{content:'';position:absolute;top:2px;left:2px;width:14px;height:14px;background:#fff;border-radius:50%;transition:transform .15s ease;box-shadow:0 1px 2px rgba(0,0,0,.2)}
.switch input:checked + .track,.switch-list .switch input:checked + .track{background:var(--primary)}
.switch input:checked + .track::after,.switch-list .switch input:checked + .track::after{transform:translateX(14px)}
.switch-list{display:flex;flex-direction:column;gap:10px;padding:4px 0}
.log-view{background:#0a0a0a;color:#e4e4e7;border-radius:6px;padding:16px;font-family:"SF Mono",Monaco,"Cascadia Code","Roboto Mono",Consolas,monospace;font-size:12px;line-height:1.6;max-height:calc(100vh - 280px);overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.log-view::-webkit-scrollbar-thumb{background:#3f3f46}
.log-view::-webkit-scrollbar-thumb:hover{background:#52525b}
.log-view .lv-info{color:#4ade80}
.log-view .lv-warn{color:#fbbf24}
.log-view .lv-error{color:#f87171}
.log-view .lv-debug{color:#a78bfa}
.log-view .lv-empty{color:#71717a}

/* 豆瓣缓存 */
.douban-toolbar{display:flex;align-items:center;gap:12px;margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.douban-toolbar input[type="text"]{height:32px;border:1px solid var(--border);border-radius:4px;padding:0 10px;font-size:13px;font-family:inherit;min-width:180px;flex:1;max-width:320px;transition:all .15s ease}
.douban-toolbar input[type="text"]:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px rgba(37,99,235,.1)}
.douban-toolbar .spacer{flex:1}
.douban-filters{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
.douban-chip{height:28px;padding:0 12px;border:1px solid var(--border);background:#fff;border-radius:14px;font-size:12px;color:#71717a;cursor:pointer;font-family:inherit;transition:all .15s ease}
.douban-chip:hover{border-color:var(--primary);color:var(--primary)}
.douban-chip.active{background:var(--primary);border-color:var(--primary);color:#fff}
.douban-table{width:100%;border-collapse:collapse;font-size:13px}
.douban-table thead th{text-align:left;padding:10px 8px;border-bottom:2px solid var(--border);color:#71717a;font-weight:600;font-size:12px;user-select:none;white-space:nowrap}
.douban-table thead th.sortable{cursor:pointer}
.douban-table thead th.sortable:hover{color:var(--text)}
.douban-table thead th .sort-arrow{color:#a1a1aa;margin-left:4px;font-size:10px}
.douban-table tbody tr{border-bottom:1px solid #f4f4f5;transition:background .1s ease}
.douban-table tbody tr:hover{background:#fafafa}
.douban-table tbody td{padding:9px 8px;vertical-align:middle}
.douban-table .col-title{max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.douban-table .col-rating{font-weight:600;color:#ea580c;font-variant-numeric:tabular-nums;white-space:nowrap}
.douban-table .col-type{color:#71717a;white-space:nowrap}
.douban-table .col-type .tag{display:inline-block;padding:1px 6px;border-radius:3px;font-size:11px;background:#f4f4f5;color:#52525b}
.douban-table .col-type .tag.season{background:#fef3c7;color:#92400e}
.douban-table .col-time{color:#a1a1aa;white-space:nowrap;font-variant-numeric:tabular-nums;font-size:12px}
.douban-table .col-action{text-align:right;width:60px}
.douban-pager{display:flex;align-items:center;justify-content:center;gap:12px;margin-top:16px;font-size:13px;color:#71717a}
.douban-pager button{height:30px;padding:0 12px;border:1px solid var(--border);background:#fff;border-radius:4px;cursor:pointer;font-family:inherit;font-size:13px;color:var(--text);transition:all .15s ease}
.douban-pager button:hover:not(:disabled){border-color:var(--primary);color:var(--primary)}
.douban-pager button:disabled{opacity:.4;cursor:not-allowed}

@media (max-width:768px){
.douban-table .col-time{display:none}
.douban-table .col-title{max-width:160px}
}

.tab-pane{display:none}
.tab-pane.active{display:block}

.toast-wrap{position:fixed;top:16px;right:16px;z-index:9999;display:flex;flex-direction:column;gap:8px;pointer-events:none}
.toast{background:#fff;border-left:4px solid var(--primary);padding:10px 16px;font-size:13px;color:var(--text);box-shadow:0 1px 3px rgba(0,0,0,.08),0 4px 12px rgba(0,0,0,.06);min-width:220px;max-width:340px;pointer-events:auto;border-radius:0 4px 4px 0}
.toast.success{border-left-color:var(--success)}
.toast.error{border-left-color:var(--danger)}
.toast.warning{border-left-color:var(--warning)}

.menu-toggle{display:none;flex-direction:column;justify-content:center;width:24px;height:24px;gap:5px;cursor:pointer;margin-right:8px;flex-shrink:0;padding:0;background:transparent;border:none}
.menu-toggle span{display:block;width:20px;height:2px;background:var(--text);border-radius:1px;transition:all .15s ease}
.sidebar-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:999}

@media (max-width:768px){
.menu-toggle{display:flex}
.sidebar{transform:translateX(-100%);transition:transform .2s ease;z-index:1000}
body.sidebar-open .sidebar{transform:translateX(0)}
body.sidebar-open .sidebar-overlay{display:block}
.main{margin-left:0}
.topbar{padding:16px 16px 0}
.topbar .title{font-size:18px}
.content{padding:16px}
.stats-grid{grid-template-columns:1fr;gap:12px}
.path-item{flex-direction:column;align-items:flex-start;gap:8px}
.path-item .paths{width:100%;white-space:normal;word-break:break-all}
.add-form{flex-direction:column;align-items:stretch;gap:12px}
.add-form .btn{width:100%}
.log-view{max-height:calc(100vh - 240px)}
.log-toolbar{flex-wrap:wrap;gap:12px}
}
</style>
</head>
<body>

<div class="sidebar-overlay" id="sidebarOverlay"></div>

<div class="sidebar" id="sidebar">
<div class="sidebar-logo">FNYSFD</div>
<div class="sidebar-nav">
<div class="nav-item active" data-tab="overview">概览</div>
<div class="nav-item" data-tab="config">配置</div>
<div class="nav-item" data-tab="douban">豆瓣缓存</div>
<div class="nav-item" data-tab="paths">路径管理</div>
<div class="nav-item" data-tab="logs">日志</div>
</div>
<div class="sidebar-footer">
<div class="nav-item danger" id="logoutBtn">退出登录</div>
</div>
</div>

<div class="main">
<div class="topbar">
<div class="menu-toggle" id="menuToggle"><span></span><span></span><span></span></div>
<span class="title" id="pageTitle">概览</span>
<div class="right">
<span class="status-dot"></span>
<span>运行中</span>
</div>
</div>

<div class="content">

<!-- 概览 -->
<div class="tab-pane active" id="pane-overview">
<div class="stats-grid">
<div class="stat-card stat-blue"><div class="num" id="statTotal">-</div><div class="lbl">缓存查找次数</div></div>
<div class="stat-card stat-green"><div class="num" id="statHitRate">-</div><div class="lbl">缓存命中率</div></div>
<div class="stat-card stat-purple"><div class="num" id="statPreload">-</div><div class="lbl">预加载成功数</div></div>
<div class="stat-card stat-orange"><div class="num" id="statMem">-</div><div class="lbl">内存使用</div></div>
</div>

<div class="card">
<div class="card-title">缓存详情</div>
<div class="sys-row"><div class="k">MediaSource 缓存</div><div class="v" id="statMedia">-</div></div>
<div class="sys-row"><div class="k">直链缓存</div><div class="v" id="statStream">-</div></div>
<div class="sys-row"><div class="k">STRM 缓存</div><div class="v" id="statStrm">-</div></div>
<div class="sys-row"><div class="k">URL 缓存</div><div class="v" id="statUrl">-</div></div>
<div class="sys-row"><div class="k">命中次数</div><div class="v" id="statHit">-</div></div>
<div class="sys-row"><div class="k">未命中次数</div><div class="v" id="statMiss">-</div></div>
<div class="sys-row"><div class="k">淘汰次数</div><div class="v" id="statEvict">-</div></div>
<div class="card-actions">
<button class="btn-text danger" id="clearCacheBtn">清空缓存</button>
</div>
</div>

<div class="card">
<div class="card-title">豆瓣评分</div>
<div class="sys-row"><div class="k">状态</div><div class="v" id="doubanStatus">-</div></div>
<div class="sys-row"><div class="k">缓存条目</div><div class="v" id="doubanEntries">-</div></div>
<div class="sys-row"><div class="k">缓存文件大小</div><div class="v" id="doubanFileSize">-</div></div>
<div class="sys-row"><div class="k">命中 / 未命中</div><div class="v" id="doubanHits">-</div></div>
<div class="sys-row"><div class="k">抓取成功 / 失败</div><div class="v" id="doubanFetched">-</div></div>
<div class="sys-row"><div class="k">最后更新</div><div class="v" id="doubanUpdated">-</div></div>
<div class="card-actions">
<button class="btn-text" id="viewDoubanCacheBtn">查看缓存清单 →</button>
</div>
</div>

<div class="card">
<div class="card-title">系统信息</div>
<div class="sys-row"><div class="k">版本</div><div class="v" id="sysVersion">-</div></div>
<div class="sys-row"><div class="k">Go 版本</div><div class="v" id="sysGo">-</div></div>
<div class="sys-row"><div class="k">运行时间</div><div class="v" id="sysUptime">-</div></div>
<div class="sys-row"><div class="k">Goroutine 数</div><div class="v" id="sysGoroutine">-</div></div>
<div class="sys-row"><div class="k">操作系统</div><div class="v" id="sysOS">-</div></div>
<div class="sys-row"><div class="k">CPU 核心数</div><div class="v" id="sysCPU">-</div></div>
<div class="sys-row"><div class="k">启动时间</div><div class="v" id="sysStartTime">-</div></div>
<div class="sys-row"><div class="k">架构</div><div class="v" id="sysArch">-</div></div>
</div>
</div>

<!-- 豆瓣缓存 -->
<div class="tab-pane" id="pane-douban">
<div class="card">
<div class="card-title">豆瓣缓存清单（<span id="doubanCacheCount">0</span> 条）</div>
<div class="douban-toolbar">
<input type="text" id="doubanSearchBox" placeholder="搜索名称...">
<div class="spacer"></div>
<button class="btn-text" id="refreshDoubanCacheBtn">刷新</button>
<button class="btn-text danger" id="clearDoubanCacheBtn">清空全部</button>
</div>
<div class="douban-filters">
<button type="button" class="douban-chip active" data-type="all">全部</button>
<button type="button" class="douban-chip" data-type="movie">电影</button>
<button type="button" class="douban-chip" data-type="series">剧集</button>
<button type="button" class="douban-chip" data-type="season">季</button>
<button type="button" class="douban-chip" data-type="unknown">未知</button>
</div>
<div id="doubanCacheEmpty" class="path-empty" style="display:none">暂无缓存</div>
<table class="douban-table" id="doubanCacheTable" style="display:none">
<thead>
<tr>
<th class="sortable" data-sort="title">名称<span class="sort-arrow"></span></th>
<th class="sortable" data-sort="rating">评分<span class="sort-arrow"></span></th>
<th>类型</th>
<th class="sortable" data-sort="fetched_at">抓取时间<span class="sort-arrow"></span></th>
<th class="col-action"></th>
</tr>
</thead>
<tbody id="doubanCacheTbody"></tbody>
</table>
<div class="douban-pager" id="doubanPager" style="display:none">
<button id="doubanPrevBtn">上一页</button>
<span id="doubanPageInfo">-</span>
<button id="doubanNextBtn">下一页</button>
</div>
</div>
</div>

<!-- 配置 -->
<div class="tab-pane" id="pane-config">
<div class="card">
<div class="form-group-title">基础配置</div>
<div class="form-item">
<label>监听地址 (listen)</label>
<input type="text" id="cfgListen" placeholder=":28005">
<div class="hint">反代服务监听的地址和端口</div>
</div>
<div class="form-item">
<label>目标服务地址 (target)</label>
<input type="text" id="cfgTarget" placeholder="http://127.0.0.1:8005">
<div class="hint">被反代的上游服务地址</div>
</div>
<div class="form-item">
<label>日志级别</label>
<select id="cfgLogLevel">
<option value="trace">Trace</option>
<option value="debug">Debug</option>
<option value="info">Info</option>
<option value="warn">Warn</option>
<option value="error">Error</option>
</select>
<div class="hint">日志输出级别，生产环境推荐 Info</div>
</div>
<div class="form-item">
<label>STRM 解析模式</label>
<select id="cfgStrmResolveMode">
<option value="auto">auto - 智能识别（推荐）</option>
<option value="passthrough">passthrough - 透传（全 302 strm 用）</option>
<option value="always">always - 始终解析（回退行为）</option>
</select>
<div class="hint">auto: 自动识别 CDN 直链跳过解析；passthrough: 所有 strm 直接给播放器（适合 openlist/litepan 等工具生成的 302 strm）；always: 强制 HTTP 解析。修改后立即生效，无需重启。</div>
</div>
<div class="form-item">
<label>功能开关</label>
<div class="switch-list">
<label class="switch"><input type="checkbox" id="cfgEnableLanStrm" checked><span class="track"></span><span>支持内网 strm 地址</span></label>
<label class="switch"><input type="checkbox" id="cfgEnablePreload" checked><span class="track"></span><span>启用预加载</span></label>
<label class="switch"><input type="checkbox" id="cfgEnableCDNWarmup" checked><span class="track"></span><span>CDN 预热（首播更快）</span></label>
<label class="switch"><input type="checkbox" id="cfgEnableSmartTTL" checked><span class="track"></span><span>智能签名 TTL 检测</span></label>
<label class="switch"><input type="checkbox" id="cfgEnableDoubanRating" checked><span class="track"></span><span>启用豆瓣评分（反代注入）</span></label>
</div>
<div class="hint">内网 strm：关闭后内网地址 strm 直接返回给播放器（适合 bridge 网络）；预加载：关闭后每次播放都需等待解析；CDN 预热：关闭后首次播放加载更慢但节省带宽；智能 TTL：根据 URL 签名有效期动态设置缓存，解决播放中 403 问题；豆瓣评分：关闭后不抓取也不注入豆瓣评分，已有缓存保留。</div>
</div>
<div class="form-item">
<label>缓存 TTL（分钟）</label>
<input type="number" id="cfgCacheTTL" min="1" placeholder="30">
<div class="hint">缓存条目存活时间（默认 30 分钟，配合智能签名 TTL 避免播放中 403）</div>
</div>
</div>

<!-- ✅ 飞牛账号 -->
<div class="card">
<div class="form-group-title">飞牛账号</div>
<div class="form-item">
<label>飞牛用户名</label>
<input type="text" id="cfgFnosUsername" placeholder="留空则不主动登录">
<div class="hint">服务启动时主动登录飞牛拿 Emby Token，避免依赖用户访问触发被动捕获。留空则回退到环境变量 FNOS_USERNAME。</div>
</div>
<div class="form-item">
<label>飞牛密码</label>
<input type="password" id="cfgFnosPassword" placeholder="未修改请留空">
<div class="hint">留空表示不修改。修改后需重启生效。留空时回退到环境变量 FNOS_PASSWORD。</div>
</div>
</div>

<div class="card">
<div class="form-group-title">缓存配置</div>
<div class="form-item">
<label>最大缓存条目数</label>
<input type="number" id="cfgMaxItems" min="100" placeholder="10000">
<div class="hint">超过此数量时按 LRU 策略淘汰</div>
</div>
</div>

<div class="card">
<div class="form-group-title">面板配置</div>
<div class="form-item">
<label>面板用户名</label>
<input type="text" id="cfgUser" placeholder="admin">
</div>
<div class="form-item">
<label>面板密码</label>
<input type="password" id="cfgPass" placeholder="未修改请留空">
<div class="hint">留空表示不修改当前密码</div>
</div>
</div>

<div class="card">
<div class="form-group-title">全库扫描预取</div>
<div class="form-item">
<label class="switch"><input type="checkbox" id="cfgEnableLibraryScan"><span class="track"></span><span>启用全库扫描</span></label>
<div class="hint">定时扫描整个媒体库，批量预取所有影片的 PlaybackInfo（首次播放无需等待）</div>
</div>
<div class="form-item">
<label>定时扫描时间</label>
<input type="text" id="cfgLibraryScanCron" placeholder="03:00" style="width:120px">
<div class="hint">24小时制 HH:MM 格式，如 03:00 表示每天凌晨3点扫描（留空则不定时扫描）</div>
</div>
<div class="form-item">
<label class="switch"><input type="checkbox" id="cfgLibraryScanOnStart"><span class="track"></span><span>启动后立即扫描</span></label>
<div class="hint">服务启动后立即执行一次全库扫描（需要认证信息就绪，即用户已访问过飞牛）</div>
</div>
<div class="form-item">
<label>扫描并发数</label>
<input type="number" id="cfgLibraryScanConcurrency" min="1" max="20" placeholder="2">
<div class="hint">同时预取的影片数量（建议 1-4，过高可能卡死容器）</div>
</div>
<div class="form-item">
<label>每页间隔（毫秒）</label>
<input type="number" id="cfgLibraryScanIntervalMs" min="0" max="60000" placeholder="500">
<div class="hint">每页查询之间的等待时间，避免请求过密（建议 300-1000ms）</div>
</div>
<div class="form-item">
<label>增量扫描间隔（分钟）</label>
<input type="number" id="cfgLibraryScanIncrementalMinutes" min="1" max="1440" placeholder="5">
<div class="hint">每 N 分钟拉一次每个库的"最新 200 项"，只预取飞牛未 probe 的项（1-1440，建议 5-30）。修改后需重启生效。</div>
</div>
<div class="form-item">
<label>手动扫描</label>
<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
<button class="btn btn-primary" id="triggerScanBtn">立即扫描</button>
<span id="scanStatus" style="font-size:13px;color:#888">未扫描</span>
</div>
<div class="hint">手动触发一次全库扫描。扫描需要认证信息（用户已访问过飞牛网页端）</div>
</div>
</div>

<div class="form-actions">
<button class="btn btn-primary" id="saveConfigBtn">保存配置</button>
</div>
</div>

<!-- 路径管理 -->
<div class="tab-pane" id="pane-paths">
<div class="notice" id="pathNotice">
💡 如果宿主机目录已通过上层挂载点映射到容器内，添加后会<strong>立即生效</strong>，无需重启。
如果路径在容器内暂不可访问，请点击<strong>"重启容器"</strong>按钮应用配置。
</div>

<div style="margin-bottom:16px">
<button class="btn btn-warning" id="restartContainerBtn">🔄 重启容器（应用新Volume）</button>
</div>

<div class="card">
<div class="card-title">当前 Volume 映射（<span id="pathCount">0</span> 个）</div>
<div id="pathList"></div>
<div class="path-empty" id="pathEmpty">暂无 Volume 映射，请在下方添加</div>
</div>

<div class="card">
<div class="card-title">添加 Volume 映射</div>
<div class="add-form">
<div class="form-item">
<label>主机路径</label>
<input type="text" id="hostPath" placeholder="/vol1/1000/Media">
</div>
<div class="form-item">
<label>容器路径</label>
<input type="text" id="containerPath" placeholder="/vol1/1000/Media">
</div>
<button class="btn btn-primary" id="addVolumeBtn">添加</button>
</div>
</div>
</div>

<!-- 日志 -->
<div class="tab-pane" id="pane-logs">
<div class="card">
<div class="card-title">实时日志</div>
<div class="log-toolbar">
<button class="btn-text" id="refreshLogBtn">刷新</button>
<button class="btn-text" id="clearLogViewBtn">清空显示</button>
<label class="switch">
<input type="checkbox" id="autoRefreshLog" checked>
<span class="track"></span>
<span>自动刷新（5秒）</span>
</label>
<div class="spacer"></div>
<button class="btn-text danger" id="clearLogFileBtn">删除日志文件</button>
</div>
<div class="log-view" id="logView"><span class="lv-empty">加载中...</span></div>
</div>
</div>

</div>
</div>

<div class="toast-wrap" id="toastWrap"></div>

<script>
(function(){
'use strict';

/* ===== 状态 ===== */
var currentTab='overview';
var logAutoRefresh=true;
var logTimer=null;
var statsTimer=null;

/* 豆瓣缓存 */
var doubanAllItems=[];
var doubanFiltered=[];
var doubanSortKey='fetched_at';
var doubanSortAsc=false;
var doubanPage=1;
var doubanPageSize=100;
var doubanFilterType='all';

/* ===== 工具函数 ===== */
function $(id){return document.getElementById(id)}

function esc(s){
if(s==null)return '';
var d=document.createElement('div');
d.appendChild(document.createTextNode(String(s)));
return d.innerHTML.replace(/"/g,'&quot;').replace(/'/g,'&#39;')
}

function toast(msg,type){
var wrap=$('toastWrap');
var el=document.createElement('div');
el.className='toast '+(type||'success');
el.textContent=msg;
wrap.appendChild(el);
var dur=3000;
if(type==='error'||type==='warning')dur=5000;
setTimeout(function(){
if(el.parentNode)el.parentNode.removeChild(el);
},dur);
}

function ajax(url,method,data,cb){
var opt={method:method||'GET',headers:{'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest'}};
if(data!==null&&data!==undefined)opt.body=JSON.stringify(data);
fetch(url,opt)
.then(function(r){
if(!r.ok){cb('HTTP '+r.status,null);return null}
return r.json();
})
.then(function(d){
if(d==null)return;
if(d.code===401){location.href='/login';return}
cb(null,d);
})
.catch(function(e){cb(e.message||'网络错误',null)})
}

function fmtUptime(sec){
if(!sec||sec<0)return '-';
var d=Math.floor(sec/86400);
var h=Math.floor((sec%86400)/3600);
var m=Math.floor((sec%3600)/60);
var s=Math.floor(sec%60);
if(d>0)return d+'天'+h+'时'+m+'分';
if(h>0)return h+'时'+m+'分'+s+'秒';
if(m>0)return m+'分'+s+'秒';
return s+'秒';
}

function fmtNum(n){
if(n==null||isNaN(n))return '-';
if(n>=1000000)return (n/1000000).toFixed(2)+'M';
if(n>=1000)return (n/1000).toFixed(1)+'K';
return String(n);
}

/* ===== Tab 切换 ===== */
function switchTab(name){
var items=document.querySelectorAll('.nav-item[data-tab]');
var panes=document.querySelectorAll('.tab-pane');
for(var i=0;i<panes.length;i++)panes[i].classList.remove('active');
for(var j=0;j<items.length;j++)items[j].classList.remove('active');
var pane=$('pane-'+name);
if(pane)pane.classList.add('active');
for(var k=0;k<items.length;k++){
if(items[k].getAttribute('data-tab')===name){items[k].classList.add('active');break}
}
currentTab=name;
var titles={overview:'概览',config:'配置',douban:'豆瓣缓存',paths:'路径管理',logs:'日志'};
$('pageTitle').textContent=titles[name]||'';
if(name==='logs'){loadLogs();startLogTimer()}
else{stopLogTimer()}
if(name==='overview'){loadStats();loadSystem();startStatsTimer()}
else{stopStatsTimer()}
if(name==='config'){loadConfig()}
if(name==='paths'){loadPaths()}
if(name==='douban'){loadDoubanCache()}
}

function startLogTimer(){
stopLogTimer();
if(logAutoRefresh){logTimer=setInterval(loadLogs,5000)}
}
function stopLogTimer(){
if(logTimer){clearInterval(logTimer);logTimer=null}
}

function startStatsTimer(){
stopStatsTimer();
statsTimer=setInterval(function(){loadStats();loadSystem()},30000);
}
function stopStatsTimer(){
if(statsTimer){clearInterval(statsTimer);statsTimer=null}
}

/* ===== 概览 ===== */
function loadStats(){
ajax('/api/stats','GET',null,function(err,d){
if(err||!d||d.code!==200||!d.data)return;
var s=d.data;
$('statTotal').textContent=fmtNum((s.hit_count||0)+(s.miss_count||0));
$('statHitRate').textContent=(s.hit_rate||0).toFixed(1)+'%';
$('statPreload').textContent=fmtNum(s.preload_success!=null?s.preload_success:((s.strm_cache_count||0)+(s.url_cache_count||0)));
$('statMem').textContent=(s.memory_usage_mb||0).toFixed(1)+' MB';
$('statMedia').textContent=fmtNum(s.media_source_count||0);
$('statStream').textContent=fmtNum(s.stream_url_count||0);
$('statStrm').textContent=fmtNum(s.strm_cache_count||0);
$('statUrl').textContent=fmtNum(s.url_cache_count||0);
$('statHit').textContent=fmtNum(s.hit_count||0);
$('statMiss').textContent=fmtNum(s.miss_count||0);
$('statEvict').textContent=fmtNum(s.evicted_count||0);
if(s.douban_enabled!==undefined){
var de=s.douban_enabled;
var st=$('doubanStatus');
if(st){
st.textContent=de?'已启用':'已禁用';
st.style.color=de?'#16a34a':'#dc2626';
}
$('doubanEntries').textContent=fmtNum(s.douban_entries||0);
$('doubanFileSize').textContent=(s.douban_file_size_kb||0)+' KB';
$('doubanHits').textContent=fmtNum(s.douban_stat_hits||0)+' / '+fmtNum(s.douban_stat_misses||0);
$('doubanFetched').textContent=fmtNum(s.douban_stat_fetched||0)+' / '+fmtNum(s.douban_stat_failed||0);
$('doubanUpdated').textContent=s.douban_updated_at||'-';
}
});
}

function loadSystem(){
ajax('/api/system','GET',null,function(err,d){
if(err||!d||d.code!==200||!d.data)return;
var s=d.data;
$('sysVersion').textContent=s.version||'-';
$('sysGo').textContent=s.go_version||'-';
$('sysUptime').textContent=fmtUptime(s.uptime_seconds);
$('sysGoroutine').textContent=s.goroutines||'-';
$('sysOS').textContent=s.os||'-';
$('sysCPU').textContent=s.num_cpu||'-';
$('sysStartTime').textContent=s.start_time||'-';
$('sysArch').textContent=s.arch||'-';
});
}

/* ===== 豆瓣缓存 ===== */
function loadDoubanCache(){
ajax('/api/douban/cache','GET',null,function(err,d){
if(err){toast('加载豆瓣缓存失败: '+err,'error');return}
if(!d||d.code!==200){toast('加载豆瓣缓存失败','error');return}
doubanAllItems=Array.isArray(d.data)?d.data:[];
doubanPage=1;
applyDoubanFilterAndRender();
});
}

function applyDoubanFilterAndRender(){
var q=($('doubanSearchBox').value||'').trim().toLowerCase();
doubanFiltered=doubanAllItems.filter(function(it){
if(doubanFilterType!=='all'){
var t=it.type||'unknown';
if(t!==doubanFilterType)return false;
}
if(!q)return true;
var title=(it.title||'').toLowerCase();
return title.indexOf(q)>-1;
});

var key=doubanSortKey;
var asc=doubanSortAsc;
doubanFiltered.sort(function(a,b){
var va,vb;
if(key==='rating'){
va=Number(a.rating)||0;
vb=Number(b.rating)||0;
}else{
va=String(a[key]||'');
vb=String(b[key]||'');
}
if(va<vb)return asc?-1:1;
if(va>vb)return asc?1:-1;
return 0;
});

var ths=document.querySelectorAll('#doubanCacheTable thead th.sortable');
for(var i=0;i<ths.length;i++){
var th=ths[i];
var k=th.getAttribute('data-sort');
var arrow=th.querySelector('.sort-arrow');
if(!arrow)continue;
if(k===doubanSortKey){
arrow.textContent=doubanSortAsc?'▲':'▼';
}else{
arrow.textContent='';
}
}

renderDoubanPage();
}

function renderDoubanPage(){
var tbody=$('doubanCacheTbody');
var table=$('doubanCacheTable');
var empty=$('doubanCacheEmpty');
var pager=$('doubanPager');
$('doubanCacheCount').textContent=doubanFiltered.length;

if(!doubanFiltered.length){
table.style.display='none';
pager.style.display='none';
empty.style.display='block';
empty.textContent=doubanAllItems.length?'无匹配结果':'暂无缓存';
return;
}
empty.style.display='none';
table.style.display='';
pager.style.display='';

var totalPages=Math.max(1,Math.ceil(doubanFiltered.length/doubanPageSize));
if(doubanPage>totalPages)doubanPage=totalPages;
if(doubanPage<1)doubanPage=1;

var start=(doubanPage-1)*doubanPageSize;
var end=Math.min(start+doubanPageSize,doubanFiltered.length);

var h='';
for(var i=start;i<end;i++){
var it=doubanFiltered[i];
var typeLabel, typeCls;
var t=it.type||'unknown';
if(t==='season'){
typeLabel='第'+it.season_num+'季';
typeCls='tag season';
}else if(t==='movie'){
typeLabel='电影';
typeCls='tag';
}else if(t==='series'){
typeLabel='剧集';
typeCls='tag';
}else{
typeLabel='未知';
typeCls='tag';
}
var ratingText=(it.rating!=null)?Number(it.rating).toFixed(1):'-';
var allKeysJson=esc(JSON.stringify(it.all_keys||[it.key]));

h+='<tr>';
h+='<td class="col-title" title="'+esc(it.title||it.key)+'">'+esc(it.title||'(无名称)')+'</td>';
h+='<td class="col-rating">⭐ '+ratingText+'</td>';
h+='<td class="col-type"><span class="'+typeCls+'">'+esc(typeLabel)+'</span></td>';
h+='<td class="col-time">'+esc(it.fetched_at||'-')+'</td>';
h+='<td class="col-action"><button type="button" class="btn-text danger douban-del-btn" data-keys="'+allKeysJson+'">删除</button></td>';
h+='</tr>';
}
tbody.innerHTML=h;

$('doubanPageInfo').textContent='第 '+doubanPage+' / '+totalPages+' 页';
$('doubanPrevBtn').disabled=(doubanPage<=1);
$('doubanNextBtn').disabled=(doubanPage>=totalPages);

var btns=tbody.querySelectorAll('button.douban-del-btn');
for(var j=0;j<btns.length;j++){
(function(b){
b.onclick=function(){
var raw=b.getAttribute('data-keys');
var keys=[];
try{keys=JSON.parse(raw)}catch(e){keys=[]}
if(!keys.length)return;
deleteDoubanCache(keys);
};
})(btns[j]);
}
}

function deleteDoubanCache(keys){
if(!confirm('确定要删除这条豆瓣缓存吗？\n\n删除后下次扫描时会重新抓取。'))return;
ajax('/api/douban/cache/delete','POST',{keys:keys},function(err,d){
if(err){toast('删除失败: '+err,'error');return}
if(d&&d.code===200){
toast(d.message||'已删除','success');
loadDoubanCache();
loadStats();
}else{
toast((d&&d.message)||'删除失败','error');
}
});
}

function clearDoubanCache(){
if(!confirm('确定要清空全部豆瓣缓存吗？\n\n清空后所有评分会在下次扫描时重新抓取，这可能需要较长时间。'))return;
ajax('/api/douban/cache/clear','POST',null,function(err,d){
if(err){toast('清空失败: '+err,'error');return}
if(d&&d.code===200){
toast(d.message||'已清空','success');
loadDoubanCache();
loadStats();
}else{
toast((d&&d.message)||'清空失败','error');
}
});
}

/* ===== 配置 ===== */
function loadConfig(){
ajax('/api/config/get','GET',null,function(err,d){
if(err||!d||d.code!==200||!d.data)return;
var data=d.data;
$('cfgListen').value=data.listen||'';
$('cfgTarget').value=data.target||'';
$('cfgLogLevel').value=data.log_level||'info';
$('cfgStrmResolveMode').value=data.strm_resolve_mode||'auto';
$('cfgCacheTTL').value=data.cache_ttl||'';
$('cfgMaxItems').value=data.max_cache_items||10000;
$('cfgUser').value=data.dashboard_user||'admin';
$('cfgPass').value='';
// ✅ 飞牛账号
$('cfgFnosUsername').value=data.fnos_username||'';
$('cfgFnosPassword').value='';

$('cfgEnableLanStrm').checked = data.enable_lan_strm !== false;
$('cfgEnablePreload').checked = data.enable_preload !== false;
$('cfgEnableCDNWarmup').checked = data.enable_cdn_warmup !== false;
$('cfgEnableSmartTTL').checked = data.enable_smart_ttl !== false;
$('cfgEnableDoubanRating').checked = data.enable_douban_rating !== false;
$('cfgEnableLibraryScan').checked = data.enable_library_scan === true;
$('cfgLibraryScanCron').value = data.library_scan_cron || '';
$('cfgLibraryScanOnStart').checked = data.library_scan_on_start === true;
$('cfgLibraryScanConcurrency').value = data.library_scan_concurrency || 2;
$('cfgLibraryScanIntervalMs').value = data.library_scan_interval_ms != null ? data.library_scan_interval_ms : 500;
$('cfgLibraryScanIncrementalMinutes').value = data.library_scan_incremental_minutes || 5;
loadScanStatus();
});
}

function saveConfig(){
var pass=$('cfgPass').value;
var fnosPass=$('cfgFnosPassword').value;
var ttlInput=$('cfgCacheTTL').value.trim();
if(ttlInput===''){toast('缓存 TTL 不能为空','warning');return}
var ttlVal=parseInt(ttlInput,10);
if(isNaN(ttlVal)||ttlVal<1){toast('缓存 TTL 必须是大于 0 的整数','warning');return}
var maxItemsVal=parseInt($('cfgMaxItems').value.trim(),10)||10000;
var data={
listen:$('cfgListen').value.trim(),
target:$('cfgTarget').value.trim(),
log_level:$('cfgLogLevel').value,
strm_resolve_mode:$('cfgStrmResolveMode').value,
cache_ttl:ttlVal,
dashboard_user:$('cfgUser').value.trim(),
max_cache_items:maxItemsVal,
enable_lan_strm:$('cfgEnableLanStrm').checked,
enable_preload:$('cfgEnablePreload').checked,
enable_cdn_warmup:$('cfgEnableCDNWarmup').checked,
enable_smart_ttl:$('cfgEnableSmartTTL').checked,
enable_douban_rating:$('cfgEnableDoubanRating').checked,
enable_library_scan:$('cfgEnableLibraryScan').checked,
library_scan_cron:$('cfgLibraryScanCron').value.trim(),
library_scan_on_start:$('cfgLibraryScanOnStart').checked,
library_scan_concurrency:parseInt($('cfgLibraryScanConcurrency').value.trim(),10)||2,
library_scan_interval_ms:parseInt($('cfgLibraryScanIntervalMs').value.trim(),10)||500,
library_scan_incremental_minutes:parseInt($('cfgLibraryScanIncrementalMinutes').value.trim(),10)||5,
// ✅ 飞牛账号
fnos_username:$('cfgFnosUsername').value.trim()
};
if(pass&&pass!==''&&pass!=='****'){data.dashboard_pass=pass}
// 飞牛密码：仅在输入非空且非占位符时提交
if(fnosPass&&fnosPass!==''&&fnosPass!=='****'){data.fnos_password=fnosPass}
ajax('/api/config/update','POST',data,function(err,r){
if(err){toast('保存失败: '+err,'error');return}
if(r&&r.code===200){
toast(r.message||'配置已保存','success');
$('cfgPass').value='';
$('cfgFnosPassword').value='';
if(r.data&&r.data.needs_restart){
toast(r.data.message||'检测到需要重启的配置变更，请重启服务生效','warning');
}
setTimeout(loadConfig,300);
}else{
toast((r&&r.message)||'保存失败','error');
}
});
}

/* ===== 全库扫描 ===== */
function triggerScan(){
var btn=$('triggerScanBtn');
btn.disabled=true;
btn.textContent='扫描中...';
ajax('/api/scan/trigger','POST',null,function(err,r){
if(err){toast('扫描触发失败: '+err,'error');btn.disabled=false;btn.textContent='立即扫描';return}
if(r&&r.code===200){
toast(r.message||'扫描已启动','success');
pollScanStatus();
}else{
toast((r&&r.message)||'扫描触发失败','error');
btn.disabled=false;
btn.textContent='立即扫描';
}
});
}

var scanStatusTimer=null;
function loadScanStatus(){
ajax('/api/scan/status','GET',null,function(err,d){
if(err||!d||d.code!==200||!d.data){updateScanStatusUI(null);return}
updateScanStatusUI(d.data);
if(scanStatusTimer&&!d.data.running){
clearInterval(scanStatusTimer);scanStatusTimer=null;
var btn=$('triggerScanBtn');
if(btn){btn.disabled=false;btn.textContent='立即扫描'}
}
});
}

function pollScanStatus(){
if(scanStatusTimer){clearInterval(scanStatusTimer)}
loadScanStatus();
scanStatusTimer=setInterval(loadScanStatus,3000);
}

function updateScanStatusUI(data){
var el=$('scanStatus');
var btn=$('triggerScanBtn');
if(!el)return;
if(!data){
el.textContent='未扫描';
if(btn){btn.disabled=false;btn.textContent='立即扫描'}
return;
}
if(data.running){
el.textContent='扫描中...';
el.style.color='#2196f3';
if(btn){btn.disabled=true;btn.textContent='扫描中...'}
}else{
var stats=data.lastScanStats||{};
var time=data.lastScanTime||'';
if(time){
el.textContent='上次扫描: '+time+' | 成功 '+stats.success+' 跳过 '+stats.skipped+' 失败 '+stats.failed;
}else{
el.textContent='未扫描';
}
el.style.color='#888';
if(btn){btn.disabled=false;btn.textContent='立即扫描'}
}
}

/* ===== 路径管理 ===== */
function loadPaths(){
ajax('/api/strm_paths/get','GET',null,function(err,d){
if(err||!d||d.code!==200||!d.data)return;
var vols=d.data.volumes||[];
if(!vols.length&&d.data.strm_volumes){
for(var i=0;i<d.data.strm_volumes.length;i++){
var parts=d.data.strm_volumes[i].split(':');
vols.push({host_path:parts[0]||'',container_path:parts[1]||'',volume_format:d.data.strm_volumes[i]});
}
}
renderPaths(vols);
});
}

function renderPaths(volumes){
var list=$('pathList');
var empty=$('pathEmpty');
$('pathCount').textContent=volumes.length;
if(!volumes.length){list.innerHTML='';empty.style.display='block';return}
empty.style.display='none';
var h='';
for(var i=0;i<volumes.length;i++){
var vol=volumes[i];
var hostPath=vol.host_path||'';
var containerPath=vol.container_path||'';
var volFormat=vol.volume_format||(hostPath+':'+containerPath+':ro');
h+='<div class="path-item">';
h+='<div class="paths">'+esc(hostPath)+' <span class="arrow">→</span> '+esc(containerPath)+'</div>';
h+='<button type="button" class="btn-text danger" data-vol="'+esc(volFormat)+'">删除</button>';
h+='</div>';
}
list.innerHTML=h;
var btns=list.querySelectorAll('button[data-vol]');
for(var j=0;j<btns.length;j++){
(function(b){
b.onclick=function(){deleteVolume(b.getAttribute('data-vol'))};
})(btns[j]);
}
}

function addVolume(){
var hostInput=$('hostPath');
var containerInput=$('containerPath');
var hostPath=hostInput.value.trim();
var containerPath=containerInput.value.trim();
if(!hostPath){toast('请输入主机路径','warning');hostInput.focus();return}
if(!containerPath){toast('请输入容器路径','warning');containerInput.focus();return}
if(hostPath.charAt(0)!=='/'){toast('主机路径必须以 / 开头','warning');hostInput.focus();return}
if(containerPath.charAt(0)!=='/'){toast('容器路径必须以 / 开头','warning');containerInput.focus();return}
if(hostPath.indexOf('..')!==-1){toast('主机路径不允许包含 ..','warning');return}
if(containerPath.indexOf('..')!==-1){toast('容器路径不允许包含 ..','warning');return}
ajax('/api/strm_paths/add','POST',{host_path:hostPath,container_path:containerPath},function(err,d){
if(err){toast('添加失败: '+err,'error');return}
if(d&&d.code===200){
var availableNow=d.data&&d.data.available_now;
if(availableNow){
toast(d.message||'Volume 已添加并立即生效','success');
}else{
toast(d.message||'Volume 已添加，建议重启容器生效','warning');
}
if(d.data&&d.data.compose_updated===false){
toast('docker-compose.yml 未自动更新，请手动修改','warning');
}
hostInput.value='';
containerInput.value='';
loadPaths();
}else{
toast((d&&d.message)||'添加失败','error');
}
});
}

function deleteVolume(vol){
if(!confirm('确定要删除该 Volume 映射吗？\n\n'+vol+'\n\n删除后建议重启容器以完全释放挂载。'))return;
ajax('/api/strm_paths/delete','POST',{volume_format:vol},function(err,d){
if(err){toast('删除失败: '+err,'error');return}
if(d&&d.code===200){
toast(d.message||'已删除','success');
loadPaths();
}else{
toast((d&&d.message)||'删除失败','error');
}
});
}

function restartContainer(){
if(!confirm('确定要重启容器吗？\n\n重启过程约需 5-10 秒，期间服务会短暂不可用。'))return;
var btn=$('restartContainerBtn');
btn.disabled=true;
btn.textContent='🔄 正在重启...';
ajax('/api/docker/restart','POST',null,function(err,d){
btn.disabled=false;
btn.textContent='🔄 重启容器（应用新Volume）';
if(err){toast('重启失败: '+err,'error');return}
if(d&&d.code===200){
toast(d.message||'容器重启命令已发送','success');
}else{
toast((d&&d.message)||'重启失败','error');
}
});
}

/* ===== 日志 ===== */
function loadLogs(){
var el=$('logView');
ajax('/api/logs','GET',null,function(err,d){
if(err){el.innerHTML='<span class="lv-empty">加载失败: '+esc(err)+'</span>';return}
if(!d||d.code!==200||!d.logs||!d.logs.length){
el.innerHTML='<span class="lv-empty">暂无日志</span>';
return
}
var h='';
for(var i=0;i<d.logs.length;i++){
var l=d.logs[i];
var cls='lv-info';
if(l.indexOf('[ERROR]')>-1||l.indexOf('[错误]')>-1||l.indexOf('ERROR')>-1)cls='lv-error';
else if(l.indexOf('[WARN]')>-1||l.indexOf('[警告]')>-1||l.indexOf('WARN')>-1)cls='lv-warn';
else if(l.indexOf('[DEBUG]')>-1||l.indexOf('[调试]')>-1||l.indexOf('DEBUG')>-1)cls='lv-debug';
h+='<div class="'+cls+'">'+esc(l)+'</div>';
}
el.innerHTML=h;
el.scrollTop=el.scrollHeight;
});
}

function clearLogView(){
$('logView').innerHTML='<span class="lv-empty">已清空显示（日志文件未删除）</span>';
}

function clearLogs(){
if(!confirm('确定删除所有日志文件？此操作不可恢复！'))return;
ajax('/api/logs/clear','POST',null,function(err,d){
if(err){toast('操作失败: '+err,'error');return}
if(d&&d.code===200){
toast(d.message||'日志已清除','success');
loadLogs();
}else{
toast((d&&d.message)||'操作失败','error');
}
});
}

function clearCache(){
if(!confirm('确定清空所有缓存？此操作不可恢复！'))return;
ajax('/api/cache/clear','POST',null,function(err,d){
if(err){toast('操作失败: '+err,'error');return}
if(d&&d.code===200){
toast(d.message||'缓存已清空','success');
loadStats();
}else{
toast((d&&d.message)||'操作失败','error');
}
});
}

/* ===== 退出登录 ===== */
function logout(){
if(!confirm('确定要退出登录吗？'))return;
stopStatsTimer();
stopLogTimer();
fetch('/api/logout',{method:'POST',headers:{'X-Requested-With':'XMLHttpRequest'}}).then(function(){
location.href='/login';
}).catch(function(){location.href='/login'});
}

/* ===== 事件绑定 ===== */
function bindEvents(){
var navItems=document.querySelectorAll('.nav-item[data-tab]');
for(var i=0;i<navItems.length;i++){
navItems[i].onclick=function(){
switchTab(this.getAttribute('data-tab'));
if(window.innerWidth<=768){document.body.classList.remove('sidebar-open')}
};
}
$('logoutBtn').onclick=logout;
$('saveConfigBtn').onclick=saveConfig;
$('addVolumeBtn').onclick=addVolume;
$('triggerScanBtn').onclick=triggerScan;
$('restartContainerBtn').onclick=restartContainer;
$('clearCacheBtn').onclick=clearCache;
$('refreshLogBtn').onclick=loadLogs;
$('clearLogViewBtn').onclick=clearLogView;
$('clearLogFileBtn').onclick=clearLogs;
$('autoRefreshLog').onchange=function(){
logAutoRefresh=this.checked;
if(currentTab==='logs'&&logAutoRefresh)startLogTimer();
else stopLogTimer();
};

// 豆瓣缓存
$('viewDoubanCacheBtn').onclick=function(){switchTab('douban')};
$('refreshDoubanCacheBtn').onclick=loadDoubanCache;
$('clearDoubanCacheBtn').onclick=clearDoubanCache;
$('doubanSearchBox').oninput=function(){doubanPage=1;applyDoubanFilterAndRender()};
$('doubanPrevBtn').onclick=function(){if(doubanPage>1){doubanPage--;renderDoubanPage()}};
$('doubanNextBtn').onclick=function(){
var totalPages=Math.max(1,Math.ceil(doubanFiltered.length/doubanPageSize));
if(doubanPage<totalPages){doubanPage++;renderDoubanPage()}
};

var sortThs=document.querySelectorAll('#doubanCacheTable thead th.sortable');
for(var s=0;s<sortThs.length;s++){
(function(th){
th.onclick=function(){
var k=th.getAttribute('data-sort');
if(doubanSortKey===k){
doubanSortAsc=!doubanSortAsc;
}else{
doubanSortKey=k;
doubanSortAsc=false;
}
applyDoubanFilterAndRender();
};
})(sortThs[s]);
}

// ✅ 类型筛选 chip
var chips=document.querySelectorAll('.douban-chip');
for(var c=0;c<chips.length;c++){
(function(chip){
chip.onclick=function(){
doubanFilterType=chip.getAttribute('data-type')||'all';
doubanPage=1;
var all=document.querySelectorAll('.douban-chip');
for(var x=0;x<all.length;x++)all[x].classList.remove('active');
chip.classList.add('active');
applyDoubanFilterAndRender();
};
})(chips[c]);
}

var menuToggle=$('menuToggle');
if(menuToggle){
menuToggle.onclick=function(){document.body.classList.toggle('sidebar-open')};
}
var sidebarOverlay=$('sidebarOverlay');
if(sidebarOverlay){
sidebarOverlay.onclick=function(){document.body.classList.remove('sidebar-open')};
}
}

/* ===== 初始化 ===== */
function init(){
bindEvents();
loadStats();
loadSystem();
startStatsTimer();
}

init();
})();
</script>
</body>
</html>`
