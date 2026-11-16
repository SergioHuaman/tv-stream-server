// Datos del perfil (Mi lista, progreso, vistos, preferencias) guardados en la PC.
(function () {
  var SYNC = /^(mylist|watching|pref|watched:|eps:)/;
  var page = location.pathname.split('/').pop();
  var pid = null;
  try { pid = sessionStorage.getItem('pid'); } catch (e) {}
  if (!pid) {
    if (page !== 'perfiles.html') { location.replace('perfiles.html'); }
    return;
  }

  var nativeSet = Storage.prototype.setItem, nativeRemove = Storage.prototype.removeItem;
  var queue = {}, timer = null;

  function send(ops, sync) {
    try {
      var r = new XMLHttpRequest();
      r.open('POST', '/api/store/' + encodeURIComponent(pid), !sync);
      r.setRequestHeader('Content-Type', 'application/json');
      r.send(JSON.stringify({ ops: ops }));
    } catch (e) {}
  }
  function flush(sync) {
    clearTimeout(timer); timer = null;
    var ops = [], k;
    for (k in queue) { if (queue.hasOwnProperty(k)) { ops.push(queue[k]); } }
    queue = {};
    if (ops.length) { send(ops, sync); }
  }
  function track(op) {
    queue[op.k] = op;
    clearTimeout(timer);
    timer = setTimeout(function () { flush(false); }, 600);
  }
  function syncedKeys() {
    var out = [], i, k;
    for (i = 0; i < localStorage.length; i++) { k = localStorage.key(i); if (SYNC.test(k)) { out.push(k); } }
    return out;
  }

  // 1) Traer los datos de este perfil desde la PC (antes de que cargue el resto de la página).
  var data = null;
  try {
    var x = new XMLHttpRequest();
    x.open('GET', '/api/store/' + encodeURIComponent(pid), false);
    x.send();
    if (x.status === 200) { data = JSON.parse(x.responseText); }
  } catch (e) {}

  if (data) {
    var last = localStorage.getItem('lastpid'), local = syncedKeys(), i, k, empty = true;
    for (k in data) { if (data.hasOwnProperty(k)) { empty = false; break; } }
    if (!last && empty && local.length) {
      // Primera vez con perfiles: lo que ya estaba guardado en la TV pasa a este perfil.
      var ops = [];
      for (i = 0; i < local.length; i++) { ops.push({ k: local[i], v: localStorage.getItem(local[i]) }); }
      send(ops, true);
    } else {
      for (i = 0; i < local.length; i++) { nativeRemove.call(localStorage, local[i]); }
      for (k in data) { if (data.hasOwnProperty(k)) { nativeSet.call(localStorage, k, data[k]); } }
    }
    nativeSet.call(localStorage, 'lastpid', pid);
  }

  // 2) A partir de aquí, cada cambio se envía a la PC.
  try {
    Storage.prototype.setItem = function (key, val) {
      nativeSet.call(this, key, val);
      if (this === window.localStorage && SYNC.test(key)) { track({ k: key, v: String(val) }); }
    };
    Storage.prototype.removeItem = function (key) {
      nativeRemove.call(this, key);
      if (this === window.localStorage && SYNC.test(key)) { track({ k: key, del: 1 }); }
    };
  } catch (e) {}
  window.addEventListener('pagehide', function () { flush(true); });
  window.addEventListener('beforeunload', function () { flush(true); });
})();