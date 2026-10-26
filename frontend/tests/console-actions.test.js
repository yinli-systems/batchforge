// Development-only regression checks. Run with: node --test frontend/tests/*.test.js
// The console itself remains buildless and does not require Node.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const path = require('node:path');
const {test} = require('node:test');
const vm = require('node:vm');
const {webcrypto} = require('node:crypto');
const {File} = require('node:buffer');

const source = readFileSync(path.join(__dirname, '..', 'app.js'), 'utf8');
const startupIndex = source.indexOf("document.addEventListener('click'");
assert.ok(startupIndex > 0, 'The harness must stop before browser event registration.');

function storage() {
  const entries = new Map();
  return {
    getItem(key) { return entries.get(key) ?? null; },
    setItem(key, value) { entries.set(key, String(value)); },
    removeItem(key) { entries.delete(key); }
  };
}

function jsonResponse(body) {
  return {ok: true, status: 200, json: async () => body};
}

function harness(fetch, sessionStorage = storage(), fixtures = {}) {
  const observed = {notices: [], opened: [], rendered: [], dialogCloses: 0, downloads: [], blobs: []};
  const confirmButton = {disabled: false};
  const drawer = {innerHTML: '', scrollTop: 0, contains() { return false; }};
  const document = {
    createElement(tag) {
      assert.equal(tag, 'a');
      return {href: '', download: '', click() {observed.downloads.push(this.download);}};
    },
    querySelector(selector) {
      if (Object.hasOwn(fixtures, selector)) return fixtures[selector];
      if (selector === '#confirm-submit') return confirmButton;
      if (selector === '#confirm-dialog') return {close() { observed.dialogCloses++; }};
      if (selector === '#batch-drawer') return drawer;
      throw new Error(`Unexpected DOM access: ${selector}`);
    }
  };
  const context = vm.createContext({
    document, sessionStorage, fetch, Headers, FormData, TextEncoder, TextDecoder, Uint8Array, Blob, File, location: {hash:''},
    URL: {createObjectURL(blob) {observed.blobs.push(blob);return 'blob:review';}, revokeObjectURL() {}},
    setTimeout(callback) {callback();}, crypto: webcrypto, observed
  });
  // Keep the real actions and HTTP client. Stub only unrelated UI refresh/navigation
  // so these tests exercise network outcomes without a DOM framework or a server.
  vm.runInContext(source.slice(0, startupIndex) + `
    toast = (message, type) => observed.notices.push({message, type});
    refresh = async () => {};
    refreshDetail = async () => {};
    openBatch = async id => observed.opened.push(id);
    const renderDetails = renderDrawer;
    renderDrawer = () => observed.rendered.push(state.detail);
    globalThis.actions = {state, renderFleet, loadFleet, createBatch, retryBatch, cancelBatch, renderDetails, verifyReceipt, loadEvidence, resetEvidence, renderEvidencePanel, capacityNotice, qualityView, loadEvaluations, resetQualityEvidence, downloadEvaluation};
  `, context, {filename: 'frontend/app.js'});
  context.actions.state.key = 'test-tenant-key';
  context.actions.state.detailId = 'source-batch';
  return {...context.actions, observed, confirmButton, drawer};
}

test('cancellation reports completion when completion wins the race', async () => {
  const app = harness(async (url, request) => {
    assert.equal(url, '/v1/batches/source-batch/cancel');
    assert.equal(request.method, 'POST');
    assert.equal(request.headers.get('Authorization'), 'Bearer test-tenant-key');
    return jsonResponse({id: 'source-batch', status: 'completed'});
  });

  await app.cancelBatch({preventDefault() {}});

  assert.equal(app.state.detail.status, 'completed');
  assert.equal(app.observed.rendered.at(-1).status, 'completed');
  assert.match(app.observed.notices.at(-1).message, /already completed/i);
  assert.doesNotMatch(app.observed.notices.at(-1).message, /batch cancelled/i);
  assert.equal(app.observed.dialogCloses, 1);
  assert.equal(app.confirmButton.disabled, false);
});

function createFixtures() {
  const fixtures = {'#create-submit': {disabled:false}, '#create-error': {hidden:true,textContent:''},
    '#existing-input-id': {value:'file_'+'a'.repeat(32)}, '#batch-name': {value:'Reused input'},
    '#create-dialog': {closes:0,close(){this.closes++;}}};
  return fixtures;
}

test('stale fleet data never appears as current utilization and labels are escaped', () => {
  const app=harness(async()=>jsonResponse({}));
  app.state.fleet={rejected_reports:0,data:[{id:'abc',observed_device_count:1,reports:[{inventory_state:'stale',
    inventory:{observed_at:'2026-10-25T15:00:00Z',devices:[{index:0,name:'<script>bad</script>',memory_total_mib:81920,
      utilization_percent:100,memory_used_mib:74000,power_watts:690,temperature_celsius:60}],links:[]}}]}]};
  const html=app.renderFleet();
  assert.ok(!html.includes('<script>'));assert.ok(html.includes('&lt;script&gt;'));
  assert.ok(!html.includes('100%'));assert.ok(html.includes('—%'));
  assert.match(html,/do not reserve GPUs/);
});

test('fleet responses from an old workspace cannot populate the next workspace', async () => {
  const app=harness(async()=>{app.state.key='next-workspace';return jsonResponse({data:[{id:'private-node'}]});});
  await app.loadFleet();assert.equal(app.state.fleet,null);
});

test('reusing input skips upload and retries lost acknowledgement with the same task-set key', async () => {
  const fixtures=createFixtures(), calls=[];
  const app=harness(async (url,request) => {
    calls.push({url,key:request.headers.get('Idempotency-Key'),body:request.body});
    assert.equal(url,'/v1/batches');
    assert.equal(JSON.parse(request.body).input_file_id,fixtures['#existing-input-id'].value);
    if(calls.length===1)throw new TypeError('Lost acknowledgement');
    return jsonResponse({id:'batch-reused'});
  },storage(),fixtures);
  app.state.inputMode='existing';
  await app.createBatch({preventDefault(){}});
  assert.equal(app.observed.opened.length,0);
  await app.createBatch({preventDefault(){}});
  assert.equal(calls[0].key,calls[1].key);
  assert.equal(calls[0].body,calls[1].body);
  assert.equal(app.observed.opened.at(-1),'batch-reused');
  assert.equal(fixtures['#create-dialog'].closes,1);
});

test('invalid reusable file ID never creates or uploads work', async () => {
  const fixtures=createFixtures();fixtures['#existing-input-id'].value='../another-workspace';
  let requests=0;
  const app=harness(async()=>{requests++;},storage(),fixtures);app.state.inputMode='existing';
  await app.createBatch({preventDefault(){}});
  assert.equal(requests,0);assert.match(fixtures['#create-error'].textContent,/valid input file ID/);
});

test('workspace switch during upload prevents creating tasks under the next key', async () => {
  const fixtures=createFixtures(),calls=[];
  const app=harness(async (url) => {
    calls.push(url);app.state.key='next-workspace';return jsonResponse({id:'file_'+'a'.repeat(32)});
  },storage(),fixtures);
  app.state.inputMode='upload';app.state.uploadFile=new File(['private test request'],'input.jsonl');
  await app.createBatch({preventDefault(){}});
  assert.deepEqual(calls,['/v1/files']);assert.equal(app.observed.opened.length,0);
});

test('workspace switch after create does not show an old-workspace result', async () => {
  const fixtures=createFixtures();
  const app=harness(async()=>{app.state.key='next-workspace';return jsonResponse({id:'old-workspace-batch'});},storage(),fixtures);
  app.state.inputMode='existing';await app.createBatch({preventDefault(){}});
  assert.equal(app.observed.opened.length,0);assert.equal(fixtures['#create-dialog'].closes,0);
});

test('a confirmed cancellation still reports cancellation', async () => {
  const app = harness(async () => jsonResponse({id: 'source-batch', status: 'cancelled'}));

  await app.cancelBatch({preventDefault() {}});

  assert.equal(app.state.detail.status, 'cancelled');
  assert.match(app.observed.notices.at(-1).message, /^Batch cancelled\./);
  assert.equal(app.confirmButton.disabled, false);
});

test('a lost retry acknowledgement cannot create duplicate work, including after tab reload', async () => {
  const sharedSession = storage();
  const accepted = new Map();
  const keys = [];
  let loseAcknowledgement = true;
  let billableBatches = 0;
  let currentButton;
  const server = async (url, request) => {
    assert.equal(url, '/v1/batches/source-batch/retry');
    assert.equal(request.method, 'POST');
    assert.equal(currentButton.disabled, true, 'The action remains disabled during submission.');
    const key = request.headers.get('Idempotency-Key');
    assert.ok(key, 'Every retry submission must carry an idempotency key.');
    keys.push(key);
    if (!accepted.has(key)) accepted.set(key, {id: `retry-${++billableBatches}`, status: 'in_progress'});
    if (loseAcknowledgement) {
      loseAcknowledgement = false;
      // The server committed work, but the client never received the response.
      throw new TypeError('Connection closed before response');
    }
    return jsonResponse(accepted.get(key));
  };

  const firstTab = harness(server, sharedSession);
  currentButton = {disabled: false};
  await firstTab.retryBatch(currentButton);
  assert.equal(billableBatches, 1);
  assert.equal(firstTab.observed.opened.length, 0);
  assert.equal(currentButton.disabled, false, 'An ambiguous failure allows a safe retry.');

  // A fresh context models a reload: only session storage survives.
  const reloadedTab = harness(server, sharedSession);
  currentButton = {disabled: false};
  await reloadedTab.retryBatch(currentButton);
  assert.equal(keys[1], keys[0], 'Resubmission must recover the accepted attempt.');
  assert.equal(billableBatches, 1, 'The accepted task set must exist only once.');
  assert.equal(reloadedTab.observed.opened.at(-1), 'retry-1');

  // A later intentional retry, after acknowledged success, starts a new attempt.
  reloadedTab.state.detailId = 'source-batch';
  currentButton = {disabled: false};
  await reloadedTab.retryBatch(currentButton);
  assert.notEqual(keys[2], keys[1]);
  assert.equal(billableBatches, 2);
  assert.equal(reloadedTab.observed.opened.at(-1), 'retry-2');
});

function finishedBatch(history) {
  return {
    id: 'source-batch', status: 'failed', model: 'test-model',
    request_counts: {total: 10, completed: 8, failed: 2, running: 0, pending: 0, cancelled: 0},
    usage: {prompt_tokens: 123, completion_tokens: 456},
    output_file_id: 'file-output', error_file_id: 'file-errors',
    ...(history ? {history_retention: history} : {})
  };
}

for (const [name, history, expectedNotice] of [
  ['fully retired', {state: 'retired', retired_requests: 10}, /Task details have been fully retired/],
  ['partially retired', {state: 'retiring', retired_requests: 3}, /3 of 10 task details have been retired/],
  ['retirement started before the first page', {state: 'retiring', retired_requests: 0}, /Task detail retirement has started/]
]) {
  test(`${name} keeps totals, usage and downloads while blocking even a stale retry control`, async () => {
    let requests = 0;
    const app = harness(async () => { requests++; throw new Error('No retry may be submitted'); });
    app.state.detail = finishedBatch(history);
    app.renderDetails();
    assert.match(app.drawer.innerHTML, expectedNotice);
    assert.match(app.drawer.innerHTML, /Batch totals and token usage are preserved/);
    assert.match(app.drawer.innerHTML, /data-download-file="file-output"/);
    assert.match(app.drawer.innerHTML, /data-download-file="file-errors"/);
    assert.match(app.drawer.innerHTML, /Input tokens<\/dt><dd>123<\/dd>/);
    assert.match(app.drawer.innerHTML, /Output tokens<\/dt><dd>456<\/dd>/);
    assert.match(app.drawer.innerHTML, /Total requests<\/dt><dd class="">10<\/dd>/);
    assert.doesNotMatch(app.drawer.innerHTML, /data-action="retry-batch"|Retry creates a new batch/);
    const staleButton = {disabled: false};
    await app.retryBatch(staleButton);
    assert.equal(requests, 0);
    assert.equal(staleButton.disabled, false);
    assert.match(app.observed.notices.at(-1).message, /cannot be retried.*history retirement has started/i);
  });
}

test('an unknown retention state is never interpolated and a positive retired count still prevents retry', async () => {
  const injected = '<img src=x onerror="alert(1)">';
  let requests = 0;
  const app = harness(async () => { requests++; throw new Error('No retry'); });
  app.state.detail = finishedBatch({state: injected, retired_requests: 2});
  app.state.detail.output_file_id = 'file-"<unsafe>';
  app.renderDetails();
  assert.match(app.drawer.innerHTML, /Task history partially retired/);
  assert.match(app.drawer.innerHTML, /2 of 10 task details have been retired/);
  assert.doesNotMatch(app.drawer.innerHTML, /<img|onerror|data-action="retry-batch"/);
  assert.match(app.drawer.innerHTML, /data-download-file="file-&quot;&lt;unsafe&gt;"/);
  await app.retryBatch({disabled: false});
  assert.equal(requests, 0);
});

test('a normal unmarked terminal batch preserves its existing retry and result behavior', async () => {
  let requests = 0;
  const app = harness(async (url, request) => {
    requests++;
    assert.equal(url, '/v1/batches/source-batch/retry');
    assert.equal(request.method, 'POST');
    return jsonResponse({id: 'retry-normal', status: 'in_progress'});
  });
  app.state.detail = finishedBatch();
  app.renderDetails();
  assert.match(app.drawer.innerHTML, /data-action="retry-batch"/);
  assert.match(app.drawer.innerHTML, /Retry creates a new batch from failed tasks only/);
  assert.doesNotMatch(app.drawer.innerHTML, /aria-label="Task history retention"/);
  assert.match(app.drawer.innerHTML, /data-download-file="file-output"/);
  await app.retryBatch({disabled: false});
  assert.equal(requests, 1);
  assert.equal(app.observed.opened.at(-1), 'retry-normal');
});

const receiptFixture = JSON.parse(readFileSync(path.join(__dirname, 'fixtures', 'ledger-receipt.json'), 'utf8'));

test('browser verifies Python canonical receipt and rejects altered usage', async () => {
  const app = harness(async () => {throw new Error('No network expected');});
  assert.equal((await app.verifyReceipt(receiptFixture)).receipt_sha256, receiptFixture.receipt_sha256);
  const tampered = structuredClone(receiptFixture);
  tampered.receipt.accepted_usage.completion_tokens++;
  await assert.rejects(app.verifyReceipt(tampered), /checksum does not match/);
  const imprecise = structuredClone(receiptFixture);
  imprecise.receipt.accepted_usage.completion_tokens = 9007199254740992;
  await assert.rejects(app.verifyReceipt(imprecise), /imprecise/);
});

test('a closed or replaced drawer cannot be overwritten by stale evidence responses', async () => {
  const releases = [];
  const app = harness(url => new Promise(resolve => releases.push(() => resolve(jsonResponse(
    url.includes('/receipt') ? receiptFixture : {data: [], has_more: false, retired_tasks: 0}
  )))));
  const pending = app.loadEvidence();
  assert.equal(releases.length, 2);
  app.resetEvidence();
  app.state.detailId = 'another-batch';
  releases.forEach(release => release());
  await pending;
  assert.equal(app.state.detailReceipt, null);
  assert.equal(app.state.detailTaskPage, null);
  assert.equal(app.state.evidenceLoading, false);
});

test('evidence UI escapes task identifiers and exposes limited verification scope', () => {
  const app = harness(async () => {throw new Error('No network expected');});
  app.state.detailReceipt = receiptFixture;
  app.state.detailTaskPage = {data: [{custom_id: '<img src=x onerror=alert(1)>', status: 'completed',
    attempts: 2, max_attempts: 3, prompt_tokens: 4, completion_tokens: 5,
    completion_seconds: 1.25, worker_id: '<script>bad()</script>'}], has_more: true, retired_tasks: 1};
  const html = app.renderEvidencePanel();
  assert.doesNotMatch(html, /<img|<script>/);
  assert.match(html, /&lt;img/);
  assert.match(html, /Next 25 tasks/);
  assert.match(html, /not a signature, hardware attestation, or answer-quality score/);
  assert.match(html, /1 task rows have been retired/);
});

const qualityFixture = JSON.parse(readFileSync(path.join(__dirname, 'fixtures', 'quality-evaluation.json'), 'utf8'));
const engineFixture = JSON.parse(readFileSync(path.join(__dirname, 'fixtures', 'engine-evaluation.json'), 'utf8'));

test('engine score correction stays separate from failed Batch quality and nested cost is explicit', () => {
  const app = harness(async () => {throw new Error('No network expected');});
  app.state.connected = true;app.state.qualityLoaded = true;
  app.state.evaluations = [structuredClone(qualityFixture)];app.state.engineEvaluations = [structuredClone(engineFixture)];
  const html = app.qualityView();
  assert.match(html, /Batch API evidence/);assert.match(html, /Quality gate failed/);
  assert.match(html, /Engine benchmarks/);assert.match(html, /Engine quality gate passed/);
  assert.match(html, /82\.00/);assert.doesNotMatch(html, /82\.01/);
  assert.match(html, /1 legacy scores corrected; original report preserved/);
  assert.match(html, /\$2\.23 at \$16\.10\/h/);assert.match(html, /\$2\.14 · do not add/);
  assert.match(html, /no Batch delivery or usage-ledger verification/);
  assert.match(html, /not an invoice/);assert.match(html, /does not prove current availability/);
});

test('unverified engine scope hides scores and cost while escaping publisher names', () => {
  const app = harness(async () => {throw new Error('No network expected');});
  app.state.connected = true;app.state.qualityLoaded = true;
  const e = structuredClone(engineFixture);e.scope.batch_ledger_verified = true;e.name = '<img src=x onerror=alert(1)>';
  app.state.engineEvaluations = [e];const html = app.qualityView();
  assert.match(html, /Engine evidence unverified/);assert.match(html, /&lt;img/);
  assert.doesNotMatch(html, /<img|82\.00|\$2\.23/);
});

test('both quality sources are invalidated when workspace changes while responses are pending', async () => {
  const releases = [];
  const app = harness(async (url, request) => {
    assert.equal(request.headers.get('Authorization'), 'Bearer test-tenant-key');
    assert.ok(['/v1/evaluations', '/v1/engine-evaluations'].includes(url));
    return new Promise(resolve => releases.push(() => resolve(jsonResponse({data: [url.includes('engine-') ? engineFixture : qualityFixture]}))));
  });
  const pending = app.loadEvaluations();assert.equal(releases.length, 2);
  app.state.key = 'new-workspace';app.resetQualityEvidence();releases.forEach(f => f());await pending;
  assert.equal(app.state.evaluations.length, 0);assert.equal(app.state.engineEvaluations.length, 0);
  assert.equal(app.state.qualityLoaded, false);assert.equal(app.state.qualityLoading, false);
});

test('missing engine endpoint on an old deployment does not hide Batch reports', async () => {
  const app = harness(async url => url === '/v1/evaluations' ? jsonResponse({data: [qualityFixture]}) :
    {ok: false, status: 404, json: async () => ({error: {message: 'Not deployed'}})});
  app.state.connected = true;await app.loadEvaluations();
  assert.equal(app.state.engineQualityError, '');assert.match(app.qualityView(), /Quality gate failed/);
});

test('a failed Batch evidence request does not erase a valid separate engine benchmark', async () => {
  const app = harness(async url => url === '/v1/engine-evaluations' ? jsonResponse({data: [engineFixture]}) :
    {ok: false, status: 503, json: async () => ({error: {message: 'Batch evidence unavailable'}})});
  app.state.connected = true;await app.loadEvaluations();
  const html = app.qualityView();assert.match(html, /Batch evidence unavailable/);assert.match(html, /Engine quality gate passed/);
});

test('engine evidence download verifies exact bytes and rejects a checksummed Batch substitution', async () => {
  const crypto = require('node:crypto');
  for (const substituted of [false, true]) {
    const value = structuredClone(engineFixture);
    if (substituted) value.object = 'quality_evaluation';
    const raw = JSON.stringify(value), hash = crypto.createHash('sha256').update(raw).digest('hex');
    const app = harness(async url => {
      assert.equal(url, `/v1/engine-evaluations/${engineFixture.id}`);
      return {ok: true, status: 200, headers: new Headers({'X-BatchForge-Evidence-SHA256': hash}), arrayBuffer: async () => new TextEncoder().encode(raw).buffer};
    });
    await app.downloadEvaluation({disabled: false, dataset: {evaluationId: engineFixture.id, evidenceKind: 'engine'}});
    if (substituted) {assert.equal(app.observed.downloads.length, 0);assert.match(app.observed.notices[0].message, /scope changed/);}
    else {assert.equal(app.observed.downloads.length, 1);assert.equal(await app.observed.blobs[0].text(), raw);}
  }
});

test('published allocation cannot imply an invoice or turn a failed gate into a pass', () => {
  const app = harness(async () => {throw new Error('Unexpected fetch');});
  app.state.connected = true;app.state.qualityLoaded = true;
  const value = structuredClone(qualityFixture);
  value.cost = {kind: 'operator_rate_time_allocation', whole_pod_window_usd: '11.75338',
    hourly_rate_usd: '16.10', usd_per_on_time_reference_exact_result: '0.00206598'};
  value.answer_contract = {id: 'json-span-v1', invalid_results: 18};
  app.state.evaluations = [value];
  const html = app.qualityView();
  assert.match(html, /\$11.75 at \$16.10\/h/);assert.match(html, /\$2.07/);
  assert.match(html, /not an invoice/);assert.match(html, /Quality gate failed/);
  value.verification_state = 'batch_ledger_mismatch';
  assert.doesNotMatch(app.qualityView(), /\$11.75/);
});

test('quality view separates successful delivery from a failed measured quality gate', () => {
  const app = harness(async () => {throw new Error('No network expected');});
  app.state.connected = true; app.state.qualityLoaded = true;
  app.state.evaluations = [structuredClone(qualityFixture)];
  const html = app.qualityView();
  assert.match(html, /Quality gate failed/);
  assert.match(html, /All reported tasks delivered/);
  assert.match(html, /53\.82/); assert.match(html, /75\.82/);
  assert.doesNotMatch(html, /Quality gate passed/);
  assert.match(html, /not a production accuracy guarantee/);
  app.state.evaluations[0].name = '<img src=x onerror=alert(1)>';
  app.state.evaluations[0].verification_state = 'batch_ledger_mismatch';
  app.state.evaluations[0].metrics = null;
  const unverified = app.qualityView();
  assert.match(unverified, /Ledger linkage unverified/);
  assert.doesNotMatch(unverified, /53\.82|<img/);
  assert.match(unverified, /&lt;img/);
});

test('quality responses cannot populate a different workspace after switching keys', async () => {
  let release;
  const app = harness(async (url, request) => {
    assert.equal(url, '/v1/evaluations');
    assert.equal(request.headers.get('Authorization'), 'Bearer test-tenant-key');
    return new Promise(resolve => {release = () => resolve(jsonResponse({data: [qualityFixture]}));});
  });
  const pending = app.loadEvaluations();
  app.state.key = 'another-workspace';app.resetQualityEvidence();
  release();await pending;
  assert.equal(app.state.evaluations.length, 0);
  assert.equal(app.state.qualityLoaded, false);
  assert.equal(app.state.qualityLoading, false);
});

test('an in-flight evidence download is discarded if workspace ownership changes', async () => {
  let release;
  const app = harness(async () => new Promise(resolve => {release = () => resolve(jsonResponse(qualityFixture));}));
  const button = {disabled: false, dataset: {evaluationId: qualityFixture.id}};
  const pending = app.downloadEvaluation(button);
  assert.equal(button.disabled, true);
  app.state.key = 'another-workspace';app.resetQualityEvidence();release();await pending;
  assert.equal(button.disabled, false);
  assert.equal(app.observed.notices.length, 0);
});

test('evidence download verifies and saves exact received bytes without JSON reformatting', async () => {
  const raw = JSON.stringify(qualityFixture);
  const hash = require('node:crypto').createHash('sha256').update(raw).digest('hex');
  const app = harness(async () => new Response(raw, {headers: {'X-BatchForge-Evidence-SHA256': hash}}));
  const button = {disabled: false, dataset: {evaluationId: qualityFixture.id}};
  await app.downloadEvaluation(button);
  assert.equal(app.observed.downloads.length, 1);
  assert.equal(await app.observed.blobs[0].text(), raw);
  assert.equal(button.disabled, false);
});

test('corrupt or missing evidence checksums cannot produce a downloaded artifact', async () => {
  for (const headers of [{}, {'X-BatchForge-Evidence-SHA256': '0'.repeat(64)}]) {
    const app = harness(async () => new Response(JSON.stringify(qualityFixture), {headers}));
    await app.downloadEvaluation({disabled: false, dataset: {evaluationId: qualityFixture.id}});
    assert.equal(app.observed.downloads.length, 0);
    assert.match(app.observed.notices[0].message, /checksum/);
  }
});
