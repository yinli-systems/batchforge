// Development-only regression checks. Run with: node --test frontend/tests/*.test.js
// The console itself remains buildless and does not require Node.
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const path = require('node:path');
const {test} = require('node:test');
const vm = require('node:vm');
const {webcrypto} = require('node:crypto');

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

function harness(fetch, sessionStorage = storage()) {
  const observed = {notices: [], opened: [], rendered: [], dialogCloses: 0};
  const confirmButton = {disabled: false};
  const document = {
    querySelector(selector) {
      if (selector === '#confirm-submit') return confirmButton;
      if (selector === '#confirm-dialog') return {close() { observed.dialogCloses++; }};
      throw new Error(`Unexpected DOM access: ${selector}`);
    }
  };
  const context = vm.createContext({
    document, sessionStorage, fetch, Headers, FormData, crypto: webcrypto, observed
  });
  // Keep the real actions and HTTP client. Stub only unrelated UI refresh/navigation
  // so these tests exercise network outcomes without a DOM framework or a server.
  vm.runInContext(source.slice(0, startupIndex) + `
    toast = (message, type) => observed.notices.push({message, type});
    refresh = async () => {};
    refreshDetail = async () => {};
    openBatch = async id => observed.opened.push(id);
    renderDrawer = () => observed.rendered.push(state.detail);
    globalThis.actions = {state, retryBatch, cancelBatch};
  `, context, {filename: 'frontend/app.js'});
  context.actions.state.key = 'test-tenant-key';
  context.actions.state.detailId = 'source-batch';
  return {...context.actions, observed, confirmButton};
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
