/**
 * Q1-D Frontend test harness — exercises ACTUAL production JS from static/index.html.
 *
 * Fidelity approach:
 *   1. Read and extract the main inline <script> from static/index.html (486 KB).
 *   2. Apply a minimal, targeted transform to make module-level `let` state
 *      variables accessible on the vm sandbox object (var hoisting). This is
 *      a test-infrastructure adaptation only — the FUNCTION BODIES are untouched.
 *   3. Suppress two top-level async calls (_fetchCsrfToken, checkAuth, init) that
 *      depend on real browser APIs. Functions defined by the script are unaffected.
 *   4. Evaluate in a Node vm context with shimmed browser globals.
 *   5. Every test calls the REAL production function from the sandbox. No logic
 *      is copied into this file.
 *
 * Run: node tests/test_q1d_frontend.js
 */

'use strict';

const fs   = require('fs');
const path = require('path');
const vm   = require('vm');

// ── 1. Extract production script ──────────────────────────────────────────────

const htmlPath = path.resolve(__dirname, '../static/index.html');
const html = fs.readFileSync(htmlPath, 'utf8');

const scriptBlocks = [];
const scriptRe = /<script>([\s\S]*?)<\/script>/g;
let m;
while ((m = scriptRe.exec(html)) !== null) scriptBlocks.push(m[1]);

if (scriptBlocks.length < 2) {
    console.error('ERROR: main script block not found'); process.exit(1);
}
let script = scriptBlocks[1]; // 486 KB production script

// ── 2. Targeted transforms (infrastructure only — no function body changes) ───
//
// Problem: `let foo = x` at module level in a vm script is scoped to the script,
// NOT on the sandbox/global object. Tests need to read/write these state variables.
//
// Fix: convert the specific module-level state `let` bindings to assignments on
// the sandbox (which already has these as properties from the sandbox init below).
// The function bodies that close over them are completely unmodified.

const stateVarSubstitutions = [
    // [original declaration line,  replacement]
    [`let categories = [];         // default scope (visible, for pickers)`,
     `categories = [];             // default scope (test: hoisted to sandbox)`],
    [`let managementCategories = []; // manage scope (all accessible, for settings screen)`,
     `managementCategories = [];     // manage scope (test: hoisted)`],
    [`let categoryDisplayLookup = {}; // id → {name_he, color} for historical display (populated from manage scope)`,
     `categoryDisplayLookup = {};     // test: hoisted`],
    [`let _manageCatTab = 'active';`,
     `_manageCatTab = 'active';`],
    [`let _editingCatId = null;`,
     `_editingCatId = null;`],
    [`let _newCatSourceSelect = null;`,
     `_newCatSourceSelect = null;`],
];
for (const [orig, repl] of stateVarSubstitutions) {
    if (!script.includes(orig)) {
        console.error(`ERROR: expected state var declaration not found: ${orig.slice(0,60)}`);
        process.exit(1);
    }
    script = script.replace(orig, repl);
}

// ── 3. Suppress top-level calls that need real browser/server ─────────────────
// _fetchCsrfToken() at module level (line 4320 in original)
script = script.replace(/^_fetchCsrfToken\(\);$/m,  '// test: suppressed _fetchCsrfToken()');
script = script.replace(/^checkAuth\(\);$/m,         '// test: suppressed checkAuth()');
script = script.replace(/^init\(\);$/m,              '// test: suppressed init()');

// ── 4. Browser-environment shim ───────────────────────────────────────────────

function makeElement(tag) {
    const el = {
        tag, className: '', textContent: '', innerHTML: '', value: '', selectedIndex: 0,
        disabled: false, onclick: null,
        style: { cssText: '', display: '' }, dataset: {}, _children: [], _attrs: {},
        appendChild(child) { this._children.push(child); return child; },
        setAttribute(k, v) { this._attrs[k] = v; },
        getAttribute(k)    { return this._attrs[k] ?? null; },
        classList: (() => {
            const s = new Set();
            return {
                _set: s,
                add(c)       { s.add(c); },
                remove(c)    { s.delete(c); },
                contains(c)  { return s.has(c); },
                toggle(c)    { s.has(c) ? s.delete(c) : s.add(c); },
            };
        })(),
        querySelectorAll() { return []; },
        querySelector()    { return null; },
        remove()           { },
        focus()            { },
    };
    return el;
}

const _domStore = {};

const shimDocument = {
    getElementById(id) { return _domStore[id] || null; },
    createElement(tag) { return makeElement(tag); },
    createTextNode(t)  { return { nodeType: 3, textContent: t }; },
    querySelectorAll() { return []; },
    querySelector()    { return null; },
    body:              { appendChild() {}, removeChild() {} },
    addEventListener() {},
};

const shimWindow = {
    scrollY: 0,
    scrollTo() {},
    fetch: async () => ({ json: async () => ({}) }),
    addEventListener() {},
};

// Pre-populate DOM elements the production functions reference
[
    'expenseCategory','filterCategory','editExpCategory',
    'newCatName','newCatIcon','newCatColor','addCatError',
    'addCategoryModal','manageCategoriesModal',
    'manageCatList','manageCatEditForm',
    'editCatName','editCatIcon','editCatColor',
    'navAiSettingsItem','navUserItem','navUserDivider','navLogoutItem',
    'navResetItem','navAuthItem','navUsername','adminTabNav',
].forEach(id => {
    _domStore[id] = makeElement('div');
    _domStore[id].value = id === 'newCatColor' ? '#6366f1' : '';
    _domStore[id].innerHTML = '';
    _domStore[id].style = { display: '' };
});

// ── 5. Build sandbox with state variables pre-declared ────────────────────────

const apiCallLog = [];

const sandbox = {
    // Browser globals
    document: shimDocument,
    window: shimWindow,
    fetch: shimWindow.fetch,
    bootstrap: {
        Modal: class {
            constructor() {}
            show() {}
            hide() {}
            static getInstance() { return { hide() {}, show() {} }; }
        },
    },
    console,
    setTimeout: (fn, _) => { try { fn(); } catch(_) {} },
    clearTimeout: () => {},
    setInterval: () => 0,
    clearInterval: () => {},
    localStorage:   { getItem: () => null, setItem: () => {} },
    sessionStorage: { getItem: () => null, setItem: () => {} },
    navigator: { serviceWorker: { register: () => Promise.resolve() } },
    location: { reload() {}, pathname: '/' },
    history:  { replaceState() {}, pushState() {} },
    performance: { now: () => 0 },
    screen: { width: 1280 },

    // App globals that will be redefined by the script but need initial values
    api:          async (url, opts) => { apiCallLog.push({ url, method: (opts && opts.method) || 'GET' }); return {}; },
    showToast:    () => {},
    t:            (k) => k,
    tParams:      (k) => k,
    refreshAll:   async () => {},
    loadBudget:   async () => {},
    loadCategories: async () => {},
    appAlert:     async () => {},
    getPlanSetting: (k, d) => d,

    // State variables (will be assigned into by the transformed script)
    categories:            [],
    managementCategories:  [],
    categoryDisplayLookup: {},
    _manageCatTab:         'active',
    _editingCatId:         null,
    _newCatSourceSelect:   null,
    currentLang:           'he',
    currentMonth:          '2026-09',
    currentPlanId:         1,
    isLoggedIn:            true,
    isAdmin:               false,
    charts:                {},

    // JS builtins
    Promise, Array, Set, Map, Object, JSON, Math, Date,
    parseInt, parseFloat, String, Number, Boolean, RegExp, Error,
    encodeURIComponent, decodeURIComponent, isNaN, isFinite,
    Symbol, WeakMap, WeakSet,
    undefined, null: null,
};

vm.createContext(sandbox);

try {
    vm.runInContext(script, sandbox, { filename: 'static/index.html', timeout: 15000 });
} catch (e) {
    // Some init-time calls may fail (e.g. checkAuth was not fully suppressed) — OK
    // as long as function definitions were evaluated.
}

// Verify production functions are present
const requiredFns = [
    'esc', 'categoryOptionsHtml', 'addNewCategory', 'renderManageCatList',
    'manageCatAction', 'restoreCategoryById', 'saveEditCategory', 'loadCategories',
    'cancelEditCategory', 'openManageCategories',
];
const missing = requiredFns.filter(fn => typeof sandbox[fn] !== 'function');
if (missing.length) {
    console.error('FATAL: production functions not found:', missing.join(', '));
    process.exit(1);
}

console.log('✔ Production JS loaded and evaluated from static/index.html');
console.log(`  Script size: ${(script.length / 1024).toFixed(1)} KB  |  Functions verified: ${requiredFns.length}`);
console.log('  Transforms applied: 6 state-var let→assignment, 3 top-level call suppressions');
console.log('  Function BODIES: untouched — all tests call real production code\n');

// ── Test runner ───────────────────────────────────────────────────────────────

let passed = 0, failed = 0;
const results = [];

async function test(name, fn) {
    try {
        await fn();
        console.log(`  ✓  ${name}`);
        passed++;
        results.push({ name, ok: true });
    } catch (e) {
        console.error(`  ✗  ${name}`);
        console.error(`       ${e.message}`);
        failed++;
        results.push({ name, ok: false, err: e.message });
    }
}

function assert(condition, msg)              { if (!condition) throw new Error(msg || 'Assertion failed'); }
function assertContains(h, n, msg)           { if (typeof h !== 'string') throw new Error(`non-string: ${typeof h}`); if (!h.includes(n)) throw new Error(msg || `Expected "${n}" in output`); }
function assertNotContains(h, n, msg)        { if (typeof h !== 'string') throw new Error(`non-string: ${typeof h}`); if (h.includes(n)) throw new Error(msg || `Did not expect "${n}" in output`); }

// ── State helpers ─────────────────────────────────────────────────────────────

function setCategories(arr)           { sandbox.categories = arr; }
function setManagementCategories(arr) { sandbox.managementCategories = arr; }

function getManageCatListHtml(tab) {
    sandbox._manageCatTab = tab || 'active';
    const container = makeElement('div');
    _domStore['manageCatList'] = container;
    sandbox.renderManageCatList();
    return container.innerHTML;
}

async function runAddNewCategory(apiResponse, name = 'test') {
    const errEl = makeElement('div');
    errEl.classList._set.clear();
    errEl.classList._set.add('d-none');
    _domStore['addCatError']    = errEl;
    _domStore['newCatName'].value  = name;
    _domStore['newCatIcon'].value  = '';
    _domStore['newCatColor'].value = '#6366f1';

    sandbox.api = async (url, opts) => {
        apiCallLog.push({ url, method: (opts && opts.method) || 'GET' });
        return apiResponse;
    };
    sandbox.loadCategories = async () => {};
    sandbox.loadBudget     = async () => {};

    await sandbox.addNewCategory();
    return errEl;
}

// ── Test fixtures ─────────────────────────────────────────────────────────────

const systemCat  = { id: 'food',       name_he: 'מזון',      color: '#e74c3c', icon: null, is_system: true,  is_effectively_hidden: false, is_directly_hidden: false };
const miscCat    = { id: 'misc',       name_he: 'שונות',     color: '#999',    icon: null, is_system: true,  is_effectively_hidden: false, is_directly_hidden: false };
const customCat  = { id: 'custom_abc', name_he: 'חיות מחמד', color: '#6366f1', icon: '🐾', is_system: false, is_effectively_hidden: false, is_directly_hidden: false };
const directHid  = { id: 'education',  name_he: 'חינוך',     color: '#3498db', icon: null, is_system: true,  is_effectively_hidden: true,  is_directly_hidden: true };
const parentHid  = { id: 'housing',    name_he: 'דיור',      color: '#c0392b', icon: null, is_system: true,  is_effectively_hidden: true,  is_directly_hidden: true };
const inheritHid = { id: 'mortgage',   name_he: 'משכנתא',   color: '#e67e22', icon: null, is_system: true,  is_effectively_hidden: true,  is_directly_hidden: false, hidden_by_ancestor_id: 'housing' };
const xssCat     = { id: 'xsscat',     name_he: '<img src=x onerror=alert(1)>', color: '#fff', icon: null, is_system: false, is_effectively_hidden: false, is_directly_hidden: false };

// ── Tests ─────────────────────────────────────────────────────────────────────

(async () => {

// ── XSS / safe rendering ─────────────────────────────────────────────────────
console.log('XSS / safe rendering  [production esc()]');

await test('esc() escapes < > & " \'', () => {
    const { esc } = sandbox;
    assert(esc('<img src=x onerror=alert(1)>') === '&lt;img src=x onerror=alert(1)&gt;');
    assert(esc('"hello"') === '&quot;hello&quot;');
    assert(esc("it's") === 'it&#39;s');
    assert(esc('a & b') === 'a &amp; b');
});

await test('esc(null) and esc(undefined) return empty string', () => {
    assert(sandbox.esc(null) === '');
    assert(sandbox.esc(undefined) === '');
});

// ── categoryOptionsHtml ───────────────────────────────────────────────────────
console.log('\ncategoryOptionsHtml  [production function]');

await test('malicious category name is HTML-escaped', () => {
    setCategories([{ id: 'food', name_he: '<script>alert(1)</script>' }]);
    const html = sandbox.categoryOptionsHtml('food');
    assertNotContains(html, '<script>', 'raw <script> tag must not appear');
    assertContains(html, '&lt;script&gt;');
});

await test('malicious category id: double-quote encoded, no attribute break-out', () => {
    setCategories([{ id: '" onmouseover="alert(1)', name_he: 'test' }]);
    const html = sandbox.categoryOptionsHtml();
    assertNotContains(html, 'value="" onmouseover', 'attribute break-out must be blocked');
    assertContains(html, '&quot;', 'double-quote must be encoded');
});

await test('includes __new__ and __manage__ options', () => {
    setCategories([{ id: 'food', name_he: 'מזון' }]);
    const html = sandbox.categoryOptionsHtml();
    assertContains(html, '__new__');
    assertContains(html, '__manage__');
});

await test('selected category has selected attribute', () => {
    setCategories([{ id: 'food', name_he: 'מזון' }, { id: 'education', name_he: 'חינוך' }]);
    const html = sandbox.categoryOptionsHtml('education');
    assert(html.includes('value="education" selected'), `'education' must be selected; html=${html.slice(0,200)}`);
});

await test('hidden category absent from picker (picker uses filtered categories array)', () => {
    // picker array only contains effective-visible items — simulating server filtering
    setCategories([{ id: 'food', name_he: 'מזון' }]);
    const html = sandbox.categoryOptionsHtml();
    assertContains(html, 'food');
    assertNotContains(html, 'education');
});

// ── renderManageCatList ───────────────────────────────────────────────────────
console.log('\nrenderManageCatList  [production function]');

await test('misc has NO hide button in active tab', () => {
    setManagementCategories([miscCat]);
    const html = getManageCatListHtml('active');
    assertNotContains(html, 'הסתר', 'misc must never show hide button');
});

await test('system category has NO edit button in active tab', () => {
    setManagementCategories([systemCat]);
    const html = getManageCatListHtml('active');
    assertNotContains(html, 'עריכה', 'system must not show edit');
});

await test('system category HAS hide button in active tab', () => {
    setManagementCategories([systemCat]);
    const html = getManageCatListHtml('active');
    assertContains(html, 'הסתר', 'system must show hide button');
});

await test('custom category has both edit and hide in active tab', () => {
    setManagementCategories([customCat]);
    const html = getManageCatListHtml('active');
    assertContains(html, 'עריכה');
    assertContains(html, 'הסתר');
});

await test('directly hidden: restore button in hidden tab', () => {
    setManagementCategories([directHid]);
    const html = getManageCatListHtml('hidden');
    assertContains(html, 'שחזר');
    assertNotContains(html, 'מוסתר ע"י');
});

await test('inherited-only hidden: ancestor name shown, NO restore button', () => {
    const parentLookup = { ...parentHid, is_effectively_hidden: false }; // for name lookup only
    setManagementCategories([inheritHid, parentLookup]);
    const html = getManageCatListHtml('hidden');
    assertContains(html, 'דיור', 'ancestor name must appear');
    assertContains(html, 'מוסתר ע"י', '"hidden by" label must appear');
    assertNotContains(html, 'שחזר', 'inherited-only must NOT show restore button');
});

await test('no delete action in active tab', () => {
    setManagementCategories([systemCat, customCat, miscCat]);
    const html = getManageCatListHtml('active');
    assertNotContains(html, 'מחק');
    assertNotContains(html, 'delete');
    assertNotContains(html, 'trash');
});

await test('no delete action in hidden tab', () => {
    setManagementCategories([directHid]);
    const html = getManageCatListHtml('hidden');
    assertNotContains(html, 'מחק');
    assertNotContains(html, 'delete');
});

await test('category names in management list are HTML-escaped (XSS)', () => {
    setManagementCategories([xssCat]);
    const html = getManageCatListHtml('active');
    assertNotContains(html, '<img src=x');
    assertContains(html, '&lt;img');
});

// ── addNewCategory duplicate handling ─────────────────────────────────────────
console.log('\naddNewCategory duplicate handling  [production function, async]');

await test('duplicate_active: error shown in Hebrew', async () => {
    const errEl = await runAddNewCategory({ error: 'duplicate_active', category_id: 'food' });
    assert(!errEl.classList._set.has('d-none'), 'error element must be visible');
    assert(errEl.textContent.includes('קיימת'), `must contain "קיימת", got: "${errEl.textContent}"`);
});

await test('duplicate_hidden: restore button child appended (reads category_id field)', async () => {
    const errEl = await runAddNewCategory({ error: 'duplicate_hidden', category_id: 'custom_abc' });
    assert(!errEl.classList._set.has('d-none'), 'error element must be visible');
    const hasRestoreChild = errEl._children.some(c => c.textContent === 'לשחזר?');
    assert(hasRestoreChild, `restore button must be in _children. Got: ${JSON.stringify(errEl._children.map(c=>c.textContent))}`);
});

await test('duplicate_inherited_hidden: ancestor name resolved from managementCategories', async () => {
    setManagementCategories([{ id: 'housing', name_he: 'דיור', color: '#c00', icon: null }]);
    const errEl = await runAddNewCategory({
        error: 'duplicate_inherited_hidden',
        category_id: 'mortgage',
        hidden_by_ancestor_id: 'housing',
    });
    assert(!errEl.classList._set.has('d-none'), 'error must be visible');
    const textNode = errEl._children.find(c => c.nodeType === 3);
    assert(textNode && textNode.textContent.includes('דיור'),
        `ancestor "דיור" must appear in text node. Children: ${JSON.stringify(errEl._children.map(c=>({type:c.nodeType,text:c.textContent})))}`);
});

await test('duplicate_inherited_hidden: manage button present', async () => {
    setManagementCategories([{ id: 'housing', name_he: 'דיור', color: '#c00', icon: null }]);
    const errEl = await runAddNewCategory({
        error: 'duplicate_inherited_hidden',
        category_id: 'mortgage',
        hidden_by_ancestor_id: 'housing',
    });
    const manageBtn = errEl._children.find(c => c.tag && c.textContent === 'פתח ניהול קטגוריות');
    assert(manageBtn, 'manage categories button must be present');
});

await test('duplicate_inherited_hidden: NO restore-child button', async () => {
    setManagementCategories([{ id: 'housing', name_he: 'דיור', color: '#c00', icon: null }]);
    const errEl = await runAddNewCategory({
        error: 'duplicate_inherited_hidden',
        category_id: 'mortgage',
        hidden_by_ancestor_id: 'housing',
    });
    const hasRestoreBtn = errEl._children.some(c => c.textContent === 'לשחזר?');
    assert(!hasRestoreBtn, 'inherited hidden must NOT show a restore-child button');
});

// ── Summary ───────────────────────────────────────────────────────────────────
console.log(`\n${'─'.repeat(60)}`);
console.log(`Q1-D frontend tests: ${passed} passed, ${failed} failed`);
console.log('Source: ACTUAL production functions evaluated from static/index.html');
if (failed > 0) process.exit(1);

})();
