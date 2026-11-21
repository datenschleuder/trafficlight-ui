'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const i18n = require('./static/i18n.js');

test('dictionaries have matching nonempty keys and placeholders', () => {
  const keys = Object.keys(i18n.dictionaries.de).sort();
  assert.ok(keys.length > 0);
  const placeholders = value => [...value.matchAll(/\{(\w+)\}/g)].map(m => m[1]).sort();
  for (const lang of i18n.languages) {
    const dict = i18n.dictionaries[lang];
    assert.deepEqual(Object.keys(dict).sort(), keys, lang);
    for (const key of keys) {
      assert.equal(typeof dict[key], 'string', `${lang}.${key}`);
      assert.ok(dict[key].trim(), `${lang}.${key}`);
      assert.deepEqual(placeholders(dict[key]), placeholders(i18n.dictionaries.de[key]), `${lang}.${key}`);
    }
  }
});

test('all literal translation keys used by the frontend exist', () => {
  const html = fs.readFileSync(path.join(__dirname, 'static/index.html'), 'utf8');
  const js = fs.readFileSync(path.join(__dirname, 'static/app.js'), 'utf8');
  const keys = [
    ...[...html.matchAll(/data-i18n(?:-title|-placeholder)?="([^"]+)"/g)].map(m => m[1]),
    ...[...js.matchAll(/\b(?:t|ht)\('([^']+)'/g)].map(m => m[1]),
    ...[...js.matchAll(/I18n\.translate\(lang, '([^']+)'/g)].map(m => m[1]),
  ];
  for (const key of keys) for (const lang of i18n.languages) {
    assert.ok(i18n.dictionaries[lang][key], `${lang}.${key}`);
  }
});

test('detects saved selection, supported browser languages and fallback', () => {
  const storage = { getItem: key => key === i18n.storageKey ? 'FR-ca' : null };
  assert.equal(i18n.detect(storage, ['es-MX']), 'fr');
  assert.equal(i18n.detect({ getItem: () => 'it' }, ['it-IT', 'es-MX']), 'es');
  assert.equal(i18n.detect(null, ['ja', 'en-US']), 'en');
  assert.equal(i18n.detect(null, ['ja']), 'de');
  assert.equal(i18n.supported('de-DE'), 'de');
  assert.equal(i18n.supported('english'), null);
});

test('saves selection and tolerates unavailable storage', () => {
  let saved;
  const storage = { setItem: (key, value) => { saved = [key, value]; } };
  i18n.save(storage, 'es');
  assert.deepEqual(saved, [i18n.storageKey, 'es']);
  const unavailable = { getItem() { throw Error('denied'); }, setItem() { throw Error('denied'); } };
  assert.equal(i18n.detect(unavailable, ['fr-FR']), 'fr');
  assert.doesNotThrow(() => i18n.save(unavailable, 'fr'));
});

test('interpolates placeholders and fails on missing keys or values', () => {
  assert.equal(i18n.translate('en', 'days', { n: 7 }), '7 days');
  assert.equal(i18n.translate('es', 'since', { date: '<date>' }), 'desde <date>');
  assert.throws(() => i18n.translate('en', 'not-a-key'), /Missing translation/);
  assert.throws(() => i18n.translate('en', 'days'), /Missing translation parameter/);
});
