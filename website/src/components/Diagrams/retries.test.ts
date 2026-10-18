import {expect, it} from 'vitest';
import {retries} from './retries';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('holds a retry in the queue, and lets the receive count decide where it goes next', () => {
  const {links} = retries(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('outcome')).toEqual(['wait']);
  // The message waits in place: nothing is republished, so the next stop is the next receive.
  expect(from('wait')).toEqual(['cap']);
  expect(from('cap')).toEqual(['again', 'deadLetter']);
  expect([...from('again'), ...from('deadLetter')]).toEqual([]);
});
