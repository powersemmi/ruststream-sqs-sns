import {expect, it} from 'vitest';
import {visibility} from './visibility';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('takes a received message out of view, and ends it by a delete or brings it back', () => {
  const {links} = visibility(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('stored')).toEqual(['inFlight']);
  expect(from('inFlight')).toEqual(['deleted', 'lapsed', 'zero']);
  // A delete ends the message; both other ways put it back in view for the next receive.
  expect(from('deleted')).toEqual([]);
  expect(from('lapsed')).toEqual(['again']);
  expect(from('zero')).toEqual(['again']);
  expect(from('again')).toEqual(['deadLetter']);
});
