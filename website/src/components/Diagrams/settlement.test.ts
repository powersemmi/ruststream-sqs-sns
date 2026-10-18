import {expect, it} from 'vitest';
import {OUTCOMES, settlement} from './settlement';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('draws each outcome as one row: the outcome, the SQS call it makes, and what that does', () => {
  const {links} = settlement(words);
  const to = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(OUTCOMES).toEqual(['ack', 'retry', 'retryAfter', 'drop', 'unsettled']);
  for (const outcome of OUTCOMES) {
    expect(to(`${outcome}Outcome`)).toEqual([`${outcome}Call`]);
    expect(to(`${outcome}Call`)).toEqual([`${outcome}Effect`]);
  }
});

it('lines the three parts of a row up in their columns', () => {
  const {boxes, lanes} = settlement(words);
  const centre = (id: string): number => {
    const box = boxes.find((candidate) => candidate.id === id)!;
    return box.x + box.w / 2;
  };
  for (const outcome of OUTCOMES) {
    expect(['Outcome', 'Call', 'Effect'].map((part) => centre(`${outcome}${part}`))).toEqual(
      lanes!.map((lane) => lane.x),
    );
  }
});
