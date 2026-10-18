import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The message groups the drawing holds, and the state of each message, head first. The head of
// the first group is in flight, so the rest of that group waits behind it.
export const GROUPS = [
  ['inFlight', 'held', 'held'],
  ['delivered', 'next', 'next'],
  ['delivered', 'next'],
] as const;

const KIND = {inFlight: 'core', held: 'bad', delivered: 'good', next: 'step'} as const;

/*
 * A FIFO queue keeps the order of each message group: while a group has a message in flight, its
 * next message waits, and the other groups keep delivering.
 */
export function groups(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'queue', x: 0, y: 0, w: 360, h: 184, kind: 'group', lines: lines(words, 'queue')},
  ];
  GROUPS.forEach((messages, group) => {
    const y = 28 + group * 52;
    boxes.push({
      id: `g${group}`,
      x: 12,
      y,
      w: 72,
      h: 40,
      kind: 'broker',
      code: true,
      lines: lines(words, `g${group}`),
    });
    messages.forEach((state, index) => {
      boxes.push({
        id: `g${group}m${index}`,
        x: 96 + index * 86,
        y,
        w: 78,
        h: 40,
        kind: KIND[state],
        lines: [String(index + 1), word(words, state)],
      });
    });
  });
  boxes.push(
    {id: 'consumer', x: 12, y: 216, w: 336, h: 44, kind: 'core', lines: lines(words, 'consumer')},
    {
      id: 'deduplication',
      x: 0,
      y: 284,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'deduplication'),
    },
  );
  const links: Link[] = [{from: 'queue', to: 'consumer', label: word(words, 'receives')}];
  return {boxes, links};
}
