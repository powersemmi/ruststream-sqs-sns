import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The messages the drawing stores, and the width of one in the queue.
export const STORED = 5;
const CELL = 58;
const STEP = 68;

/*
 * A queue: producers send to it, it stores each message until a consumer deletes it, and the
 * consumers that poll it compete, each message going to one of them.
 */
export function queue(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'producers', x: 90, y: 0, w: 180, h: 44, kind: 'service', lines: lines(words, 'producers')},
    {id: 'queue', x: 0, y: 72, w: 360, h: 124, kind: 'group', lines: lines(words, 'queue')},
  ];
  for (let index = 0; index < STORED; index += 1) {
    boxes.push({
      id: `m${index}`,
      x: 15 + index * STEP,
      y: 100,
      w: CELL,
      h: 30,
      kind: 'step',
      lines: [String(index + 1)],
    });
  }
  boxes.push(
    {id: 'kinds', x: 12, y: 142, w: 336, h: 44, kind: 'note', lines: lines(words, 'kinds')},
    {id: 'first', x: 0, y: 226, w: 172, h: 62, kind: 'core', lines: lines(words, 'first')},
    {id: 'second', x: 188, y: 226, w: 172, h: 62, kind: 'core', lines: lines(words, 'second')},
    {id: 'retention', x: 0, y: 304, w: 360, h: 44, kind: 'note', lines: lines(words, 'retention')},
  );
  const links: Link[] = [
    {from: 'producers', to: 'queue', label: word(words, 'sends')},
    {from: 'queue', to: 'first', exitAt: 86 / 360, label: word(words, 'receives')},
    {from: 'queue', to: 'second', exitAt: 274 / 360, label: word(words, 'receives')},
  ];
  return {boxes, links};
}
