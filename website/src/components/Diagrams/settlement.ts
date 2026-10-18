import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

// The outcomes a delivery settles with, one row each, in the order the page's table lists them.
export const OUTCOMES = ['ack', 'retry', 'retryAfter', 'drop', 'unsettled'] as const;

// The columns of a row: the outcome, the SQS call it makes, and what the call does.
const COLUMNS = [
  {part: 'Outcome', x: 0, w: 104, kind: 'service', code: true},
  {part: 'Call', x: 116, w: 132, kind: 'broker', code: true},
  {part: 'Effect', x: 260, w: 100, kind: 'step', code: false},
] as const;

// The rows that need a third line: the drop, whose call depends on the receive count.
const TALL = new Set<string>(['drop']);

/*
 * Settlement on SQS as a table: each outcome of a handler is one call against the queue, and the
 * call decides whether the message is gone or comes back.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const boxes: Box[] = [];
  const links: Link[] = [];
  let y = 28;
  for (const outcome of OUTCOMES) {
    const h = TALL.has(outcome) ? 58 : 44;
    for (const column of COLUMNS) {
      boxes.push({
        id: `${outcome}${column.part}`,
        x: column.x,
        y,
        w: column.w,
        h,
        kind: column.kind,
        code: column.code,
        lines: lines(words, `${outcome}${column.part}`),
      });
    }
    links.push(
      {from: `${outcome}Outcome`, to: `${outcome}Call`},
      {from: `${outcome}Call`, to: `${outcome}Effect`},
    );
    y += h + 16;
  }
  const lanes: Lane[] = COLUMNS.map((column) => ({
    x: column.x + column.w / 2,
    label: word(words, column.part.toLowerCase()),
  }));
  return {boxes, links, lanes};
}
