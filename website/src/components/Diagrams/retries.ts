import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A retry on SQS, which holds a message back and counts its receives itself: the delivery is
 * made invisible for the delay, comes back as the same message, and the queue's redrive policy
 * decides on the next receive whether it is delivered again or moved to the dead-letter queue.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const wide = (id: string, y: number, kind: Box['kind']): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    wide('delivery', 0, 'broker'),
    wide('outcome', 72, 'service'),
    {...wide('wait', 144, 'broker'), code: true},
    wide('cap', 216, 'core'),
    {id: 'again', x: 0, y: 296, w: 170, h: 58, kind: 'step', lines: lines(words, 'again')},
    {
      id: 'deadLetter',
      x: 190,
      y: 296,
      w: 170,
      h: 58,
      kind: 'bad',
      lines: lines(words, 'deadLetter'),
    },
    {id: 'drop', x: 0, y: 376, w: 360, h: 44, kind: 'note', lines: lines(words, 'drop')},
    {id: 'crash', x: 0, y: 428, w: 360, h: 44, kind: 'note', lines: lines(words, 'crash')},
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'delivery', to: 'outcome', label: word(words, 'handled')},
    {from: 'outcome', to: 'wait', label: word(words, 'retries')},
    {from: 'wait', to: 'cap', label: word(words, 'after')},
    {from: 'cap', to: 'again', exitAt: along(85), label: word(words, 'below')},
    {from: 'cap', to: 'deadLetter', exitAt: along(275), label: word(words, 'past')},
  ];
  return {boxes, links};
}
