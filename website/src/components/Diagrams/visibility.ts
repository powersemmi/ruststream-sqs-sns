import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The visibility of one message: a receive takes it out of view, and a delete ends it, while a
 * lapsed timeout or a visibility of zero puts it back for the next receive, which counts it once
 * more and, past the redrive policy's cap, hands it to the dead-letter queue instead.
 */
export function visibility(words: Words): {boxes: Box[]; links: Link[]} {
  const way = (id: string, x: number, kind: Box['kind'], href?: string): Box => ({
    id,
    x,
    y: 168,
    w: 112,
    h: 62,
    kind,
    code: id === 'zero',
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'stored', x: 12, y: 0, w: 336, h: 44, kind: 'broker', lines: lines(words, 'stored')},
    {id: 'inFlight', x: 12, y: 74, w: 336, h: 62, kind: 'core', lines: lines(words, 'inFlight')},
    way('deleted', 0, 'good'),
    way('lapsed', 124, 'step'),
    way('zero', 248, 'step'),
    {id: 'again', x: 124, y: 262, w: 236, h: 44, kind: 'broker', lines: lines(words, 'again')},
    {
      id: 'deadLetter',
      x: 124,
      y: 338,
      w: 236,
      h: 44,
      kind: 'bad',
      lines: lines(words, 'deadLetter'),
      href: '/architecture/settlement/',
    },
    {id: 'extend', x: 0, y: 404, w: 360, h: 44, kind: 'note', lines: lines(words, 'extend')},
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'stored', to: 'inFlight', label: word(words, 'receive')},
    {from: 'inFlight', to: 'deleted', exitAt: along(56)},
    {from: 'inFlight', to: 'lapsed', exitAt: along(180)},
    {from: 'inFlight', to: 'zero', exitAt: along(304)},
    {from: 'lapsed', to: 'again', enterAt: 56 / 236},
    {from: 'zero', to: 'again', enterAt: 180 / 236},
    {from: 'again', to: 'deadLetter', label: word(words, 'pastCap')},
  ];
  return {boxes, links};
}
