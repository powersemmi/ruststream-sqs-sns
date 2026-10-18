import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The message groups of the drawing, and the lane each one's key hashes to. Two groups share a
// lane: lanes are fewer than groups, or their keys collide.
export const GROUP_LANES = [0, 1, 0] as const;

const LANE_X = [12, 188] as const;

/*
 * One subscription's dispatch: the pump receives a batch ahead, the stream hands the messages
 * over one at a time, and `workers(n, by_key)` keys each message to a lane by its message group.
 */
export function lanes(words: Words): {boxes: Box[]; links: Link[]} {
  const wide = (id: string, y: number, kind: Box['kind'], href?: string): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind,
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    wide('queue', 0, 'broker', '/components/subscriptions/fifo/'),
    wide('pump', 72, 'broker'),
    wide('stream', 144, 'broker'),
    {...wide('keyed', 216, 'core'), code: true},
    ...LANE_X.map(
      (x, lane): Box => ({
        id: `lane${lane}`,
        x,
        y: 296,
        w: 160,
        h: 58,
        kind: 'service',
        lines: lines(words, `lane${lane}`),
      }),
    ),
    {id: 'standard', x: 0, y: 376, w: 360, h: 44, kind: 'note', lines: lines(words, 'standard')},
    {id: 'replicas', x: 0, y: 428, w: 360, h: 44, kind: 'note', lines: lines(words, 'replicas')},
  ];
  const links: Link[] = [
    {from: 'queue', to: 'pump', label: word(words, 'receives')},
    {from: 'pump', to: 'stream', label: word(words, 'oneBatch')},
    {from: 'stream', to: 'keyed', label: word(words, 'oneAtATime')},
    ...LANE_X.map((x, lane) => ({
      from: 'keyed',
      to: `lane${lane}`,
      exitAt: (x + 80 - 12) / 336,
    })),
  ];
  return {boxes, links};
}
