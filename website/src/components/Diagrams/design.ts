import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the AWS SDK, and SQS and SNS at the bottom. The consuming side is the left column,
 * the publishing side the right one; both send through the SDK clients of the runtime they run on.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the clients under
  // it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 228, {kind: 'group'}),
    box('subscriber', left, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/path/',
    }),
    box('publishers', right, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/path/',
    }),
    box('message', left, 294, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/sqs/visibility/',
    }),
    box('policies', right, 294, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/publishing/',
    }),
    box('broker', left, 362, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/broker/',
    }),
    box('descriptor', right, 362, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/subscriptions/sqs-queue/',
    }),
    box('sdk', 0, 452, 360, 80, {kind: 'group'}),
    box('clients', 12, 478, 336, 44, {kind: 'broker'}),
    box('aws', 12, 556, 336, 36, {kind: 'broker'}),
    box('inProcess', 0, 608, 360, 44, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'subscriber', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'subscriber', to: 'clients', exit: 'left', enter: 'left'},
    {from: 'publishers', to: 'clients', exit: 'right', enter: 'right'},
    {from: 'clients', to: 'aws', label: word(words, 'https')},
  ];
  return {boxes, links};
}
