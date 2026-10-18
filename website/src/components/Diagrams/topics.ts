import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Fan-out: a publish to an SNS topic becomes a copy in every queue subscribed to it, and each
 * queue is consumed the ordinary way.
 */
export function topics(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (id: string, x: number, y: number, w: number, kind: Box['kind']): Box => ({
    id,
    x,
    y,
    w,
    h: 44,
    kind,
    lines: lines(words, id),
  });
  const boxes: Box[] = [
    box('publisher', 90, 0, 180, 'service'),
    box('topic', 60, 76, 240, 'broker'),
    box('billing', 12, 168, 156, 'broker'),
    box('shipping', 192, 168, 156, 'broker'),
    box('billingConsumer', 12, 248, 156, 'core'),
    box('shippingConsumer', 192, 248, 156, 'core'),
    {id: 'envelope', x: 0, y: 316, w: 360, h: 44, kind: 'note', lines: lines(words, 'envelope')},
  ];
  const links: Link[] = [
    {from: 'publisher', to: 'topic', label: word(words, 'publish')},
    {from: 'topic', to: 'billing', exitAt: 30 / 240, label: word(words, 'raw')},
    {from: 'topic', to: 'shipping', exitAt: 210 / 240},
    {from: 'billing', to: 'billingConsumer', label: word(words, 'receive')},
    {from: 'shipping', to: 'shippingConsumer', label: word(words, 'receive')},
  ];
  return {boxes, links};
}
