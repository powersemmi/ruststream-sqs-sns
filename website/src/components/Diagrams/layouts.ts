import {design} from './design';
import type {Box, Lane, Link} from './flow';
import {groups} from './groups';
import {lanes} from './lanes';
import {lifecycle} from './lifecycle';
import {path} from './path';
import {queue} from './queue';
import {retries} from './retries';
import {settlement} from './settlement';
import {topics} from './topics';
import {visibility} from './visibility';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  queue,
  visibility,
  groups,
  topics,
  design,
  lifecycle,
  path,
  settlement,
  retries,
  lanes,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
