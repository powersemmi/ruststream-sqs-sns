/*
 * This crate's results, as the pages of this site show them. The document is the one the
 * benchmark recipe committed beside the pages, read when the site is built: a version of the
 * documentation shows the numbers measured for it, and an archive keeps its own.
 */

import document from '../../../../docs/benchmarks/results.json';
import {BROKERS, type Broker, type Loaded, type Results} from './model';

export const SQS: Broker = BROKERS.find((broker) => broker.repo === 'ruststream-sqs-sns')!;

export const sqs: Loaded = {broker: SQS, results: document as Results};
