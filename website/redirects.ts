// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

// The SQS guide of the earlier site moved to the crate's rustdoc, where its sections are the
// sections of the crate overview; the earlier site already sent its address there.
const guides: Record<string, string> = {
  sqs: 'index.html',
};

// Pages that moved within the site.
const pages: Record<string, string> = {
  // The benchmarks explain what the architecture costs.
  benchmarks: '/architecture/benchmarks/',
};

/**
 * Old address to its target, relative to the build's base URL. `docsRs` is the crate's rustdoc
 * for the version built, with a trailing slash.
 */
export function redirects(docsRs: string): Record<string, string> {
  return {
    ...Object.fromEntries(
      Object.entries(guides).map(([from, to]) => [from, `${docsRs}ruststream_sqs_sns/${to}`]),
    ),
    ...pages,
  };
}
