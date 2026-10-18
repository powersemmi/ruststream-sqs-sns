import {describe, expect, it} from 'vitest';
import {jsonLd, softwareSourceCode} from './structuredData';

describe('softwareSourceCode', () => {
  it('describes the project as the schema.org type for a code repository', () => {
    expect(
      softwareSourceCode({
        description: 'The Amazon SQS broker for RustStream.',
        url: 'https://powersemmi.github.io/ruststream-sqs-sns/latest/ru/',
      }),
    ).toEqual({
      '@context': 'https://schema.org',
      '@type': 'SoftwareSourceCode',
      name: 'ruststream-sqs-sns',
      description: 'The Amazon SQS broker for RustStream.',
      url: 'https://powersemmi.github.io/ruststream-sqs-sns/latest/ru/',
      codeRepository: 'https://github.com/powersemmi/ruststream-sqs-sns',
      programmingLanguage: 'Rust',
      runtimePlatform: 'tokio',
      license: 'https://www.apache.org/licenses/LICENSE-2.0',
    });
  });
});

describe('jsonLd', () => {
  it('keeps a value from closing the script element it is written into', () => {
    const text = jsonLd({description: 'handlers </script><b>'});
    expect(text).not.toContain('</script>');
    expect(JSON.parse(text)).toEqual({description: 'handlers </script><b>'});
  });
});
