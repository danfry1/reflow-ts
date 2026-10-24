# Security Policy

## Supported Versions

Fixes are released for the latest minor version.

| Version        | Supported          |
| -------------- | ------------------ |
| Latest 0.x     | :white_check_mark: |
| Older versions | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly by emailing **danf@hotmail.co.za** instead of opening a public issue.

You should expect a response within 48 hours. If confirmed, a fix will be prioritized and released as a patch version.

## Supply Chain

How a release of `reflow-ts` gets from this repository to npm:

- **One runtime dependency**, `@standard-schema/spec`, which contains types only. Database drivers (`better-sqlite3`, `pg`) are optional peer dependencies you install yourself.
- **Pinned development dependencies.** Every dev dependency is pinned to an exact version, checked in CI, and installed from the committed lockfile with `--frozen-lockfile`. Installs refuse any package version published less than 14 days ago, and Dependabot waits as long before proposing one.
- **Pinned CI.** Every GitHub Action is pinned to a full commit SHA, the Bun version is fixed, and service images are pinned by digest. Workflows default to read-only permissions, and checkouts do not keep the token.
- **Reviewed dependency changes.** Pull requests that add a dependency with a known vulnerability fail review, and CodeQL and OpenSSF Scorecard run on every change.
- **Publishing isolated from dependency code.** The release workflow builds, tests, and packs in a job with read-only permissions. A separate job, which runs no project or dependency code, stages that exact tarball on npm through trusted publishing.
- **Provenance and a human in the loop.** Each version carries [npm provenance](https://docs.npmjs.com/generating-provenance-statements) linking it to the commit and workflow that built it, and is staged rather than published: it cannot be installed until a maintainer approves it with two-factor authentication.

To check a version you installed: `npm audit signatures`.
