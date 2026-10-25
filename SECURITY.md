# Security Policy

## Supported Versions

Fixes are released for the latest minor version.

| Version        | Supported          |
| -------------- | ------------------ |
| Latest 0.x     | :white_check_mark: |
| Older versions | :x:                |

## Reporting a Vulnerability

Please report security issues privately, not in a public issue, discussion or pull request:

- **Preferred:** use GitHub's private vulnerability reporting. Open this repository's **Security** tab and choose **Report a vulnerability**, or go to <https://github.com/danfry1/reflow-ts/security/advisories/new>.
- **Alternatively:** email **security@danfry.dev**.

Include the version or commit, steps to reproduce, and the impact you expect.

You should expect an acknowledgement within 7 days, and credit in the advisory if you want it. If confirmed, a fix will be prioritized and released as a patch version, followed by a published security advisory.

## Supply Chain

How a release of `reflow-ts` gets from this repository to npm:

- **One runtime dependency**, `@standard-schema/spec`, which contains types only. Database drivers (`better-sqlite3`, `pg`) are optional peer dependencies you install yourself.
- **Pinned development dependencies.** Every dev dependency is pinned to an exact version, checked in CI, and installed from the committed lockfile with `--frozen-lockfile`. Dependabot proposes no version until it has been public for 14 days, and adding or updating a package locally refuses one younger than that.
- **Pinned CI.** Every GitHub Action is pinned to a full commit SHA, the Bun version is fixed, and service images are pinned by digest. Workflows default to read-only permissions, and checkouts do not keep the token.
- **Audited dependencies.** Every CI run audits the whole lockfile, transitive packages included, and fails on any known advisory of moderate severity or worse; the few exceptions are listed in the workflow with the advisory and the date their fix becomes usable. Pull requests are also checked by GitHub's dependency review, which sees direct dependencies. CodeQL runs on every pull request to `main` and every push to it, and OpenSSF Scorecard on every push to `main` and weekly.
- **Publishing isolated from dependency code.** The release workflow builds, tests, and packs in a job with read-only permissions, after checking that the tag matches the package version and is on `main`. A separate job, holding only the npm publishing permission and installing nothing, stages that exact tarball with the npm that ships with Node; a third, holding only permission to write releases, creates the GitHub Release.
- **Provenance and a human in the loop.** Each version carries [npm provenance](https://docs.npmjs.com/generating-provenance-statements) linking it to the commit and workflow that built it, and is staged rather than published: it cannot be installed until a maintainer approves it with two-factor authentication.

To check a version you installed: `npm audit signatures`.
