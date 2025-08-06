# Releasing

To release a new version, start
the [Prepare Release](https://github.com/wulf-data-engineering/kiek/actions/workflows/release_pr.yml) workflow with
major, minor or
patch as input.

It creates a pull request for the new version.

Merging that pull request starts
the [Release](https://github.com/wulf-data-engineering/kiek/actions/workflows/release.yml) workflow, which creates a new
draft release with the binaries for macOS and Linux.
It also drafts a pull request for
the [Homebrew tap Formula](https://github.com/wulf-data-engineering/homebrew-tap/blob/main/Formula/kiek.rb).