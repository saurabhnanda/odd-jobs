# Contribution Guidelines

## Goal for odd-jobs

Broadly speaking, I want to strike a good balance between features, configurability, and ease-of-use - with the last one getting a little more weightage. For example, I don't want `odd-jobs` to devolve into a mess of complicated types and custom monads, which, _theoretically_, give a lot of compile-time guarantees, but _practically_ make the library nearly-impossible to use!

I'd like to see odd-jobs become a mature and production-ready job-queue.

## How to contribute?

1. For small fixes (like obvious typos, obvious ommissions in docs, etc), go ahead and directly open a PR. Please don't worry if the PR is open for a few days - I generally work on things in a batch.
2. For integrating with other frameworks, like Yesod, Snap, IHP, and more, if you are aware of how to reasonably integrate with the target framework, please open the PR. We will need to decide whether to  convert this repo into a mono-repo, so that all such integrations can be released in-step with core package.
3. For larger features or fixes, it's best to quickly bounce-off the idea on the issue tracker before investing time in writing code. 
    - I already have a broad roadmap in mind for the project (search for [issues tagged with `roadmap`](https://github.com/saurabhnanda/odd-jobs/issues?q=is%3Aissue+is%3Aopen+label%3Aroadmap)). See if the feature/fix you have in mind is already on the roadmap. Participate in the relevant discussion (on the issue tracker) and let your intent to work on that issue be known (to avoid effort duplication).
    - If you want to work on something that is not on the roadmap, please raise an issue and start a discussion first. Try sharing the real-life use-case for the feature, and brainstorming possible solutions. Designing something that is powerful, yet simple, is harder than it sounds!

## Hacktoberfest 2020

<img src="https://hacktoberfest.digitalocean.com/assets/HF-full-logo-b05d5eb32b3f3ecc9b2240526104cf4da3187b8b61963dd9042fdc2536e4a76c.svg" width="300" title="Hacktoberfest 2020 Logo" />

Take a look at all all [issues tagged with `hacktoberfest` label](https://github.com/saurabhnanda/odd-jobs/issues?q=is%3Aissue+is%3Aopen+label%3Ahacktoberfest). These seem like they can be tackled within one month of picking them up.
