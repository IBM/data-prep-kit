# Contributing

👍🎉 First off, thank you for taking the time to contribute! 🎉👍

The following is a set of guidelines for contributing. These are just guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

- [What Should I Know Before I Get Started?](#what-should-i-know-before-i-get-started)
  - [Code of Conduct](#code-of-conduct)
  - [How Do I Start Contributing?](#how-do-i-start-contributing)
- [How Can I Contribute?](#how-can-i-contribute)
  - [Reporting Bugs](#reporting-bugs)
    - [How Do I Submit A (Good) Bug Report?](#how-do-i-submit-a-good-bug-report)
  - [Suggesting Enhancements](#suggesting-enhancements)
    - [How Do I Submit A (Good) Enhancement Suggestion?](#how-do-i-submit-a-good-enhancement-suggestion)
- [Set up your dev environment](#set-up-your-dev-environment)
  - [Useful commands](#useful-commands)
- [Your First Code Contribution](#your-first-code-contribution)
  - [How to contribute](#how-to-contribute)
    - [Code Review](#code-review)

## What Should I Know Before I Get Started?

If you're new to GitHub and working with inner source repositories, this section will be helpful.
Also, please read documentation on the project, describing inner workings of its components. we recommend starting with the [overview](data-processing-lib/doc/overview.md)
Otherwise, you can skip to learning how to [set up your dev environment](#set-up-your-dev-environment)

### Code of Conduct

This project adheres to the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

Please report unacceptable behavior to one of the maintainers.

### How Do I Start Contributing?

The below workflow is designed to help you begin your first contribution journey. It will guide you through creating and picking up issues, working through them, having your work reviewed, and then merging.

Help on inner source projects is always welcome and there is always something that can be improved. For example, documentation (like the text you are reading now) can always use improvement, code can always be clarified, variables or functions can always be renamed or commented on, and there is always a need for more test coverage. If you see something that you think should be fixed, take ownership! Here is how you get started:

## How Can I Contribute?

When contributing, it's useful to start by looking at issues. After picking up an issue, writing code, or updating a document, make a pull request and your work will be reviewed and merged. If you're adding a new feature, it's best to write an issue first to discuss it with maintainers first.

### Reporting Bugs

This section guides you through submitting a bug report. Following these guidelines helps maintainers and the community understand your report ✏️, reproduce the behavior 💻, and find related reports 🔎.

#### How Do I Submit A (Good) Bug Report?

Bugs are tracked as [GitHub issues](https://guides.github.com/features/issues/). Create an issue on that and provide the information suggested in the bug report issue template.

### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion, including completely new features, tools, and minor improvements to existing functionality. Following these guidelines helps maintainers and the community understand your suggestion ✏️ and find related suggestions 🔎

#### How Do I Submit A (Good) Enhancement Suggestion?

Enhancement suggestions are tracked as [GitHub issues](https://guides.github.com/features/issues/). Create an issue and provide the information suggested in the feature requests or user story issue template.

### Code formatting

We use [pre-commit](https://pre-commit.com) to share git pre-commit hooks. You'll need to [install it](https://pre-commit.com/#install) and set it up for our repo

We recommend that you install pre-commit on your machine so that code formatting is transparent to your development workflow.

Unsure where to begin contributing? You can start by looking through these issues:

- Issues with the `good first issue` tag - these should only require a few lines of code and are good targets if you're just starting contributing.
- Issues with the `help-wanted` tag - these range from simple to more complex, but are generally things we want but can't get to in a short time frame.

### How to contribute

To contribute to this repo, you'll use the Fork and Pull model common in many open source repositories. For details on this process, watch [how to contribute](https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github).

When ready, you can create a pull request. Pull requests are often referred to as "PR". In general, we follow the standard [github pull request](https://help.github.com/en/articles/about-pull-requests) process. Follow the template to provide details about your pull request to the maintainers.

Before sending pull requests, make sure your changes pass tests.

#### Code Review

Once you've [created a pull request](#how-to-contribute), maintainers will review your code and likely make suggestions to fix before merging. It will be easier for your pull request to receive reviews if you consider the criteria the reviewers follow while working. Remember to:

- Run tests locally and ensure they pass
- Follow the project coding conventions
- Write detailed commit messages
- Break large changes into a logical series of smaller patches, which are easy to understand individually and combine to solve a broader issue

Note: if you believe your pull request isn't getting enough attention, contact a maintainer.