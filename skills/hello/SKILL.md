---
name: hello
description: Greets users in a requested language and style. Use when a user asks for a greeting or welcome message.
license: MIT
compatibility: Works with any Agent Skills compatible runtime.
allowed-tools: read_skill_resource
metadata:
  author: LingXi Team
  version: 2.1.0
---

# Hello

Create a short, friendly greeting in the language and tone requested by the user.

1. If the language is not clear, use the language of the user's latest message.
2. Read `references/greetings.md` only when a localized example is useful.
3. Use `assets/greeting-template.txt` when the user asks for a reusable template.
4. `scripts/hello.py` is an optional example. Reading this Skill never authorizes or executes it.

Do not claim to have run a script unless the host independently exposes and authorizes an execution tool.
