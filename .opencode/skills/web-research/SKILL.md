---
name: web-research
description: Search the internet proactively to find best practices, examples, and solutions for any programming task
license: MIT
compatibility: opencode
---

## Core principle

**Always search the web when uncertain.** It is better to research than to guess. Use the internet as your primary source for learning libraries, finding solutions, and staying current.

## When to search

Search proactively when:

- Writing new code that uses unfamiliar libraries or APIs
- Implementing features that could benefit from known patterns
- Encountering errors or bugs with unclear solutions
- Adding dependencies - verify they are well-maintained and recommended
- Making architectural decisions
- Working with unfamiliar frameworks or languages
- Need to look up syntax or language features

## Research workflow

1. **Identify the knowledge gap** - What specifically do you need to know?
2. **Craft a focused query** - Use specific keywords (e.g., "python playwright async context manager", "react useeffect cleanup")
3. **Search first, then dive deeper** - Use `websearch` to find relevant resources, then `webfetch` specific URLs
4. **Synthesize and apply** - Combine findings into a solution that fits the project

## Available tools

| Tool | Purpose |
|------|---------|
| `websearch` | General web search with configurable result count |
| `webfetch` | Fetch detailed content from specific URLs |
| `codesearch` | Specialized search for code examples and documentation |

## Guidelines

- Use `codesearch` for library-specific queries (very effective for frameworks)
- Use `websearch` for general programming questions and recent information
- Always search when introducing a new library - check docs, examples, and community recommendations
- When multiple solutions exist, research trade-offs before choosing
- Keep search queries specific and include the library/framework name when relevant
