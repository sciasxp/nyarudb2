# Contributing to NyaruDB2

Thank you for your interest in contributing to **NyaruDB2**! We welcome all kinds of contributions—bug reports, feature requests, documentation improvements, or code enhancements. To make the process smooth for everyone, please follow these guidelines.

---

## 📃 Code of Conduct
Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md) to ensure a welcoming and respectful community.

---

## 🐞 Reporting Issues
If you encounter a bug or have a feature suggestion:

1. Search existing [issues](https://github.com/galileostudio/NyaruDB2/issues) to see if it’s already reported.
2. If not, open a new issue. Include:
   - A clear and descriptive title.
   - Reproduction steps or a minimal code sample.
   - Expected vs. actual behavior.
   - Environment (macOS/iOS version, Xcode version, Swift version, etc.).

---

## 🌱 Getting Started Locally

1. **Fork** the repository and **clone** your fork:
   ```bash
   git clone https://github.com/<your-username>/NyaruDB2.git
   cd NyaruDB2
   ```

2. **Install prerequisites**:
   - Xcode 15 or later
   - Swift 5.9 or later

3. **Build** the project and run tests:
   ```bash
   swift build
   swift test
   ```

4. (Optional) Open Xcode:
   ```bash
   open Package.swift
   ```

---

## 🖋️ Coding Style

- Follow [Swift API Design Guidelines](https://swift.org/documentation/api-design-guidelines/).
- Use consistent indentation (4 spaces) and line length (~100 characters).
- Write clear, concise, and type-safe Swift code leveraging async/await.
- Add or update documentation comments (`///`) for any public API changes.
- Ensure new code is covered by unit tests in the `Tests/NyaruDB2Tests` target.

---

## 🧪 Testing

- Add unit tests under `Tests/NyaruDB2Tests`.
- Use `XCTest` for all new test cases.
- Run the full test suite before submitting:
  ```bash
  swift test --parallel
  ```

---

## 📦 Pull Request Process

1. Create a descriptive branch name: `feature/awesome-thing` or `bugfix/fix-shard-manager`.
2. Commit changes with clear messages:
   ```bash
   git commit -m "Add feature: support alternate compression algorithms"
   ```
3. Push your branch to your fork:
   ```bash
   git push origin feature/awesome-thing
   ```
4. Open a Pull Request against `main` on GitHub.
5. In your PR description, include:
   - What problem you’re solving.
   - How you solved it.
   - Any new tests or benchmarks.
6. Address review feedback promptly.

---

## 📖 Updating Documentation

- Update `README.md` for any public API or usage changes.
- If adding or changing public API, document it with examples.
- Ensure example code in the docs builds and runs.

---

## 🔒 Security Reporting

If you discover a security issue, please contact the maintainers privately at [demetrius.albuquerque@yahoo.com.br] before public disclosure.

---

## 🎉 Thank You!

Your contributions make NyaruDB2 better for everyone. We appreciate your time and effort!

