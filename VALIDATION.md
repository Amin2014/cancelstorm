# Validation

Validated on Python 3.12.14 on 2026-10-27.

- `python -m pip install --no-build-isolation -e '.[test]'`: passed.
- `PYTHONASYNCIODEBUG=1 python -W error -m pytest -q`: **30 passed**.
- `python -m ruff check .`: passed.
- `python -m ruff format --check .`: passed.
- `python -m examples.payments both`: expected broken workflow **7/10**;
  corrected workflow **10/10**. All three broken failures are cancellation scenarios.
- `python -m build --no-isolation`: wheel and source distribution built.
- Wheel installed in a clean virtual environment without dependencies: public API
  smoke test passed; importing the runtime does not require pytest.
- Source distribution includes documentation, license, examples and tests.

Python 3.13 and 3.14 are configured in GitHub Actions but were not locally available.
No GitHub or PyPI publication was performed. See README for cancellation-model and
cooperative-timeout limits; these tests do not establish exhaustive interleaving safety.
