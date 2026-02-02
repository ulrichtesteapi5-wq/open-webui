# 🐍 RPG Context Manager: Quality Rules

Este documento define padrões mínimos e gates oficiais para qualidade, segurança e consistência do código. Todos os agentes e contribuidores devem seguir estas diretrizes.

---

## 🎯 Objetivo

Manter o código consistente, tipado e seguro com um pipeline único e reproduzível.

---

## ✅ Requisitos mínimos (obrigatórios)

As ferramentas abaixo **devem** estar instaladas e passar sem erros:

- **black**
- **ruff**
- **mypy**
- **semgrep**
- **vulture**
- **bandit**
- **pip-audit**
- **codespell**

---

## 🚀 Gates oficiais (única fonte de verdade)

Use o script oficial do projeto:

`run_quality.bat`

Ele executa, em ordem:

`black .; ruff check . --fix; mypy .; semgrep scan --config auto; vulture .; bandit -r app rpg_context_manager.py; pip-audit -r requirements.txt; codespell`

---

## 🔒 Enforcement obrigatório

- **Pre-commit obrigatório** para qualquer alteração em código:
  - `pre-commit install`
  - `pre-commit run --all-files`
- Falha em qualquer gate **bloqueia** a entrega até correção.

---

## ➕ Ferramentas opcionais (não bloqueiam entrega)

Use apenas se necessário ou por demanda específica:

- **pylint**, **pyright**, **safety**, **radon**, **sourcery-cli**, **pyupgrade**

---

## 🧪 Testes

Testes são necessários, mas **não** fazem parte dos gates acima. Use:

- `pytest`
- `pytest --cov`